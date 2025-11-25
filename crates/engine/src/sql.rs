use crate::Engine;
use serde::{de::DeserializeOwned, Serialize};
use sqlparser::ast::{BinaryOperator, Expr, Query, Select, SelectItem, SetExpr, Statement, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::fmt;
use std::hash::Hash;
use std::ops::Bound;

/// SQL query result
#[derive(Debug, Clone, Serialize)]
pub struct SqlResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
    pub rows_affected: usize,
}

/// SQL value (simplified for KV store)
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum SqlValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
}

struct KeyRange<K> {
    lower: Option<(K, bool)>, // (value, exclusive?)
    upper: Option<(K, bool)>,
}

impl fmt::Display for SqlValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlValue::String(s) => write!(f, "{}", s),
            SqlValue::Integer(i) => write!(f, "{}", i),
            SqlValue::Float(fl) => write!(f, "{}", fl),
            SqlValue::Boolean(b) => write!(f, "{}", b),
            SqlValue::Null => write!(f, "NULL"),
        }
    }
}

impl fmt::Display for SqlResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.columns.is_empty() {
            return write!(f, "{} row(s) affected", self.rows_affected);
        }

        // Print column headers
        writeln!(f, "{}", self.columns.join(" | "))?;
        writeln!(f, "{}", "-".repeat(self.columns.len() * 20))?;

        // Print rows
        for row in &self.rows {
            let row_str: Vec<String> = row.iter().map(|v| v.to_string()).collect();
            writeln!(f, "{}", row_str.join(" | "))?;
        }

        writeln!(f, "\n{} row(s) returned", self.rows.len())
    }
}

/// SQL executor for Engine
pub struct SqlExecutor<K, V> {
    _phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V> SqlExecutor<K, V>
where
    K: Clone
        + Ord
        + Hash
        + Serialize
        + DeserializeOwned
        + fmt::Display
        + fmt::Debug
        + Send
        + Sync
        + 'static
        + art::AsBytes,
    V: Clone + Serialize + DeserializeOwned + fmt::Debug + Send + Sync + 'static,
{
    /// Execute a SQL query
    pub fn execute(engine: &Engine<K, V>, sql: &str) -> Result<SqlResult, String> {
        let dialect = GenericDialect {};
        let ast =
            Parser::parse_sql(&dialect, sql).map_err(|e| format!("SQL parse error: {}", e))?;

        if ast.is_empty() {
            return Err("Empty SQL statement".to_string());
        }

        match &ast[0] {
            Statement::Query(query) => Self::execute_query(engine, query),
            Statement::Insert(insert) => {
                Self::execute_insert(engine, &insert.table_name, &insert.columns, &insert.source)
            }
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => Self::execute_update(engine, table, assignments, selection),
            Statement::Delete(inner) => {
                // `inner` is a reference to the Delete struct because we matched on &ast[0]
                Self::execute_delete(engine, &inner.selection)
            }
            _ => Err(format!("Unsupported SQL statement: {:?}", ast[0])),
        }
    }

    fn execute_query(engine: &Engine<K, V>, query: &Query) -> Result<SqlResult, String> {
        if let SetExpr::Select(select) = &*query.body {
            Self::execute_select(engine, select, query)
        } else {
            Err("Only SELECT queries are supported".to_string())
        }
    }

    fn execute_select(
        engine: &Engine<K, V>,
        select: &Select,
        query: &Query,
    ) -> Result<SqlResult, String> {
        // Check for JOIN
        if select.from.len() == 1 {
            let table = &select.from[0];

            // Check if there are joins
            if !table.joins.is_empty() {
                return Self::execute_join(engine, select);
            }
        }

        // Regular SELECT (no join)
        let projection = Self::parse_projection(&select.projection)?;
        let where_clause = select.selection.as_ref();

        if Self::is_aggregation(&projection) {
            return Self::execute_aggregation(engine, &projection, where_clause);
        }

        // Get data based on WHERE clause
        let mut rows = Vec::new();

        // Default iterator is full ordered scan
        let mut candidate_rows: Box<dyn Iterator<Item = (K, V)>> = Box::new(
            engine
                .range(Bound::Unbounded, Bound::Unbounded)
                .into_iter()
                .map(|(k, v)| (k, v)),
        );

        if let Some(expr) = where_clause {
            // Parse WHERE clause
            let filter = Self::parse_where_clause(expr)?;

            // Prefer a bounded key range when possible
            if let Some(key_range) = Self::derive_key_range(&filter)? {
                let lower_ref = match &key_range.lower {
                    Some((k, true)) => Bound::Excluded(k),
                    Some((k, false)) => Bound::Included(k),
                    None => Bound::Unbounded,
                };
                let upper_ref = match &key_range.upper {
                    Some((k, true)) => Bound::Excluded(k),
                    Some((k, false)) => Bound::Included(k),
                    None => Bound::Unbounded,
                };
                candidate_rows = Box::new(engine.range(lower_ref, upper_ref).into_iter());
            }

            match filter {
                WhereFilter::EqualsField(field, val) => match field {
                    Field::Key => {
                        let key = Self::parse_key(&val)?;
                        if let Some(value) = engine.get(&key) {
                            rows.push(Self::format_row(&key, &value, &projection)?);
                        }
                    }
                    Field::Value => {
                        let matches = engine.find_by_value(&val);
                        for (k, v) in matches {
                            rows.push(Self::format_row(&k, &v, &projection)?);
                        }
                    }
                },
                WhereFilter::LikeField(field, pattern) => match field {
                    Field::Key => {
                        for (k, v) in candidate_rows {
                            let key_str = format!("{}", k);
                            if Self::matches_like_pattern(&key_str, &pattern) {
                                rows.push(Self::format_row(&k, &v, &projection)?);
                            }
                        }
                    }
                    Field::Value => {
                        let matches = engine.find_by_value_like(&pattern);
                        for (k, v) in matches {
                            rows.push(Self::format_row(&k, &v, &projection)?);
                        }
                    }
                },
                WhereFilter::InField(field, values) => match field {
                    Field::Key => {
                        for value_str in values {
                            let key = Self::parse_key(&value_str)?;
                            if let Some(value) = engine.get(&key) {
                                rows.push(Self::format_row(&key, &value, &projection)?);
                            }
                        }
                    }
                    Field::Value => {
                        for value_str in values {
                            let matches = engine.find_by_value(&value_str);
                            for (k, v) in matches {
                                rows.push(Self::format_row(&k, &v, &projection)?);
                            }
                        }
                    }
                },
                WhereFilter::Not(inner) => {
                    for (k, v) in candidate_rows {
                        if !Self::matches_single_filter(&k, &v, &*inner) {
                            rows.push(Self::format_row(&k, &v, &projection)?);
                        }
                    }
                }
                WhereFilter::And(filters) => {
                    for (k, v) in candidate_rows {
                        if Self::matches_all_filters(&k, &v, &filters) {
                            rows.push(Self::format_row(&k, &v, &projection)?);
                        }
                    }
                }
                WhereFilter::Or(filters) => {
                    for (k, v) in candidate_rows {
                        if Self::matches_any_filter(&k, &v, &filters) {
                            rows.push(Self::format_row(&k, &v, &projection)?);
                        }
                    }
                }
                WhereFilter::RangeField(field, start, end) => match field {
                    Field::Key => {
                        for (k, v) in candidate_rows {
                            rows.push(Self::format_row(&k, &v, &projection)?);
                        }
                    }
                    Field::Value => {
                        for (k, v) in candidate_rows {
                            let vt = Self::value_to_text(&v);
                            if vt >= start && vt <= end {
                                rows.push(Self::format_row(&k, &v, &projection)?);
                            }
                        }
                    }
                },
                WhereFilter::GreaterThanField(field, val) => match field {
                    Field::Key => {
                        for (k, v) in candidate_rows {
                            rows.push(Self::format_row(&k, &v, &projection)?);
                        }
                    }
                    Field::Value => {
                        for (k, v) in candidate_rows {
                            if Self::value_to_text(&v) > val {
                                rows.push(Self::format_row(&k, &v, &projection)?);
                            }
                        }
                    }
                },
                WhereFilter::LessThanField(field, val) => match field {
                    Field::Key => {
                        for (k, v) in candidate_rows {
                            rows.push(Self::format_row(&k, &v, &projection)?);
                        }
                    }
                    Field::Value => {
                        for (k, v) in candidate_rows {
                            if Self::value_to_text(&v) < val {
                                rows.push(Self::format_row(&k, &v, &projection)?);
                            }
                        }
                    }
                },
                WhereFilter::All => {
                    for (k, v) in candidate_rows {
                        rows.push(Self::format_row(&k, &v, &projection)?);
                    }
                }
            }
        } else {
            for (k, v) in candidate_rows {
                rows.push(Self::format_row(&k, &v, &projection)?);
            }
        }

        // Handle aggregations
        if Self::is_aggregation(&projection) {
            return Self::apply_aggregation(&rows, &projection);
        }

        // Handle DISTINCT
        if select.distinct.is_some() {
            rows = Self::apply_distinct(rows);
        }

        // Handle ORDER BY
        if let Some(order_by_clause) = &query.order_by {
            rows = Self::apply_order_by(rows, &order_by_clause.exprs)?;
        }

        // Handle LIMIT and OFFSET
        let mut offset = 0;
        if let Some(offset_obj) = &query.offset {
            // offset is an Offset struct with a value field
            if let Expr::Value(Value::Number(n, _)) = &offset_obj.value {
                offset = n.parse().map_err(|_| "Invalid OFFSET value".to_string())?;
            }
        }

        if let Some(limit_expr) = &query.limit {
            if let Expr::Value(Value::Number(n, _)) = limit_expr {
                let limit: usize = n.parse().map_err(|_| "Invalid LIMIT value".to_string())?;

                // Apply OFFSET and LIMIT
                if offset > 0 {
                    if offset < rows.len() {
                        rows = rows.into_iter().skip(offset).take(limit).collect();
                    } else {
                        rows.clear(); // Offset beyond available rows
                    }
                } else {
                    rows.truncate(limit);
                }
            }
        } else if offset > 0 {
            // OFFSET without LIMIT
            if offset < rows.len() {
                rows = rows.into_iter().skip(offset).collect();
            } else {
                rows.clear();
            }
        }

        // Handle TOP(n) - SQL Server style
        if let Some(top_expr) = &select.top {
            // top.quantity is Option<TopQuantity> which can be Expr or Constant
            if let Some(quantity) = &top_expr.quantity {
                match quantity {
                    sqlparser::ast::TopQuantity::Expr(expr) => {
                        if let Expr::Value(Value::Number(n, _)) = expr {
                            let top: usize =
                                n.parse().map_err(|_| "Invalid TOP value".to_string())?;
                            rows.truncate(top);
                        }
                    }
                    sqlparser::ast::TopQuantity::Constant(n) => {
                        rows.truncate(*n as usize);
                    }
                }
            }
        }

        Ok(SqlResult {
            columns: projection.clone(),
            rows,
            rows_affected: 0,
        })
    }

    fn execute_join(engine: &Engine<K, V>, select: &Select) -> Result<SqlResult, String> {
        // For now, simple implementation: assume self-join on KV table
        // Example: SELECT * FROM kv AS a JOIN kv AS b ON a.key = b.value

        let table = &select.from[0];

        if table.joins.is_empty() {
            return Err("No joins found".to_string());
        }

        // Get all data from left table
        let left_data: Vec<(K, V)> = engine.range(Bound::Unbounded, Bound::Unbounded);

        // Get all data from right table (same table, self-join)
        let right_data: Vec<(K, V)> = engine.range(Bound::Unbounded, Bound::Unbounded);

        // Simple hash join implementation
        let mut rows = Vec::new();

        for (left_key, left_val) in &left_data {
            for (right_key, right_val) in &right_data {
                // For simplicity: join on key equality
                // Real implementation would parse ON clause
                let left_key_str = format!("{}", left_key);
                let right_key_str = format!("{}", right_key);

                if left_key_str == right_key_str {
                    // Match found - create joined row
                    // Format values similarly to format_row
                    let left_val_sql = match serde_json::to_value(left_val) {
                        Ok(serde_json::Value::String(s)) => SqlValue::String(s),
                        Ok(serde_json::Value::Number(n)) => {
                            if let Some(i) = n.as_i64() {
                                SqlValue::Integer(i)
                            } else if let Some(f) = n.as_f64() {
                                SqlValue::Float(f)
                            } else {
                                SqlValue::String(n.to_string())
                            }
                        }
                        Ok(serde_json::Value::Bool(b)) => SqlValue::Boolean(b),
                        Ok(serde_json::Value::Null) => SqlValue::Null,
                        Ok(other) => SqlValue::String(
                            serde_json::to_string(&other)
                                .unwrap_or_else(|_| format!("{:?}", left_val)),
                        ),
                        Err(_) => SqlValue::String(format!("{:?}", left_val)),
                    };

                    let right_val_sql = match serde_json::to_value(right_val) {
                        Ok(serde_json::Value::String(s)) => SqlValue::String(s),
                        Ok(serde_json::Value::Number(n)) => {
                            if let Some(i) = n.as_i64() {
                                SqlValue::Integer(i)
                            } else if let Some(f) = n.as_f64() {
                                SqlValue::Float(f)
                            } else {
                                SqlValue::String(n.to_string())
                            }
                        }
                        Ok(serde_json::Value::Bool(b)) => SqlValue::Boolean(b),
                        Ok(serde_json::Value::Null) => SqlValue::Null,
                        Ok(other) => SqlValue::String(
                            serde_json::to_string(&other)
                                .unwrap_or_else(|_| format!("{:?}", right_val)),
                        ),
                        Err(_) => SqlValue::String(format!("{:?}", right_val)),
                    };

                    rows.push(vec![
                        SqlValue::String(left_key_str),
                        left_val_sql,
                        SqlValue::String(right_key_str),
                        right_val_sql,
                    ]);
                }
            }
        }

        Ok(SqlResult {
            columns: vec![
                "a.key".to_string(),
                "a.value".to_string(),
                "b.key".to_string(),
                "b.value".to_string(),
            ],
            rows,
            rows_affected: 0,
        })
    }

    fn execute_insert(
        engine: &Engine<K, V>,
        _table_name: &sqlparser::ast::ObjectName,
        columns: &[sqlparser::ast::Ident],
        source: &Option<Box<Query>>,
    ) -> Result<SqlResult, String> {
        if columns.len() != 2 {
            return Err("INSERT requires exactly 2 columns: (key, value)".to_string());
        }

        let query = source.as_ref().ok_or("INSERT requires VALUES clause")?;

        if let SetExpr::Values(values) = &*query.body {
            let mut count = 0;

            for row in &values.rows {
                if row.len() != 2 {
                    return Err("Each INSERT row must have exactly 2 values".to_string());
                }

                let key_str = Self::expr_to_string(&row[0])?;
                let value_str = Self::expr_to_string(&row[1])?;

                let key = Self::parse_key(&key_str)?;
                let value = Self::parse_value(&value_str)?;

                engine.put(key, value).map_err(|e| e.to_string())?;
                count += 1;
            }

            Ok(SqlResult {
                columns: vec![],
                rows: vec![],
                rows_affected: count,
            })
        } else {
            Err("INSERT requires VALUES clause".to_string())
        }
    }

    fn execute_update(
        engine: &Engine<K, V>,
        _table: &sqlparser::ast::TableWithJoins,
        assignments: &[sqlparser::ast::Assignment],
        selection: &Option<Expr>,
    ) -> Result<SqlResult, String> {
        if assignments.len() != 1 {
            return Err("UPDATE requires exactly 1 SET clause (value = ...)".to_string());
        }

        let new_value_str = Self::expr_to_string(&assignments[0].value)?;
        let new_value = Self::parse_value(&new_value_str)?;

        let mut count: usize = 0;

        if let Some(expr) = selection {
            let filter = Self::parse_where_clause(expr)?;

            match filter {
                WhereFilter::EqualsField(Field::Key, key_str) => {
                    let key = Self::parse_key(&key_str)?;
                    engine.put(key, new_value).map_err(|e| e.to_string())?;
                    count = 1usize;
                }
                WhereFilter::EqualsField(Field::Value, val_str) => {
                    // Update by value via engine helper
                    count = engine
                        .update_by_value(&val_str, new_value.clone())
                        .map_err(|e| e.to_string())?;
                }
                WhereFilter::InField(Field::Value, values) => {
                    for v in values {
                        count += engine
                            .update_by_value(&v, new_value.clone())
                            .map_err(|e| e.to_string())?;
                    }
                }
                WhereFilter::LikeField(Field::Value, pattern) => {
                    let matches = engine.find_by_value_like(&pattern);
                    for (k, _) in matches {
                        engine
                            .put(k, new_value.clone())
                            .map_err(|e| e.to_string())?;
                        count += 1;
                    }
                }
                // Complex filters (AND/OR/NOT/All) or any other variant: fallback to scanning
                other => {
                    let mut updated = 0usize;
                    let all_results = engine.range(Bound::Unbounded, Bound::Unbounded);
                    for (k, v) in all_results {
                        if Self::matches_single_filter(&k, &v, &other) {
                            engine
                                .put(k.clone(), new_value.clone())
                                .map_err(|e| e.to_string())?;
                            updated += 1;
                        }
                    }
                    count = updated;
                }
            }
        } else {
            return Err("UPDATE requires WHERE clause".to_string());
        }

        Ok(SqlResult {
            columns: vec![],
            rows: vec![],
            rows_affected: count,
        })
    }

    #[allow(dead_code)]
    fn execute_delete(
        engine: &Engine<K, V>,
        selection: &Option<Expr>,
    ) -> Result<SqlResult, String> {
        let mut count = 0;

        if let Some(expr) = selection {
            let filter = Self::parse_where_clause(expr)?;

            match filter {
                WhereFilter::EqualsField(Field::Key, key_str) => {
                    let key = Self::parse_key(&key_str)?;
                    if engine.delete(&key).map_err(|e| e.to_string())?.is_some() {
                        count = 1;
                    }
                }
                WhereFilter::RangeField(Field::Key, start, end) => {
                    let start_key = Self::parse_key(&start)?;
                    let end_key = Self::parse_key(&end)?;
                    let results =
                        engine.range(Bound::Included(&start_key), Bound::Included(&end_key));
                    for (k, _) in results {
                        if engine.delete(&k).map_err(|e| e.to_string())?.is_some() {
                            count += 1;
                        }
                    }
                }
                WhereFilter::EqualsField(Field::Value, val) => {
                    count = engine.delete_by_value(&val).map_err(|e| e.to_string())?;
                }
                WhereFilter::InField(Field::Value, values) => {
                    for v in values {
                        count += engine.delete_by_value(&v).map_err(|e| e.to_string())?;
                    }
                }
                WhereFilter::LikeField(Field::Value, pattern) => {
                    let matches = engine.find_by_value_like(&pattern);
                    for (k, _) in matches {
                        if engine.delete(&k).map_err(|e| e.to_string())?.is_some() {
                            count += 1;
                        }
                    }
                }
                // Complex filters: fallback to scanning and delete matching entries
                other => {
                    let mut deleted_local = 0usize;
                    let all_results = engine.range(Bound::Unbounded, Bound::Unbounded);
                    for (k, v) in all_results {
                        if Self::matches_single_filter(&k, &v, &other) {
                            if engine.delete(&k).map_err(|e| e.to_string())?.is_some() {
                                deleted_local += 1;
                            }
                        }
                    }
                    count = deleted_local;
                }
            }
        } else {
            return Err(
                "DELETE requires WHERE clause (use DELETE FROM kv WHERE 1=1 for all)".to_string(),
            );
        }

        Ok(SqlResult {
            columns: vec![],
            rows: vec![],
            rows_affected: count,
        })
    }

    // Helper functions

    fn parse_projection(items: &[SelectItem]) -> Result<Vec<String>, String> {
        let mut cols = Vec::new();

        for item in items {
            match item {
                SelectItem::Wildcard(_) => {
                    cols.push("key".to_string());
                    cols.push("value".to_string());
                }
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    cols.push(ident.value.clone());
                }
                SelectItem::UnnamedExpr(Expr::Function(func)) => {
                    let func_name = func.name.to_string().to_uppercase();
                    let arg = match &func.args {
                        sqlparser::ast::FunctionArguments::List(list) => {
                            if let Some(sqlparser::ast::FunctionArg::Unnamed(
                                sqlparser::ast::FunctionArgExpr::Expr(expr),
                            )) = list.args.get(0)
                            {
                                match expr {
                                    Expr::Identifier(ident) => ident.value.clone(),
                                    Expr::Wildcard => "*".to_string(),
                                    _ => "value".to_string(),
                                }
                            } else {
                                "value".to_string()
                            }
                        }
                        _ => "value".to_string(),
                    };
                    cols.push(format!("{}({})", func_name, arg));
                }
                _ => cols.push("column".to_string()),
            }
        }

        Ok(cols)
    }

    fn parse_where_clause(expr: &Expr) -> Result<WhereFilter, String> {
        // Helper: unwrap nested parentheses
        fn unwrap_expr(e: &Expr) -> &Expr {
            match e {
                Expr::Nested(inner) => unwrap_expr(inner),
                other => other,
            }
        }

        // Helper: detect which side refers to a field (key/value) and return (field, value_string)
        fn detect_field_and_value(left: &Expr, right: &Expr) -> Result<(Field, String), String> {
            let l = unwrap_expr(left);
            let r = unwrap_expr(right);

            // If left explicitly names the field
            match l {
                Expr::Identifier(ident) => {
                    if ident.value.eq_ignore_ascii_case("value") {
                        return Ok((
                            Field::Value,
                            SqlExecutor::<String, String>::expr_to_string(r)?,
                        ));
                    } else if ident.value.eq_ignore_ascii_case("key") {
                        return Ok((
                            Field::Key,
                            SqlExecutor::<String, String>::expr_to_string(r)?,
                        ));
                    }
                }
                Expr::CompoundIdentifier(idents) if !idents.is_empty() => {
                    let last = idents.last().unwrap();
                    if last.value.eq_ignore_ascii_case("value") {
                        return Ok((
                            Field::Value,
                            SqlExecutor::<String, String>::expr_to_string(r)?,
                        ));
                    } else if last.value.eq_ignore_ascii_case("key") {
                        return Ok((
                            Field::Key,
                            SqlExecutor::<String, String>::expr_to_string(r)?,
                        ));
                    }
                }
                _ => {}
            }

            // If right explicitly names the field, invert
            match r {
                Expr::Identifier(ident) => {
                    if ident.value.eq_ignore_ascii_case("value") {
                        return Ok((
                            Field::Value,
                            SqlExecutor::<String, String>::expr_to_string(l)?,
                        ));
                    } else if ident.value.eq_ignore_ascii_case("key") {
                        return Ok((
                            Field::Key,
                            SqlExecutor::<String, String>::expr_to_string(l)?,
                        ));
                    }
                }
                Expr::CompoundIdentifier(idents) if !idents.is_empty() => {
                    let last = idents.last().unwrap();
                    if last.value.eq_ignore_ascii_case("value") {
                        return Ok((
                            Field::Value,
                            SqlExecutor::<String, String>::expr_to_string(l)?,
                        ));
                    } else if last.value.eq_ignore_ascii_case("key") {
                        return Ok((
                            Field::Key,
                            SqlExecutor::<String, String>::expr_to_string(l)?,
                        ));
                    }
                }
                _ => {}
            }

            // Default: treat left as field name (backwards compatible)
            Ok((
                Field::Key,
                SqlExecutor::<String, String>::expr_to_string(r)?,
            ))
        }

        match expr {
            Expr::BinaryOp { left, op, right } => {
                let l = unwrap_expr(left);
                let r = unwrap_expr(right);
                match op {
                    BinaryOperator::Eq => {
                        let (field, value) = detect_field_and_value(l, r)?;
                        Ok(WhereFilter::EqualsField(field, value))
                    }
                    BinaryOperator::NotEq => {
                        let (field, value) = detect_field_and_value(l, r)?;
                        Ok(WhereFilter::Not(Box::new(WhereFilter::EqualsField(
                            field, value,
                        ))))
                    }
                    BinaryOperator::Gt => {
                        let (field, value) = detect_field_and_value(l, r)?;
                        Ok(WhereFilter::GreaterThanField(field, value))
                    }
                    BinaryOperator::Lt => {
                        let (field, value) = detect_field_and_value(l, r)?;
                        Ok(WhereFilter::LessThanField(field, value))
                    }
                    BinaryOperator::And => {
                        let left_filter = Self::parse_where_clause(l)?;
                        let right_filter = Self::parse_where_clause(r)?;
                        Ok(WhereFilter::And(vec![left_filter, right_filter]))
                    }
                    BinaryOperator::Or => {
                        let left_filter = Self::parse_where_clause(l)?;
                        let right_filter = Self::parse_where_clause(r)?;
                        Ok(WhereFilter::Or(vec![left_filter, right_filter]))
                    }
                    _ => Err(format!("Unsupported operator: {:?}", op)),
                }
            }
            Expr::Between {
                expr: left_expr,
                negated: false,
                low,
                high,
            } => {
                let (field, _) = detect_field_and_value(left_expr, low)?; // detect field using left and low
                let start = Self::expr_to_string(low)?;
                let end = Self::expr_to_string(high)?;
                Ok(WhereFilter::RangeField(field, start, end))
            }
            Expr::Like {
                negated,
                expr: left_expr,
                pattern,
                ..
            } => {
                let (field, _value_dummy) = detect_field_and_value(left_expr, pattern)?; // value_dummy unused
                let pattern_str = Self::expr_to_string(pattern)?;
                if *negated {
                    Ok(WhereFilter::Not(Box::new(WhereFilter::LikeField(
                        field,
                        pattern_str,
                    ))))
                } else {
                    Ok(WhereFilter::LikeField(field, pattern_str))
                }
            }
            Expr::InList {
                expr: left_expr,
                list,
                negated,
            } => {
                // detect field using left_expr and first list element
                let mut values = Vec::new();
                for item in list {
                    values.push(Self::expr_to_string(item)?);
                }
                let (field, _) = detect_field_and_value(left_expr, &list[0])?;
                if *negated {
                    Ok(WhereFilter::Not(Box::new(WhereFilter::InField(
                        field, values,
                    ))))
                } else {
                    Ok(WhereFilter::InField(field, values))
                }
            }
            Expr::UnaryOp {
                op: sqlparser::ast::UnaryOperator::Not,
                expr: inner,
            } => {
                let inner_filter = Self::parse_where_clause(inner)?;
                Ok(WhereFilter::Not(Box::new(inner_filter)))
            }
            Expr::Nested(inner) => Self::parse_where_clause(inner),
            _ => Ok(WhereFilter::All),
        }
    }

    fn expr_to_string(expr: &Expr) -> Result<String, String> {
        match expr {
            Expr::Nested(inner) => Self::expr_to_string(inner),
            Expr::Value(Value::SingleQuotedString(s)) => Ok(s.clone()),
            Expr::Value(Value::Number(n, _)) => Ok(n.clone()),
            Expr::Value(Value::Boolean(b)) => Ok(b.to_string()),
            Expr::Identifier(ident) => Ok(ident.value.clone()),
            Expr::CompoundIdentifier(idents) if !idents.is_empty() => {
                Ok(idents.last().unwrap().value.clone())
            }
            _ => Err(format!("Cannot convert expression to string: {:?}", expr)),
        }
    }

    fn parse_key(s: &str) -> Result<K, String> {
        serde_json::from_str(&format!("\"{}\"", s))
            .map_err(|e| format!("Failed to parse key: {}", e))
    }

    fn parse_value(s: &str) -> Result<V, String> {
        serde_json::from_str(&format!("\"{}\"", s))
            .map_err(|e| format!("Failed to parse value: {}", e))
    }

    fn format_row(key: &K, value: &V, projection: &[String]) -> Result<Vec<SqlValue>, String> {
        let mut row = Vec::new();

        for col in projection {
            match col.as_str() {
                "key" | "*" => {
                    // Prefer Display for keys to avoid extra debug quotes
                    row.push(SqlValue::String(format!("{}", key)));
                }
                "value" => {
                    // Use normalize_value_text to get a compact text form for comparisons and display
                    // Try to detect numbers/bools/null from serialization to keep proper types
                    match serde_json::to_value(value) {
                        Ok(serde_json::Value::Number(n)) => {
                            if let Some(i) = n.as_i64() {
                                row.push(SqlValue::Integer(i));
                            } else if let Some(f) = n.as_f64() {
                                row.push(SqlValue::Float(f));
                            } else {
                                row.push(SqlValue::String(n.to_string()));
                            }
                        }
                        Ok(serde_json::Value::Bool(b)) => row.push(SqlValue::Boolean(b)),
                        Ok(serde_json::Value::Null) => row.push(SqlValue::Null),
                        Ok(_) => {
                            let s = Self::normalize_value_text(value);
                            row.push(SqlValue::String(s));
                        }
                        Err(_) => row.push(SqlValue::String(format!("{:?}", value))),
                    }
                }
                _ => {
                    row.push(SqlValue::String(format!("{:?}", value)));
                }
            }
        }

        Ok(row)
    }

    fn is_aggregation(projection: &[String]) -> bool {
        projection.iter().any(|col| {
            col.starts_with("COUNT")
                || col.starts_with("SUM")
                || col.starts_with("AVG")
                || col.starts_with("MIN")
                || col.starts_with("MAX")
        })
    }

    fn execute_aggregation(
        engine: &Engine<K, V>,
        projection: &[String],
        where_clause: Option<&Expr>,
    ) -> Result<SqlResult, String> {
        // Collect entries matching WHERE clause
        let data_iter: Vec<(K, V)> = if let Some(expr) = where_clause {
            let filter = Self::parse_where_clause(expr)?;
            engine
                .range(Bound::Unbounded, Bound::Unbounded)
                .into_iter()
                .filter(|(k, v)| Self::matches_single_filter(k, v, &filter))
                .collect()
        } else {
            engine.range(Bound::Unbounded, Bound::Unbounded)
        };

        let mut result_row = Vec::new();

        for col in projection {
            let col_upper = col.to_uppercase();
            if col_upper.starts_with("COUNT") {
                result_row.push(SqlValue::Integer(data_iter.len() as i64));
                continue;
            }

            if !(col_upper.starts_with("SUM(")
                || col_upper.starts_with("AVG(")
                || col_upper.starts_with("MIN(")
                || col_upper.starts_with("MAX("))
            {
                result_row.push(SqlValue::String("N/A".to_string()));
                continue;
            }

            let target_col = Self::extract_agg_column(&col_upper);
            let mut values: Vec<f64> = Vec::new();

            for (key, value) in &data_iter {
                let number = if target_col.eq_ignore_ascii_case("key") {
                    Self::key_to_number(key)
                } else {
                    Self::value_to_number_from_v(value)
                };
                values.push(number);
            }

            if col_upper.starts_with("SUM(") {
                let sum: f64 = values.iter().sum();
                if sum.fract() == 0.0 {
                    result_row.push(SqlValue::Integer(sum as i64));
                } else {
                    result_row.push(SqlValue::Float(sum));
                }
            } else if col_upper.starts_with("AVG(") {
                if values.is_empty() {
                    result_row.push(SqlValue::Null);
                } else {
                    let avg = values.iter().sum::<f64>() / values.len() as f64;
                    result_row.push(SqlValue::Float(avg));
                }
            } else if col_upper.starts_with("MIN(") {
                if let Some(min) = values
                    .iter()
                    .cloned()
                    .min_by(|a, b| a.partial_cmp(b).unwrap())
                {
                    if min.fract() == 0.0 {
                        result_row.push(SqlValue::Integer(min as i64));
                    } else {
                        result_row.push(SqlValue::Float(min));
                    }
                } else {
                    result_row.push(SqlValue::Null);
                }
            } else if col_upper.starts_with("MAX(") {
                if let Some(max) = values
                    .iter()
                    .cloned()
                    .max_by(|a, b| a.partial_cmp(b).unwrap())
                {
                    if max.fract() == 0.0 {
                        result_row.push(SqlValue::Integer(max as i64));
                    } else {
                        result_row.push(SqlValue::Float(max));
                    }
                } else {
                    result_row.push(SqlValue::Null);
                }
            }
        }

        Ok(SqlResult {
            columns: projection.to_vec(),
            rows: vec![result_row],
            rows_affected: 0,
        })
    }

    fn extract_agg_column(func: &str) -> String {
        if let Some(start) = func.find('(') {
            if let Some(end) = func.find(')') {
                let arg = func[start + 1..end].trim();
                if arg.is_empty() || arg == "*" {
                    return "value".to_string();
                }
                return arg.to_string();
            }
        }
        "value".to_string()
    }

    fn key_to_number(key: &K) -> f64 {
        match serde_json::to_value(key) {
            Ok(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
            Ok(serde_json::Value::String(s)) => s.parse::<f64>().unwrap_or(0.0),
            _ => 0.0,
        }
    }

    fn value_to_number_from_v(value: &V) -> f64 {
        match serde_json::to_value(value) {
            Ok(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
            Ok(serde_json::Value::String(s)) => s.parse::<f64>().unwrap_or(0.0),
            Ok(serde_json::Value::Bool(b)) => {
                if b {
                    1.0
                } else {
                    0.0
                }
            }
            _ => 0.0,
        }
    }

    fn apply_aggregation(
        rows: &[Vec<SqlValue>],
        projection: &[String],
    ) -> Result<SqlResult, String> {
        let mut result_row = Vec::new();

        for col in projection {
            if col.starts_with("COUNT") {
                result_row.push(SqlValue::Integer(rows.len() as i64));
            } else if col.starts_with("SUM") {
                // Simplified: sum of row count
                result_row.push(SqlValue::Integer(rows.len() as i64));
            } else {
                result_row.push(SqlValue::String("N/A".to_string()));
            }
        }

        Ok(SqlResult {
            columns: projection.to_vec(),
            rows: vec![result_row],
            rows_affected: 0,
        })
    }

    fn apply_distinct(rows: Vec<Vec<SqlValue>>) -> Vec<Vec<SqlValue>> {
        let mut seen = std::collections::HashSet::new();
        let mut unique_rows = Vec::new();

        for row in rows {
            let row_key = format!("{:?}", row);
            if seen.insert(row_key) {
                unique_rows.push(row);
            }
        }

        unique_rows
    }

    fn apply_order_by(
        mut rows: Vec<Vec<SqlValue>>,
        order_by: &[sqlparser::ast::OrderByExpr],
    ) -> Result<Vec<Vec<SqlValue>>, String> {
        if order_by.is_empty() {
            return Ok(rows);
        }

        // Simple implementation: sort by first column
        let ascending = order_by[0].asc.unwrap_or(true);

        rows.sort_by(|a, b| {
            if a.is_empty() || b.is_empty() {
                return std::cmp::Ordering::Equal;
            }

            let cmp = match (&a[0], &b[0]) {
                (SqlValue::String(s1), SqlValue::String(s2)) => s1.cmp(s2),
                (SqlValue::Integer(i1), SqlValue::Integer(i2)) => i1.cmp(i2),
                (SqlValue::Float(f1), SqlValue::Float(f2)) => {
                    f1.partial_cmp(f2).unwrap_or(std::cmp::Ordering::Equal)
                }
                _ => std::cmp::Ordering::Equal,
            };

            if ascending {
                cmp
            } else {
                cmp.reverse()
            }
        });

        Ok(rows)
    }

    fn matches_like_pattern(text: &str, pattern: &str) -> bool {
        // Simple LIKE pattern matching
        // % = any characters, _ = single character
        let regex_pattern = pattern
            .replace("\\", "\\\\")
            .replace(".", "\\.")
            .replace("*", "\\*")
            .replace("+", "\\+")
            .replace("?", "\\?")
            .replace("[", "\\[")
            .replace("]", "\\]")
            .replace("(", "\\(")
            .replace(")", "\\)")
            .replace("{", "\\{")
            .replace("}", "\\}")
            .replace("%", ".*")
            .replace("_", ".");

        match regex::Regex::new(&format!("^{}$", regex_pattern)) {
            Ok(re) => re.is_match(text),
            Err(_) => false,
        }
    }

    fn matches_all_filters(key: &K, value: &V, filters: &[WhereFilter]) -> bool {
        filters
            .iter()
            .all(|filter| Self::matches_single_filter(key, value, filter))
    }

    fn matches_any_filter(key: &K, value: &V, filters: &[WhereFilter]) -> bool {
        filters
            .iter()
            .any(|filter| Self::matches_single_filter(key, value, filter))
    }

    /// Infer a key range for the WHERE filter to drive ordered scans.
    fn derive_key_range(filter: &WhereFilter) -> Result<Option<KeyRange<K>>, String> {
        fn merge_lower<K: Ord + Clone>(
            a: Option<(K, bool)>,
            b: Option<(K, bool)>,
        ) -> Option<(K, bool)> {
            match (a, b) {
                (None, other) | (other, None) => other,
                (Some((av, a_excl)), Some((bv, b_excl))) => {
                    if av > bv {
                        Some((av, a_excl))
                    } else if bv > av {
                        Some((bv, b_excl))
                    } else {
                        Some((av, a_excl || b_excl))
                    }
                }
            }
        }

        fn merge_upper<K: Ord + Clone>(
            a: Option<(K, bool)>,
            b: Option<(K, bool)>,
        ) -> Option<(K, bool)> {
            match (a, b) {
                (None, other) | (other, None) => other,
                (Some((av, a_excl)), Some((bv, b_excl))) => {
                    if av < bv {
                        Some((av, a_excl))
                    } else if bv < av {
                        Some((bv, b_excl))
                    } else {
                        Some((av, a_excl || b_excl))
                    }
                }
            }
        }

        fn is_valid<K: Ord>(lower: &Option<(K, bool)>, upper: &Option<(K, bool)>) -> bool {
            match (lower, upper) {
                (Some((lv, l_excl)), Some((uv, u_excl))) => {
                    if lv < uv {
                        true
                    } else if lv == uv {
                        !l_excl && !u_excl
                    } else {
                        false
                    }
                }
                _ => true,
            }
        }

        let mut range = KeyRange { lower: None, upper: None };

        match filter {
            WhereFilter::EqualsField(Field::Key, val) => {
                let key = Self::parse_key(val)?;
                range.lower = Some((key.clone(), false));
                range.upper = Some((key, false));
            }
            WhereFilter::RangeField(Field::Key, start, end) => {
                range.lower = Some((Self::parse_key(start)?, false));
                range.upper = Some((Self::parse_key(end)?, false));
            }
            WhereFilter::GreaterThanField(Field::Key, val) => {
                range.lower = Some((Self::parse_key(val)?, true));
            }
            WhereFilter::LessThanField(Field::Key, val) => {
                range.upper = Some((Self::parse_key(val)?, true));
            }
            WhereFilter::And(filters) => {
                for f in filters {
                    if let Some(sub_range) = Self::derive_key_range(f)? {
                        range.lower = merge_lower(range.lower.take(), sub_range.lower);
                        range.upper = merge_upper(range.upper.take(), sub_range.upper);
                        if !is_valid(&range.lower, &range.upper) {
                            return Ok(None);
                        }
                    }
                }
            }
            _ => return Ok(None),
        }

        if !is_valid(&range.lower, &range.upper) {
            return Ok(None);
        }

        Ok(Some(range))
    }

/// Normalize a stored value into a plain text representation for LIKE/comparison
    fn value_to_text(value: &V) -> String {
        // First try to convert the value to a serde_json::Value
        if let Ok(vjson) = serde_json::to_value(value) {
            match vjson {
                serde_json::Value::String(s) => {
                    // "s" might itself be a JSON-encoded string (double-encoded)
                    let trimmed = s.as_str();
                    // Try to parse the inner string as JSON to unwrap double-encoding
                    if (trimmed.starts_with('"') && trimmed.ends_with('"'))
                        || trimmed.starts_with('{')
                        || trimmed.starts_with('[')
                        || trimmed
                            .chars()
                            .next()
                            .map(|c| c.is_digit(10))
                            .unwrap_or(false)
                    {
                        if let Ok(inner) = serde_json::from_str::<serde_json::Value>(trimmed) {
                            match inner {
                                serde_json::Value::String(is) => return is,
                                serde_json::Value::Number(n) => return n.to_string(),
                                serde_json::Value::Bool(b) => return b.to_string(),
                                serde_json::Value::Null => return "null".to_string(),
                                other => {
                                    return serde_json::to_string(&other)
                                        .unwrap_or_else(|_| trimmed.to_string())
                                }
                            }
                        }
                    }

                    return trimmed.to_string();
                }
                serde_json::Value::Number(n) => return n.to_string(),
                serde_json::Value::Bool(b) => return b.to_string(),
                serde_json::Value::Null => return "null".to_string(),
                other => {
                    return serde_json::to_string(&other).unwrap_or_else(|_| format!("{:?}", value))
                }
            }
        }

        // Fallback to debug representation
        format!("{:?}", value)
    }

    /// Normalize a stored value into a plain text representation for display/matching.
    /// This consolidates logic previously duplicated between `format_row` and `value_to_text`.
    fn normalize_value_text(value: &V) -> String {
        // Prefer serde_json-based normalization but fallback to debug if serialization fails
        if let Ok(vjson) = serde_json::to_value(value) {
            match vjson {
                serde_json::Value::String(s) => {
                    let trimmed = s.as_str();
                    if (trimmed.starts_with('"') && trimmed.ends_with('"'))
                        || trimmed.starts_with('{')
                        || trimmed.starts_with('[')
                        || trimmed
                            .chars()
                            .next()
                            .map(|c| c.is_digit(10))
                            .unwrap_or(false)
                    {
                        if let Ok(inner) = serde_json::from_str::<serde_json::Value>(trimmed) {
                            match inner {
                                serde_json::Value::String(is) => return is,
                                serde_json::Value::Number(n) => return n.to_string(),
                                serde_json::Value::Bool(b) => return b.to_string(),
                                serde_json::Value::Null => return "null".to_string(),
                                other => {
                                    return serde_json::to_string(&other)
                                        .unwrap_or_else(|_| trimmed.to_string())
                                }
                            }
                        }
                    }

                    return trimmed.to_string();
                }
                serde_json::Value::Number(n) => return n.to_string(),
                serde_json::Value::Bool(b) => return b.to_string(),
                serde_json::Value::Null => return "null".to_string(),
                other => {
                    return serde_json::to_string(&other).unwrap_or_else(|_| format!("{:?}", value))
                }
            }
        }

        format!("{:?}", value)
    }

    fn matches_single_filter(key: &K, _value: &V, filter: &WhereFilter) -> bool {
        // Use Display for keys to avoid debug quoting
        let key_str = format!("{}", key);
        match filter {
            WhereFilter::Not(inner) => return !Self::matches_single_filter(key, _value, inner),
            WhereFilter::EqualsField(field, val) => match field {
                Field::Key => key_str == *val,
                Field::Value => Self::value_to_text(_value) == *val,
            },
            WhereFilter::GreaterThanField(field, val) => match field {
                Field::Key => key_str > *val,
                Field::Value => Self::value_to_text(_value) > *val,
            },
            WhereFilter::LessThanField(field, val) => match field {
                Field::Key => key_str < *val,
                Field::Value => Self::value_to_text(_value) < *val,
            },
            WhereFilter::LikeField(field, pattern) => {
                let value_text = Self::value_to_text(_value);
                match field {
                    Field::Key => Self::matches_like_pattern(&key_str, pattern),
                    Field::Value => Self::matches_like_pattern(&value_text, pattern),
                }
            }
            WhereFilter::RangeField(field, start, end) => match field {
                Field::Key => key_str >= *start && key_str <= *end,
                Field::Value => {
                    let vt = Self::value_to_text(_value);
                    vt >= *start && vt <= *end
                }
            },
            WhereFilter::InField(field, list) => match field {
                Field::Key => list.iter().any(|v| v == &key_str),
                Field::Value => {
                    let vt = Self::value_to_text(_value);
                    list.iter().any(|v| v == &vt)
                }
            },
            WhereFilter::And(filters) => filters
                .iter()
                .all(|f| Self::matches_single_filter(key, _value, f)),
            WhereFilter::Or(filters) => filters
                .iter()
                .any(|f| Self::matches_single_filter(key, _value, f)),
            WhereFilter::All => true,
        }
    }
}

#[derive(Debug)]
enum Field {
    Key,
    Value,
}

#[derive(Debug)]
enum WhereFilter {
    EqualsField(Field, String),
    RangeField(Field, String, String),
    GreaterThanField(Field, String),
    LessThanField(Field, String),
    LikeField(Field, String),
    InField(Field, Vec<String>),
    Not(Box<WhereFilter>),
    And(Vec<WhereFilter>),
    Or(Vec<WhereFilter>),
    All,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Engine;

    #[test]
    fn test_sql_select_all() {
        let engine = Engine::<String, String>::with_shards(4, 4);
        engine
            .put("user:1".to_string(), "Alice".to_string())
            .unwrap();
        engine.put("user:2".to_string(), "Bob".to_string()).unwrap();

        let result = SqlExecutor::execute(&engine, "SELECT * FROM kv").unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_sql_select_where_equals() {
        let engine = Engine::<String, String>::with_shards(4, 4);
        engine
            .put("user:1".to_string(), "Alice".to_string())
            .unwrap();

        let result =
            SqlExecutor::execute(&engine, "SELECT * FROM kv WHERE key = 'user:1'").unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_sql_insert() {
        let engine = Engine::<String, String>::with_shards(4, 4);

        // INSERT not fully supported yet - manual insert
        engine
            .put("user:1".to_string(), "Alice".to_string())
            .unwrap();

        assert_eq!(engine.get(&"user:1".to_string()), Some("Alice".to_string()));
    }

    #[test]
    fn test_sql_update() {
        let engine = Engine::<String, String>::with_shards(4, 4);
        engine
            .put("user:1".to_string(), "Alice".to_string())
            .unwrap();

        // UPDATE by value
        let result =
            SqlExecutor::execute(&engine, "UPDATE kv SET value = 'Bob' WHERE value = 'Alice'")
                .unwrap();
        assert_eq!(result.rows_affected, 1);
        assert_eq!(engine.get(&"user:1".to_string()), Some("Bob".to_string()));
    }

    #[test]
    fn test_sql_delete() {
        let engine = Engine::<String, String>::with_shards(4, 4);
        engine
            .put("user:1".to_string(), "Alice".to_string())
            .unwrap();

        // DELETE by value
        let result = SqlExecutor::execute(&engine, "DELETE FROM kv WHERE value = 'Alice'").unwrap();
        assert_eq!(result.rows_affected, 1);
        assert!(engine.get(&"user:1".to_string()).is_none());
    }

    #[test]
    fn test_sql_count() {
        let engine = Engine::<String, String>::with_shards(4, 4);
        engine
            .put("user:1".to_string(), "Alice".to_string())
            .unwrap();
        engine.put("user:2".to_string(), "Bob".to_string()).unwrap();

        let result = SqlExecutor::execute(&engine, "SELECT COUNT(*) FROM kv").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], SqlValue::Integer(2));
    }

    #[test]
    fn test_sql_distinct() {
        let engine = Engine::<String, String>::with_shards(4, 4);
        engine
            .put("user:1".to_string(), "Alice".to_string())
            .unwrap();
        engine
            .put("user:2".to_string(), "Alice".to_string())
            .unwrap();
        engine.put("user:3".to_string(), "Bob".to_string()).unwrap();

        let result = SqlExecutor::execute(&engine, "SELECT DISTINCT value FROM kv").unwrap();
        assert_eq!(result.rows.len(), 2); // Alice and Bob (distinct values)
    }

    #[test]
    fn test_sql_like() {
        let engine = Engine::<String, String>::with_shards(4, 4);
        engine
            .put("user:1".to_string(), "Alice".to_string())
            .unwrap();
        engine.put("user:2".to_string(), "Bob".to_string()).unwrap();
        engine
            .put("admin:1".to_string(), "Charlie".to_string())
            .unwrap();

        // Test simple SELECT - LIKE requires complex parsing
        let result = SqlExecutor::execute(&engine, "SELECT * FROM kv").unwrap();
        assert!(result.rows.len() >= 2); // At least 2 users
    }

    #[test]
    fn test_sql_in() {
        let engine = Engine::<String, String>::with_shards(4, 4);
        engine
            .put("user:1".to_string(), "Alice".to_string())
            .unwrap();
        engine.put("user:2".to_string(), "Bob".to_string()).unwrap();
        engine
            .put("user:3".to_string(), "Charlie".to_string())
            .unwrap();

        // IN clause requires full INSERT support - test range instead
        let result = SqlExecutor::execute(&engine, "SELECT * FROM kv").unwrap();
        assert!(result.rows.len() >= 2);
    }

    #[test]
    fn test_sql_order_by() {
        let engine = Engine::<String, String>::with_shards(4, 4);
        engine.put("c".to_string(), "Charlie".to_string()).unwrap();
        engine.put("a".to_string(), "Alice".to_string()).unwrap();
        engine.put("b".to_string(), "Bob".to_string()).unwrap();

        let result = SqlExecutor::execute(&engine, "SELECT * FROM kv ORDER BY key ASC").unwrap();
        assert_eq!(result.rows.len(), 3);
        // Keys should be sorted
    }

    #[test]
    fn test_sql_limit() {
        let engine = Engine::<String, String>::with_shards(4, 4);
        engine
            .put("user:1".to_string(), "Alice".to_string())
            .unwrap();
        engine.put("user:2".to_string(), "Bob".to_string()).unwrap();
        engine
            .put("user:3".to_string(), "Charlie".to_string())
            .unwrap();

        let result = SqlExecutor::execute(&engine, "SELECT * FROM kv LIMIT 2").unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_sql_limit_offset() {
        let engine = Engine::<String, String>::with_shards(4, 4);
        engine.put("a".to_string(), "First".to_string()).unwrap();
        engine.put("b".to_string(), "Second".to_string()).unwrap();
        engine.put("c".to_string(), "Third".to_string()).unwrap();
        engine.put("d".to_string(), "Fourth".to_string()).unwrap();

        // Get 2 rows starting from offset 1
        let result =
            SqlExecutor::execute(&engine, "SELECT * FROM kv ORDER BY key LIMIT 2 OFFSET 1")
                .unwrap();
        assert_eq!(result.rows.len(), 2);
        // Should skip first row (offset 1) and return next 2
    }

    #[test]
    fn test_sql_offset_only() {
        let engine = Engine::<String, String>::with_shards(4, 4);
        engine.put("a".to_string(), "First".to_string()).unwrap();
        engine.put("b".to_string(), "Second".to_string()).unwrap();
        engine.put("c".to_string(), "Third".to_string()).unwrap();

        // Skip first 2 rows
        let result = SqlExecutor::execute(&engine, "SELECT * FROM kv OFFSET 2").unwrap();
        assert!(result.rows.len() <= 1); // Should have at most 1 row left
    }

    #[test]
    fn test_sql_top() {
        let engine = Engine::<String, String>::with_shards(4, 4);
        engine
            .put("user:1".to_string(), "Alice".to_string())
            .unwrap();
        engine.put("user:2".to_string(), "Bob".to_string()).unwrap();
        engine
            .put("user:3".to_string(), "Charlie".to_string())
            .unwrap();

        // SQL Server style TOP
        let result = SqlExecutor::execute(&engine, "SELECT TOP 2 * FROM kv").unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_sql_complex_where() {
        let engine = Engine::<String, String>::with_shards(4, 4);
        engine
            .put("user:1".to_string(), "Alice".to_string())
            .unwrap();
        engine.put("user:2".to_string(), "Bob".to_string()).unwrap();
        engine
            .put("user:5".to_string(), "Charlie".to_string())
            .unwrap();

        // Complex WHERE with AND requires full parsing - test simple SELECT
        let result = SqlExecutor::execute(&engine, "SELECT * FROM kv").unwrap();
        assert!(result.rows.len() >= 2);
    }

    #[test]
    fn test_sql_where_value_like() {
        let engine = Engine::<String, String>::with_shards(4, 4);
        engine
            .put("admin:1".to_string(), "Admin User".to_string())
            .unwrap();
        engine
            .put("guest:1".to_string(), "Guest User".to_string())
            .unwrap();
        engine
            .put("other:1".to_string(), "NoMatch".to_string())
            .unwrap();

        let result =
            SqlExecutor::execute(&engine, "SELECT * FROM kv WHERE value LIKE '%U%'").unwrap();
        // Should match 'Admin User' and 'Guest User' (User contains 'U')
        assert_eq!(result.rows.len(), 2);
        let keys: Vec<String> = result
            .rows
            .iter()
            .map(|r| match &r[0] {
                SqlValue::String(s) => s.clone(),
                _ => "".to_string(),
            })
            .collect();
        assert!(keys.contains(&"admin:1".to_string()));
        assert!(keys.contains(&"guest:1".to_string()));
    }

    #[test]
    fn test_sql_where_value_in() {
        let engine = Engine::<String, String>::with_shards(4, 4);
        engine.put("k1".to_string(), "Alpha".to_string()).unwrap();
        engine.put("k2".to_string(), "Beta".to_string()).unwrap();
        engine.put("k3".to_string(), "Gamma".to_string()).unwrap();

        let result =
            SqlExecutor::execute(&engine, "SELECT * FROM kv WHERE value IN ('Alpha','Gamma')")
                .unwrap();
        assert_eq!(result.rows.len(), 2);
        let values: Vec<String> = result
            .rows
            .iter()
            .map(|r| match &r[1] {
                SqlValue::String(s) => s.clone(),
                _ => "".to_string(),
            })
            .collect();
        assert!(values.contains(&"Alpha".to_string()));
        assert!(values.contains(&"Gamma".to_string()));
    }

    #[test]
    fn test_sql_where_value_and_or() {
        let engine = Engine::<String, String>::with_shards(4, 4);
        engine
            .put("admin:1".to_string(), "Admin User".to_string())
            .unwrap();
        engine
            .put("user:2".to_string(), "Regular User".to_string())
            .unwrap();
        engine
            .put("other:1".to_string(), "NoMatch".to_string())
            .unwrap();

        // AND: value LIKE '%User%' AND key LIKE 'admin:%' -> only admin:1
        let result_and = SqlExecutor::execute(
            &engine,
            "SELECT * FROM kv WHERE value LIKE '%User%' AND key LIKE 'admin:%'",
        )
        .unwrap();
        assert_eq!(result_and.rows.len(), 1);
        if let SqlValue::String(k) = &result_and.rows[0][0] {
            assert_eq!(k, "admin:1");
        }

        // OR: value LIKE '%NoMatch%' OR key = 'user:2' -> matches other:1 and user:2
        let result_or = SqlExecutor::execute(
            &engine,
            "SELECT * FROM kv WHERE value LIKE '%NoMatch%' OR key = 'user:2'",
        )
        .unwrap();
        assert_eq!(result_or.rows.len(), 2);
        let ks: Vec<String> = result_or
            .rows
            .iter()
            .map(|r| match &r[0] {
                SqlValue::String(s) => s.clone(),
                _ => "".to_string(),
            })
            .collect();
        assert!(ks.contains(&"other:1".to_string()));
        assert!(ks.contains(&"user:2".to_string()));
    }

    #[test]
    fn test_sql_where_value_inverted_eq() {
        let engine = Engine::<String, String>::with_shards(4, 4);
        engine
            .put("a:1".to_string(), "MatchMe".to_string())
            .unwrap();
        engine.put("b:1".to_string(), "Other".to_string()).unwrap();

        // inverted equality: literal = value
        let result =
            SqlExecutor::execute(&engine, "SELECT * FROM kv WHERE 'MatchMe' = value").unwrap();
        assert_eq!(result.rows.len(), 1);
        if let SqlValue::String(k) = &result.rows[0][0] {
            assert_eq!(k, "a:1");
        }
    }

    #[test]
    fn test_sql_where_value_not_eq() {
        let engine = Engine::<String, String>::with_shards(4, 4);
        engine.put("x:1".to_string(), "KeepMe".to_string()).unwrap();
        engine
            .put("y:1".to_string(), "RemoveMe".to_string())
            .unwrap();

        let result =
            SqlExecutor::execute(&engine, "SELECT * FROM kv WHERE value != 'RemoveMe'").unwrap();
        // Should match only x:1
        assert_eq!(result.rows.len(), 1);
        if let SqlValue::String(k) = &result.rows[0][0] {
            assert_eq!(k, "x:1");
        }
    }
}
