use clap::Parser;
use std::error::Error;
use std::io::{self, BufRead, BufReader, ErrorKind, Read, Write};
use std::net::TcpStream;
use std::str;

#[derive(Parser, Debug)]
#[command(name = "neuroindex-cli")]
#[command(about = "CLI client for NeuroIndex RESP server", long_about = None)]
struct Cli {
    /// Hostname or IP of the NeuroIndex RESP server
    #[arg(short, long, default_value = "127.0.0.1")]
    host: String,

    /// Port number of the NeuroIndex RESP server
    #[arg(short, long, default_value_t = 6381)]
    port: u16,

    /// Execute a single command (and exit)
    #[arg(trailing_var_arg = true)]
    command: Vec<String>,
}

enum RespValue {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

fn main() {
    if let Err(err) = run() {
        eprintln!("Error: {}", err);
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let address = format!("{}:{}", cli.host, cli.port);

    let mut stream = TcpStream::connect(&address)?;
    stream.set_nodelay(true)?;
    let reader_stream = stream.try_clone()?;
    let mut reader = BufReader::new(reader_stream);

    if cli.command.is_empty() {
        println!(
            "Connected to NeuroIndex RESP server at {}. Type 'help' for commands, 'quit' to exit.",
            address
        );
        interactive_loop(&mut stream, &mut reader)?;
    } else {
        execute_and_print(&mut stream, &mut reader, &cli.command)?;
    }

    Ok(())
}

fn interactive_loop(stream: &mut TcpStream, reader: &mut BufReader<TcpStream>) -> Result<(), Box<dyn Error>> {
    let mut input = String::new();
    loop {
        print!("neuroindex> ");
        io::stdout().flush()?;
        input.clear();

        if io::stdin().read_line(&mut input)? == 0 {
            break;
        }

        let trimmed = input.trim();
        if trimmed.is_empty() {
            continue;
        }

        if trimmed.eq_ignore_ascii_case("quit") || trimmed.eq_ignore_ascii_case("exit") {
            let _ = send_command(stream, &["QUIT".into()]);
            println!("Bye!");
            break;
        }

        if trimmed.eq_ignore_ascii_case("help") {
            print_help();
            continue;
        }

        let args = match parse_command_line(trimmed) {
            Ok(a) if !a.is_empty() => a,
            Ok(_) => continue,
            Err(err) => {
                eprintln!("Parse error: {}", err);
                continue;
            }
        };

        if let Err(err) = execute_and_print(stream, reader, &args) {
            eprintln!("ERR: {}", err);
        }
    }
    Ok(())
}

fn print_help() {
    println!("Available commands:");
    println!("  PING [message]          - Test connectivity");
    println!("  ECHO <message>          - Echo message back");
    println!("  GET <key>               - Fetch value for key");
    println!("  SET <key> <value>       - Store value");
    println!("  MGET <key> [key ...]    - Fetch multiple keys");
    println!("  MSET <key> <value> [...]- Store multiple key/value pairs");
    println!("  DEL <key> [key ...]     - Delete keys");
    println!("  EXISTS <key> [key ...]  - Check presence");
    println!("  KEYS [pattern]          - List keys (pattern optional)");
    println!("  DBSIZE                  - Count total keys");
    println!("  INFO                    - Server stats");
    println!("  FLUSHDB                 - Delete all keys");
    println!("  FLUSHWAL                - Force WAL flush to disk");
    println!("  SNAPSHOT                - Write snapshot (requires persistence)");
    println!("  QUIT                    - Close the session");
}

fn execute_and_print(
    stream: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    args: &[String],
) -> Result<(), Box<dyn Error>> {
    send_command(stream, args)?;
    let value = read_resp(reader)?;
    print_resp(&value);
    Ok(())
}

fn send_command(stream: &mut TcpStream, args: &[String]) -> io::Result<()> {
    let payload = encode_command(args);
    stream.write_all(&payload)
}

fn encode_command(args: &[String]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for arg in args {
        let bytes = arg.as_bytes();
        out.extend_from_slice(format!("${}\r\n", bytes.len()).as_bytes());
        out.extend_from_slice(bytes);
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn read_resp(reader: &mut BufReader<TcpStream>) -> Result<RespValue, Box<dyn Error>> {
    let mut prefix = [0u8; 1];
    reader.read_exact(&mut prefix)?;
    match prefix[0] {
        b'+' => {
            let line = read_line(reader)?;
            Ok(RespValue::Simple(line))
        }
        b'-' => {
            let line = read_line(reader)?;
            Ok(RespValue::Error(line))
        }
        b':' => {
            let line = read_line(reader)?;
            let value = line.parse::<i64>().map_err(|_| io::Error::new(ErrorKind::InvalidData, "invalid integer"))?;
            Ok(RespValue::Integer(value))
        }
        b'$' => {
            let line = read_line(reader)?;
            let len = line.parse::<isize>().map_err(|_| io::Error::new(ErrorKind::InvalidData, "invalid bulk length"))?;
            if len == -1 {
                Ok(RespValue::Bulk(None))
            } else {
                let mut buf = vec![0u8; len as usize];
                reader.read_exact(&mut buf)?;
                expect_crlf(reader)?;
                Ok(RespValue::Bulk(Some(buf)))
            }
        }
        b'*' => {
            let line = read_line(reader)?;
            let len = line.parse::<isize>().map_err(|_| io::Error::new(ErrorKind::InvalidData, "invalid array length"))?;
            if len == -1 {
                Ok(RespValue::Array(None))
            } else {
                let mut items = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    items.push(read_resp(reader)?);
                }
                Ok(RespValue::Array(Some(items)))
            }
        }
        _ => Err(Box::new(io::Error::new(
            ErrorKind::InvalidData,
            "unknown RESP prefix",
        ))),
    }
}

fn read_line(reader: &mut BufReader<TcpStream>) -> io::Result<String> {
    let mut buf = Vec::new();
    reader.read_until(b'\n', &mut buf)?;
    if buf.len() < 2 || buf[buf.len() - 2] != b'\r' {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "line missing CRLF terminator",
        ));
    }
    buf.truncate(buf.len() - 2);
    String::from_utf8(buf).map_err(|_| io::Error::new(ErrorKind::InvalidData, "invalid UTF-8 in line"))
}

fn expect_crlf(reader: &mut BufReader<TcpStream>) -> io::Result<()> {
    let mut buf = [0u8; 2];
    reader.read_exact(&mut buf)?;
    if buf != [b'\r', b'\n'] {
        return Err(io::Error::new(ErrorKind::InvalidData, "missing CRLF"));
    }
    Ok(())
}

fn print_resp(value: &RespValue) {
    print_resp_inner(value, 0);
}

fn print_resp_inner(value: &RespValue, indent: usize) {
    match value {
        RespValue::Array(Some(items)) => {
            if items.is_empty() {
                println!("{}(empty array)", spaces(indent));
            } else {
                for (idx, item) in items.iter().enumerate() {
                    let prefix = format!("{}{}) ", spaces(indent), idx + 1);
                    match item {
                        RespValue::Array(Some(_)) => {
                            println!("{}", prefix);
                            print_resp_inner(item, indent + 4);
                        }
                        RespValue::Array(None) => println!("{}(null array)", prefix.trim_end()),
                        _ => println!("{}{}", prefix, format_scalar(item)),
                    }
                }
            }
        }
        RespValue::Array(None) => println!("{}(null array)", spaces(indent)),
        _ => println!("{}{}", spaces(indent), format_scalar(value)),
    }
}

fn format_scalar(value: &RespValue) -> String {
    match value {
        RespValue::Simple(s) => s.clone(),
        RespValue::Error(e) => format!("(error) {}", e),
        RespValue::Integer(i) => format!("(integer) {}", i),
        RespValue::Bulk(Some(bytes)) => match String::from_utf8(bytes.clone()) {
            Ok(s) => s,
            Err(_) => format!("(binary {} bytes)", bytes.len()),
        },
        RespValue::Bulk(None) => "(nil)".to_string(),
        RespValue::Array(Some(_)) => "(array)".to_string(),
        RespValue::Array(None) => "(null array)".to_string(),
    }
}

fn spaces(n: usize) -> String {
    " ".repeat(n)
}

fn parse_command_line(input: &str) -> Result<Vec<String>, String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut chars = input.chars().peekable();
    let mut in_quote: Option<char> = None;
    let mut escape = false;

    while let Some(ch) = chars.next() {
        if escape {
            current.push(ch);
            escape = false;
            continue;
        }

        match ch {
            '\\' => {
                escape = true;
            }
            '\'' | '"' => {
                if let Some(q) = in_quote {
                    if q == ch {
                        in_quote = None;
                    } else {
                        current.push(ch);
                    }
                } else {
                    in_quote = Some(ch);
                }
            }
            c if c.is_whitespace() && in_quote.is_none() => {
                if !current.is_empty() {
                    args.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(ch),
        }
    }

    if escape {
        return Err("unterminated escape sequence".into());
    }

    if in_quote.is_some() {
        return Err("unterminated quoted string".into());
    }

    if !current.is_empty() {
        args.push(current);
    }

    Ok(args)
}
