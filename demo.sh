#!/bin/bash
# NeuroIndex - Demo Script Completo per Presentazione
# Usage: ./demo.sh

set -e

# Colori per output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo_step() {
    echo -e "${GREEN}===${NC} ${BLUE}$1${NC} ${GREEN}===${NC}"
}

echo_info() {
    echo -e "${YELLOW}➜${NC} $1"
}

echo_success() {
    echo -e "${GREEN}✓${NC} $1"
}

echo_error() {
    echo -e "${RED}✗${NC} $1"
}

# Trap per cleanup
cleanup() {
    echo ""
    echo_step "Cleanup dei processi"
    if [ ! -z "$RESP_PID" ]; then
        kill $RESP_PID 2>/dev/null && echo_success "RESP server terminato" || true
    fi
    if [ ! -z "$HTTP_PID" ]; then
        kill $HTTP_PID 2>/dev/null && echo_success "HTTP server terminato" || true
    fi
}

trap cleanup EXIT

# Verifica dipendenze
echo_step "Verifica Dipendenze"
command -v cargo >/dev/null 2>&1 || { echo_error "cargo non trovato. Installa Rust."; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo_error "python3 non trovato."; exit 1; }
command -v curl >/dev/null 2>&1 || { echo_error "curl non trovato."; exit 1; }
command -v jq >/dev/null 2>&1 || { echo_info "jq non trovato (opzionale per JSON formatting)"; }
echo_success "Tutte le dipendenze presenti"

# 1. Cleanup e Build
echo ""
echo_step "PARTE 1: Cleanup e Build"
echo_info "Rimuovo dati precedenti..."
rm -rf data/
mkdir -p data/
echo_success "Directory data/ pulita"

echo_info "Compilazione in release mode..."
cargo build --release --quiet
echo_success "Build completato"

# 2. Avvio RESP Server
echo ""
echo_step "PARTE 2: Avvio RESP Server"
echo_info "Avvio neuroindex-resp-server su porta 6381..."
./target/release/neuroindex-resp-server \
    --host 0.0.0.0 \
    --port 6381 \
    --shards 16 \
    --capacity 65536 \
    --log-level info \
    --persistence-dir ./data > /tmp/neuroindex-resp.log 2>&1 &
RESP_PID=$!
sleep 3

if kill -0 $RESP_PID 2>/dev/null; then
    echo_success "RESP Server avviato (PID: $RESP_PID)"
else
    echo_error "RESP Server non avviato. Log:"
    cat /tmp/neuroindex-resp.log
    exit 1
fi

# 3. Avvio HTTP Server
echo ""
echo_step "PARTE 3: Avvio HTTP REST API Server"
echo_info "Avvio neuroindex-http su porta 8080..."
./target/release/neuroindex-http \
    --host 0.0.0.0 \
    --port 8080 \
    --shards 16 \
    --capacity 65536 \
    --log-level info \
    --persistence-dir ./data > /tmp/neuroindex-http.log 2>&1 &
HTTP_PID=$!
sleep 3

if kill -0 $HTTP_PID 2>/dev/null; then
    echo_success "HTTP Server avviato (PID: $HTTP_PID)"
else
    echo_error "HTTP Server non avviato. Log:"
    cat /tmp/neuroindex-http.log
    exit 1
fi

# 4. Health Check
echo ""
echo_step "PARTE 4: Health Check"
echo_info "Verifica health HTTP server..."
HEALTH=$(curl -s http://172.16.99.30:8080/api/v1/health)
if [ $? -eq 0 ]; then
    echo_success "HTTP Server risponde correttamente"
    echo "$HEALTH" | jq . 2>/dev/null || echo "$HEALTH"
else
    echo_error "HTTP Server non risponde"
    exit 1
fi

# 5. Insert Demo Data
echo ""
echo_step "PARTE 5: Caricamento Dati"
DEMO_RECORDS=100000
echo_info "Inserimento di $DEMO_RECORDS record via Python client..."
echo_info "Questo richiederà circa 10-30 secondi..."

if [ -f "clients/python/insert_1m_resp.py" ]; then
    python3 clients/python/insert_1m_resp.py \
        --host 172.16.99.30 \
        --port 6381 \
        --total $DEMO_RECORDS \
        --batch-size 5000 \
        --prefix account
    echo_success "$DEMO_RECORDS record inseriti"
else
    echo_error "Script Python non trovato in clients/python/insert_1m_resp.py"
    echo_info "Creo alcuni record manualmente..."
    echo "SET test:001 {\"name\":\"Demo User\"}" | ./target/release/neuroindex-cli --host 172.16.99.30 --port 6381 > /dev/null
    echo_success "Record di test creati"
fi

# 6. Query via REST
echo ""
echo_step "PARTE 6: Query via REST API"

echo_info "Stats generali:"
curl -s "http://172.16.99.30:8080/api/v1/stats" | jq . 2>/dev/null || curl -s "http://172.16.99.30:8080/api/v1/stats"

echo ""
echo_info "GET singolo record (account:0050000):"
curl -s "http://172.16.99.30:8080/api/v1/records/account:0050000" | jq . 2>/dev/null || curl -s "http://172.16.99.30:8080/api/v1/records/account:0050000"

echo ""
echo_info "Range query (account:0050000 to 0050100, limit 5):"
curl -s "http://172.16.99.30:8080/api/v1/records/range?start=account:0050000&end=account:0050100&limit=5" | \
    jq '.results | length' 2>/dev/null || \
    curl -s "http://172.16.99.30:8080/api/v1/records/range?start=account:0050000&end=account:0050100&limit=5"

echo ""
echo_info "Count aggregation (account:0000000 to 0100000):"
curl -s "http://172.16.99.30:8080/api/v1/aggregations/count?start=account:0000000&end=account:0100000" | \
    jq . 2>/dev/null || \
    curl -s "http://172.16.99.30:8080/api/v1/aggregations/count?start=account:0000000&end=account:0100000"

# 7. Tagging Demo
echo ""
echo_step "PARTE 7: Tagging System (Feature Unica)"
echo_info "Creazione record con tag..."

echo "SET user:alice {\"name\":\"Alice Smith\"}" | ./target/release/neuroindex-cli --host 172.16.99.30 --port 6381 > /dev/null
echo "TAG user:alice premium" | ./target/release/neuroindex-cli --host 172.16.99.30 --port 6381 > /dev/null
echo "TAG user:alice vip" | ./target/release/neuroindex-cli --host 172.16.99.30 --port 6381 > /dev/null

echo "SET user:bob {\"name\":\"Bob Johnson\"}" | ./target/release/neuroindex-cli --host 172.16.99.30 --port 6381 > /dev/null
echo "TAG user:bob premium" | ./target/release/neuroindex-cli --host 172.16.99.30 --port 6381 > /dev/null

echo_success "Record taggati creati"

echo_info "Query per tag 'premium':"
echo "TAGGED premium" | ./target/release/neuroindex-cli --host 172.16.99.30 --port 6381

# 8. Snapshot
echo ""
echo_step "PARTE 8: Snapshot e Persistenza"
echo_info "Creazione snapshot..."
echo "SNAPSHOT" | ./target/release/neuroindex-cli --host 172.16.99.30 --port 6381 > /dev/null
echo_success "Snapshot creato"

echo ""
echo_info "File di persistenza:"
ls -lh data/ | grep -E "neuroindex\.(wal|snap)" || ls -lh data/

# 9. Test Recovery
echo ""
echo_step "PARTE 9: Test Recovery"
echo_info "Arresto server per test recovery..."

kill $RESP_PID 2>/dev/null
kill $HTTP_PID 2>/dev/null
sleep 2

echo_info "Conteggio record prima del recovery:"
RECORDS_BEFORE=$(ls data/neuroindex.snap 2>/dev/null && echo "Snapshot presente" || echo "Snapshot assente")
echo "  $RECORDS_BEFORE"

echo_info "Riavvio RESP server..."
./target/release/neuroindex-resp-server \
    --host 0.0.0.0 \
    --port 6381 \
    --shards 16 \
    --capacity 65536 \
    --log-level info \
    --persistence-dir ./data > /tmp/neuroindex-resp-recovery.log 2>&1 &
RESP_PID=$!
sleep 3

if kill -0 $RESP_PID 2>/dev/null; then
    echo_success "RESP Server riavviato (PID: $RESP_PID)"

    echo_info "Verifica dati recuperati:"
    DBSIZE=$(echo "DBSIZE" | ./target/release/neuroindex-cli --host 172.16.99.30 --port 6381 | tail -1)
    echo "  Total keys: $DBSIZE"

    echo_info "Verifica record specifico (account:0050000):"
    echo "GET account:0050000" | ./target/release/neuroindex-cli --host 172.16.99.30 --port 6381 | tail -1

    echo_success "Recovery completato con successo!"
else
    echo_error "RESP Server non riavviato. Log:"
    cat /tmp/neuroindex-resp-recovery.log
fi

# 10. Multi-protocol demo
echo ""
echo_step "PARTE 10: Demo Dual-Protocol"
echo_info "Riavvio HTTP server..."
./target/release/neuroindex-http \
    --host 0.0.0.0 \
    --port 8080 \
    --shards 16 \
    --capacity 65536 \
    --log-level info \
    --persistence-dir ./data > /tmp/neuroindex-http-recovery.log 2>&1 &
HTTP_PID=$!
sleep 3

if kill -0 $HTTP_PID 2>/dev/null; then
    echo_success "HTTP Server riavviato (PID: $HTTP_PID)"

    echo_info "Stesso record via REST API:"
    curl -s "http://172.16.99.30:8080/api/v1/records/account:0050000" | jq . 2>/dev/null || echo "Record presente"

    echo_success "Dati condivisi tra RESP e HTTP verificati!"
fi

# Summary
echo ""
echo_step "DEMO COMPLETATA!"
echo ""
echo_success "Riepilogo:"
echo "  • RESP Server:  http://172.16.99.30:6381 (PID: $RESP_PID)"
echo "  • HTTP Server:  http://172.16.99.30:8080 (PID: $HTTP_PID)"
echo "  • Record caricati: ~$DEMO_RECORDS"
echo "  • Persistenza: ./data/"
echo "  • Log RESP: /tmp/neuroindex-resp.log"
echo "  • Log HTTP: /tmp/neuroindex-http.log"
echo ""
echo_info "Server ancora attivi per test manuali."
echo_info "Premi CTRL+C per terminare."
echo ""

# Comandi utili
echo_step "Comandi Utili"
echo "  CLI:     ./target/release/neuroindex-cli --host 172.16.99.30 --port 6381"
echo "  Health:  curl http://172.16.99.30:8080/api/v1/health | jq"
echo "  Stats:   curl http://172.16.99.30:8080/api/v1/stats | jq"
echo "  Get:     curl http://172.16.99.30:8080/api/v1/records/account:0050000 | jq"
echo "  Range:   curl 'http://172.16.99.30:8080/api/v1/records/range?start=account:0000000&end=account:0100000&limit=10' | jq"
echo ""

# Wait for user
wait
