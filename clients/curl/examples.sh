#!/bin/bash
#
# NeuroIndex curl Examples
# Using telnet/netcat to send RESP protocol commands
#
# Note: curl doesn't support RESP protocol directly,
# so we use netcat (nc) or telnet for raw TCP communication
#

HOST="localhost"
PORT="6381"

echo "========================================"
echo "NeuroIndex curl/netcat Examples"
echo "========================================"
echo ""

# Function to send RESP command
send_resp() {
    local cmd="$1"
    echo -ne "$cmd" | nc $HOST $PORT
    echo ""
}

echo "1. PING command"
echo "----------------"
send_resp "*1\r\n\$4\r\nPING\r\n"

echo ""
echo "2. SET command (SET mykey 'Hello NeuroIndex')"
echo "----------------"
send_resp "*3\r\n\$3\r\nSET\r\n\$5\r\nmykey\r\n\$16\r\nHello NeuroIndex\r\n"

echo ""
echo "3. GET command (GET mykey)"
echo "----------------"
send_resp "*2\r\n\$3\r\nGET\r\n\$5\r\nmykey\r\n"

echo ""
echo "4. MSET command (MSET key1 val1 key2 val2)"
echo "----------------"
send_resp "*5\r\n\$4\r\nMSET\r\n\$4\r\nkey1\r\n\$4\r\nval1\r\n\$4\r\nkey2\r\n\$4\r\nval2\r\n"

echo ""
echo "5. MGET command (MGET key1 key2)"
echo "----------------"
send_resp "*3\r\n\$4\r\nMGET\r\n\$4\r\nkey1\r\n\$4\r\nkey2\r\n"

echo ""
echo "6. EXISTS command (EXISTS key1 key2)"
echo "----------------"
send_resp "*3\r\n\$6\r\nEXISTS\r\n\$4\r\nkey1\r\n\$4\r\nkey2\r\n"

echo ""
echo "7. DEL command (DEL key1)"
echo "----------------"
send_resp "*2\r\n\$3\r\nDEL\r\n\$4\r\nkey1\r\n"

echo ""
echo "8. DBSIZE command"
echo "----------------"
send_resp "*1\r\n\$6\r\nDBSIZE\r\n"

echo ""
echo "9. KEYS command"
echo "----------------"
send_resp "*1\r\n\$4\r\nKEYS\r\n"

echo ""
echo "10. INFO command"
echo "----------------"
send_resp "*1\r\n\$4\r\nINFO\r\n"

echo ""
echo "========================================"
echo "✓ Examples completed!"
echo "========================================"
