# INDEX
- [⚙️ PORT CONFIGURATION](#⚙️port-configuration)
  - [📍 Default Ports](#📍default-ports)
  - [🔧 How to Change the Ports](#🔧how-to-change-the-ports)
    - [Method 1: Edit Systemd File (Recommended)](#method-1-edit-systemd-file-recommended)
    - [Method 2: Systemd Override](#method-2-systemd-override)
    - [Method 3: Manual Startup](#method-3-manual-startup)
  - [🌐 Bind to Different Interfaces](#🌐bind-to-different-interfaces)
    - [Localhost Only (default - more secure)](#localhost-only-default-more-secure)
    - [All Interfaces (network accessible)](#all-interfaces-network-accessible)
    - [Specific IP](#specific-ip)
  - [📋 Complete Example](#📋complete-example)
  - [✅ Verification](#✅verification)


# ⚙️ PORT CONFIGURATION

## 📍 Default Ports

- **RESP Server**: 6381
- **HTTP Server**: 8080


## 🔧 How to Change the Ports

### Method 1: Edit Systemd File (Recommended)

```bash
# Edit RESP port
sudo systemctl edit --full neuroindex-resp

# Change the ExecStart line:
ExecStart=/usr/bin/neuroindex-resp-server --port 6379 --persistence-dir /var/lib/neuroindex

# Save and reload
sudo systemctl daemon-reload
sudo systemctl restart neuroindex-resp
```

```bash
# Edit HTTP port
sudo systemctl edit --full neuroindex-http

# Change the ExecStart line:
ExecStart=/usr/bin/neuroindex-http-server --port 8000 --persistence-dir /var/lib/neuroindex

# Save and reload
sudo systemctl daemon-reload
sudo systemctl restart neuroindex-http
```


### Method 2: Systemd Override

```bash
# Create RESP override
sudo systemctl edit neuroindex-resp
```

Insert:

```ini
[Service]
ExecStart=
ExecStart=/usr/bin/neuroindex-resp-server --port 6379 --persistence-dir /var/lib/neuroindex
```

Save and:

```bash
sudo systemctl daemon-reload
sudo systemctl restart neuroindex-resp
```


### Method 3: Manual Startup

```bash
# Stop the systemd service
sudo systemctl stop neuroindex-resp

# Manually launch with custom port
/usr/bin/neuroindex-resp-server --host 0.0.0.0 --port 7000 --persistence-dir /var/lib/neuroindex
```


## 🌐 Bind to Different Interfaces

### Localhost Only (default - more secure)

```bash
--host 127.0.0.1
```


### All Interfaces (network accessible)

```bash
--host 0.0.0.0
```


### Specific IP

```bash
--host 192.168.1.100
```


## 📋 Complete Example

```bash
# RESP on standard Redis port (6379), localhost only
sudo systemctl edit --full neuroindex-resp
```

Edit `ExecStart`:

```ini
ExecStart=/usr/bin/neuroindex-resp-server --host 127.0.0.1 --port 6379 --persistence-dir /var/lib/neuroindex
```

```bash
# HTTP on port 3000, network accessible
sudo systemctl edit --full neuroindex-http
```

Edit `ExecStart`:

```ini
ExecStart=/usr/bin/neuroindex-http-server --host 0.0.0.0 --port 3000 --persistence-dir /var/lib/neuroindex
```

Reload:

```bash
sudo systemctl daemon-reload
sudo systemctl restart neuroindex-resp neuroindex-http
```


## ✅ Verification

```bash
# Check listening ports
sudo lsof -i :6379
sudo lsof -i :3000

# Test
neuroindex-cli -p 6379 PING
curl http://localhost:3000/stats
```

