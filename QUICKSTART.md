# Insula Quickstart Guide

This guide provides quick installation instructions for Insula binaries across different platforms.

## Prerequisites

- `curl` installed on your system
- Appropriate permissions to write to `/usr/local/bin` (or your preferred installation directory)

## Installation by Platform

### macOS (Darwin)

#### Intel (AMD64)
```bash
# Download insic client
curl -L -o insic https://github.com/InsulaLabs/insi/releases/download/alpha-rc.0-r3/insic-darwin-amd64.alpha-rc.0-r3
chmod +x insic
sudo mv insic /usr/local/bin/

# Download insid daemon
curl -L -o insid https://github.com/InsulaLabs/insi/releases/download/alpha-rc.0-r3/insid-darwin-amd64.alpha-rc.0-r3
chmod +x insid
sudo mv insid /usr/local/bin/
```

#### Apple Silicon (ARM64)
```bash
# Download insic client
curl -L -o insic https://github.com/InsulaLabs/insi/releases/download/alpha-rc.0-r3/insic-darwin-arm64.alpha-rc.0-r3
chmod +x insic
sudo mv insic /usr/local/bin/

# Download insid daemon
curl -L -o insid https://github.com/InsulaLabs/insi/releases/download/alpha-rc.0-r3/insid-darwin-arm64.alpha-rc.0-r3
chmod +x insid
sudo mv insid /usr/local/bin/
```

### Linux

#### Intel (AMD64)
```bash
# Download insic client
curl -L -o insic https://github.com/InsulaLabs/insi/releases/download/alpha-rc.0-r3/insic-linux-amd64.alpha-rc.0-r3
chmod +x insic
sudo mv insic /usr/local/bin/

# Download insid daemon
curl -L -o insid https://github.com/InsulaLabs/insi/releases/download/alpha-rc.0-r3/insid-linux-amd64.alpha-rc.0-r3
chmod +x insid
sudo mv insid /usr/local/bin/
```

#### ARM64
```bash
# Download insic client
curl -L -o insic https://github.com/InsulaLabs/insi/releases/download/alpha-rc.0-r3/insic-linux-arm64.alpha-rc.0-r3
chmod +x insic
sudo mv insic /usr/local/bin/

# Download insid daemon
curl -L -o insid https://github.com/InsulaLabs/insi/releases/download/alpha-rc.0-r3/insid-linux-arm64.alpha-rc.0-r3
chmod +x insid
sudo mv insid /usr/local/bin/
```

### Windows

#### Intel (AMD64)
```cmd
# Download insic client
curl -L -o insic.exe https://github.com/InsulaLabs/insi/releases/download/alpha-rc.0-r3/insic-windows-amd64.alpha-rc.0-r3.exe

# Download insid daemon
curl -L -o insid.exe https://github.com/InsulaLabs/insi/releases/download/alpha-rc.0-r3/insid-windows-amd64.alpha-rc.0-r3.exe
```

#### ARM64
```cmd
# Download insic client
curl -L -o insic.exe https://github.com/InsulaLabs/insi/releases/download/alpha-rc.0-r3/insic-windows-arm64.alpha-rc.0-r3.exe

# Download insid daemon
curl -L -o insid.exe https://github.com/InsulaLabs/insi/releases/download/alpha-rc.0-r3/insid-windows-arm64.alpha-rc.0-r3.exe
```

## Verification

After installation, verify the binaries are working:

```bash
# Check insic version
insic --version

# Check insid version
insid --version
```

## Next Steps

- Start the daemon: `insid`
- Use the client: `insic --help`
- Refer to the main documentation for configuration and usage details
