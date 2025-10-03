#!/bin/bash

set -e

# Parse command line arguments
FULL_RELEASE=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --full)
            FULL_RELEASE=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--full]"
            exit 1
            ;;
    esac
done

# Source version configuration
if [ ! -f "version.ini" ]; then
    echo "Error: version.ini not found"
    exit 1
fi

# Source the version file - need to handle variable expansion
eval "$(cat version.ini | sed 's/^/export /')"

# Build all cross-platform binaries
echo "Building cross-platform binaries..."
make cross

# Determine version suffix
if [ "$FULL_RELEASE" = true ]; then
    VERSION_SUFFIX="$RELEASE_VERSION"
    echo "Creating FULL RELEASE with version: $VERSION_SUFFIX"
else
    VERSION_SUFFIX="$CANDIDATE_VERSION"
    echo "Creating RELEASE CANDIDATE with version: $VERSION_SUFFIX"
fi

# Rename binaries to include version
echo "Renaming binaries with version suffix..."

# List of platforms built by make cross
platforms=(
    "darwin-amd64"
    "darwin-arm64"
    "windows-amd64.exe"
    "windows-arm64.exe"
    "linux-amd64"
    "linux-arm64"
)

for platform in "${platforms[@]}"; do
    # Handle server binary
    server_src="build/insid-${platform}"
    if [ -f "$server_src" ]; then
        # Extract extension if present (for windows .exe)
        if [[ $platform == *.exe ]]; then
            base_platform="${platform%.exe}"
            ext=".exe"
        else
            base_platform="$platform"
            ext=""
        fi

        server_dest="build/insid-${VERSION_SUFFIX}-${base_platform}${ext}"
        echo "Renaming $server_src -> $server_dest"
        mv "$server_src" "$server_dest"
    fi

    # Handle client binary
    client_src="build/insic-${platform}"
    if [ -f "$client_src" ]; then
        # Extract extension if present (for windows .exe)
        if [[ $platform == *.exe ]]; then
            base_platform="${platform%.exe}"
            ext=".exe"
        else
            base_platform="$platform"
            ext=""
        fi

        client_dest="build/insic-${VERSION_SUFFIX}-${base_platform}${ext}"
        echo "Renaming $client_src -> $client_dest"
        mv "$client_src" "$client_dest"
    fi
done

echo "Release build complete! Binaries available in build/ directory:"
ls -la build/ | grep -E "(insid|insic)-"
