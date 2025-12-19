# Azure Blob Storage Persistence Layer

This module provides Python file-like interfaces and pathlib-style path abstractions for Azure Blob Storage.

## Features

### AzureBlobFile
- Python file handle interface for Azure blobs
- Streaming reads with configurable chunk size (default 4MB)
- Efficient append mode using Azure AppendBlobs
- Context manager support
- Text and binary modes

### AzureBlobPath
- pathlib.Path-like interface for Azure blobs
- Shared client configuration for efficient resource usage
- Navigation with `/` operator
- Blob operations: copy, move, delete, rename
- Directory-like operations: iterdir, glob, mkdir
- Metadata and content type management

## Installation

```bash
pip install azure-storage-blob
```

## Quick Start

### Using AzureBlobFile

```python
from flowlet.persistence import AzureBlobFile, open_azure_blob

# Write to a blob
with open_azure_blob(conn_str, 'container', 'file.txt', 'w') as f:
    f.write('Hello, Azure!')

# Read from a blob (streaming)
with open_azure_blob(conn_str, 'container', 'large-file.txt', 'r') as f:
    for line in f:
        process(line)  # Only loads chunks as needed

# Efficient append mode
with open_azure_blob(conn_str, 'logs', 'app.log', 'a') as f:
    f.write('New log entry\n')  # Uses append_block() if AppendBlob
```

### Using AzureBlobPath

```python
from flowlet.persistence import AzureBlobPath

# Create a path
root = AzureBlobPath.from_connection_string(
    connection_string=conn_str,
    container='my-container'
)

# Navigate like pathlib
data_dir = root / 'data'
file_path = data_dir / 'file.txt'

# File operations
file_path.write_text('Hello, Azure!')
content = file_path.read_text()

# Check existence
if file_path.exists():
    print(f"Size: {file_path.get_size()} bytes")

# List directory
for blob in data_dir.iterdir():
    print(f"- {blob.name}")

# Glob patterns
for json_file in root.glob('**/*.json'):
    process(json_file)

# Copy/Move operations
backup_path = root / 'backup' / 'file.txt'
file_path.copy_to(backup_path)
file_path.move_to(root / 'archive' / 'file.txt')

# Metadata operations
file_path.set_metadata({'author': 'system', 'version': '1.0'})
metadata = file_path.get_metadata()
```

## Shared Client Configuration

Both `AzureBlobFile` and `AzureBlobPath` support shared `BlobServiceClient` instances for efficient resource usage:

```python
from azure.storage.blob import BlobServiceClient
from flowlet.persistence import AzureBlobPath

# Create shared client
blob_service_client = BlobServiceClient.from_connection_string(conn_str)

# All paths share the same client
root = AzureBlobPath.from_service_client(
    blob_service_client,
    container='container1'
)

# Child paths automatically inherit the client
file1 = root / 'file1.txt'
file2 = root / 'file2.txt'

# When navigating, the client is shared
subdir = root / 'subdir'
file3 = subdir / 'file3.txt'  # Still uses the same blob_service_client
```

## Advanced Usage

### Streaming Large Files

```python
from flowlet.persistence import open_azure_blob

# Stream with custom chunk size
with open_azure_blob(conn_str, 'data', 'huge.bin', 'rb', chunk_size=1024*1024) as f:
    while True:
        chunk = f.read(1024 * 1024)  # Read 1MB at a time
        if not chunk:
            break
        process_chunk(chunk)
```

### Efficient Append Operations

```python
from flowlet.persistence import open_azure_blob

# AppendBlob for efficient log appending
with open_azure_blob(conn_str, 'logs', 'app.log', 'a') as f:
    f.write('Log entry 1\n')
    f.write('Log entry 2\n')
    # Data is buffered and flushed in 4MB chunks using append_block()

    # Check if using efficient mode
    if f.is_append_blob:
        print("Using native AppendBlob - efficient!")
    elif f.is_degraded_append_mode:
        print("Warning: Using degraded mode (BlockBlob)")
```

### Directory Operations

```python
from flowlet.persistence import AzureBlobPath

root = AzureBlobPath.from_connection_string(conn_str, container='data')

# Create directory structure
logs_dir = root / 'logs' / '2024' / '12'
logs_dir.mkdir(parents=True, exist_ok=True)

# List all files recursively
for blob in root.iterdir(recursive=True):
    print(blob.as_posix())

# Delete directory and all contents
logs_dir.rmdir(recursive=True)
```

### Working with Metadata

```python
from flowlet.persistence import AzureBlobPath

file_path = AzureBlobPath.from_connection_string(
    conn_str,
    container='data',
    path='document.pdf'
)

# Set content type
file_path.set_content_type('application/pdf')

# Set custom metadata
file_path.set_metadata({
    'author': 'John Doe',
    'department': 'Engineering',
    'version': '1.2.3'
})

# Read metadata
metadata = file_path.get_metadata()
content_type = file_path.get_content_type()
```

### Path Operations

```python
from flowlet.persistence import AzureBlobPath

# Path properties
path = AzureBlobPath.from_connection_string(
    conn_str,
    container='data',
    path='reports/2024/Q4/summary.pdf'
)

print(path.name)       # 'summary.pdf'
print(path.stem)       # 'summary'
print(path.suffix)     # '.pdf'
print(path.parent)     # AzureBlobPath(..., path='reports/2024/Q4')
print(path.parts)      # ('data', 'reports', '2024', 'Q4', 'summary.pdf')
print(path.as_posix()) # 'data/reports/2024/Q4/summary.pdf'
print(str(path))       # 'az://data/reports/2024/Q4/summary.pdf'
```

## Performance Considerations

### Streaming vs. Full Load

**Streaming Read (Recommended for large files):**
```python
# Only loads 4MB chunks as needed
with open_azure_blob(conn_str, 'data', 'large.csv', 'r') as f:
    for line in f:
        process(line)
```

**Full Load (Faster for small files):**
```python
# Loads entire blob at once
path = AzureBlobPath.from_connection_string(conn_str, 'data', 'small.txt')
content = path.read_text()
```

### Append Mode Performance

| Blob Type   | Mode         | Performance                              |
|-------------|--------------|------------------------------------------|
| AppendBlob  | Append ('a') | ⚡ **Efficient** - Direct append_block() |
| BlockBlob   | Append ('a') | ⚠️ Degraded - Read all, write all        |
| New Blob    | Append ('a') | ⚡ Creates AppendBlob automatically       |

### Client Sharing Best Practices

```python
from azure.storage.blob import BlobServiceClient
from flowlet.persistence import AzureBlobPath

# ✅ GOOD: Share one client across operations
client = BlobServiceClient.from_connection_string(conn_str)
root = AzureBlobPath.from_service_client(client, 'container')

for i in range(100):
    file = root / f'file{i}.txt'
    file.write_text(f'Content {i}')  # Reuses same client

# ❌ BAD: Creating new client each time
for i in range(100):
    path = AzureBlobPath.from_connection_string(conn_str, 'container', f'file{i}.txt')
    path.write_text(f'Content {i}')  # New client each iteration!
```

## API Reference

### AzureBlobFile

File-like interface for Azure Blob Storage.

**Methods:**
- `read(size=-1)` - Read bytes/text from blob
- `readline(size=-1)` - Read single line
- `readlines(hint=-1)` - Read all lines
- `write(data)` - Write bytes/text to blob
- `writelines(lines)` - Write multiple lines
- `seek(offset, whence=0)` - Change position
- `tell()` - Get current position
- `flush()` - Upload buffered data
- `close()` - Close and upload

**Properties:**
- `closed` - Whether file is closed
- `name` - Blob name
- `is_append_blob` - Using AppendBlob
- `is_degraded_append_mode` - Using degraded append

### AzureBlobPath

Path-like interface for Azure Blob Storage.

**Methods:**
- `open(mode, encoding, chunk_size)` - Open blob as file
- `read_bytes()` - Read blob as bytes
- `read_text(encoding)` - Read blob as text
- `write_bytes(data)` - Write bytes to blob
- `write_text(text, encoding)` - Write text to blob
- `exists()` - Check if blob exists
- `is_file()` - Check if blob (file) exists
- `is_dir()` - Check if prefix (directory) exists
- `iterdir(recursive)` - Iterate over blobs
- `glob(pattern)` - Find blobs matching pattern
- `delete(missing_ok)` - Delete blob
- `copy_to(destination, overwrite)` - Copy blob
- `move_to(destination, overwrite)` - Move blob
- `rename(new_name)` - Rename blob
- `mkdir(parents, exist_ok)` - Create directory marker
- `rmdir(recursive)` - Delete directory
- `stat()` - Get blob properties
- `get_size()` - Get blob size
- `get_metadata()` / `set_metadata(metadata)` - Manage metadata
- `get_content_type()` / `set_content_type(content_type)` - Manage content type

**Properties:**
- `name` - Final path component
- `parent` - Parent directory path
- `parts` - Path components tuple
- `suffix` - File extension
- `stem` - File name without extension

## Error Handling

```python
from flowlet.persistence import AzureBlobPath

path = AzureBlobPath.from_connection_string(conn_str, 'container', 'file.txt')

try:
    content = path.read_text()
except FileNotFoundError:
    print("Blob does not exist")

# Alternatively, check first
if path.exists():
    content = path.read_text()

# Safe delete
path.delete(missing_ok=True)  # Won't raise if doesn't exist
```

## Migration from Local Filesystem

The API is designed to be similar to Python's built-in `open()` and `pathlib.Path`:

```python
# Local filesystem
from pathlib import Path
path = Path('data') / 'file.txt'
path.write_text('Hello')
content = path.read_text()

# Azure Blob Storage (nearly identical!)
from flowlet.persistence import AzureBlobPath
path = AzureBlobPath.from_connection_string(conn_str, 'container', 'data') / 'file.txt'
path.write_text('Hello')
content = path.read_text()
```
