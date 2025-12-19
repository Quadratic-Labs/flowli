# Azure Blob Storage Persistence Layer

## Overview

The Flowlet persistence layer provides a comprehensive, production-ready interface for working with Azure Blob Storage. It offers two complementary APIs:

1. **AzureBlobFile** - Python file handle interface for reading and writing blobs
2. **AzureBlobPath** - pathlib-style interface for blob navigation and operations

Both APIs are designed to be drop-in replacements for standard Python file I/O and pathlib operations, making it easy to migrate between local filesystem and Azure Blob Storage.

## Architecture

### Key Design Principles

1. **Streaming by Default**: Files are read in chunks (default 4MB) to minimize memory usage
2. **Efficient Appending**: Native AppendBlob support for log files and append-heavy workloads
3. **Shared Client Configuration**: BlobServiceClient instances are shared across operations for resource efficiency
4. **Lazy Loading**: Azure clients are only created when needed
5. **Pythonic API**: Mimics standard library interfaces (io, pathlib) for familiarity

### Components

```
flowlet.persistence/
├── azure.py              # AzureBlobFile implementation
├── azure_path.py         # AzureBlobPath implementation
├── __init__.py          # Public API exports
└── README.md            # Quick reference guide
```

## Installation

```bash
pip install azure-storage-blob
```

The persistence layer will gracefully handle missing dependencies and provide helpful error messages if `azure-storage-blob` is not installed.

## AzureBlobFile - File Handle Interface

### Overview

`AzureBlobFile` provides a file-like interface that mimics Python's built-in `open()` function. It supports all standard file operations with automatic streaming for large files.

### Basic Usage

```python
from flowlet.persistence import AzureBlobFile, open_azure_blob

# Using the convenience function (recommended)
with open_azure_blob(
    connection_string='DefaultEndpointsProtocol=https;...',
    container_name='my-container',
    blob_name='data/file.txt',
    mode='w'
) as f:
    f.write('Hello, Azure!')

# Using the class directly
file = AzureBlobFile(
    connection_string='DefaultEndpointsProtocol=https;...',
    container_name='my-container',
    blob_name='data/file.txt',
    mode='r'
)
content = file.read()
file.close()
```

### Streaming Reads

One of the key features is streaming support for large files:

```python
from flowlet.persistence import open_azure_blob

# Efficient line-by-line reading (only loads 4MB chunks)
with open_azure_blob(conn_str, 'logs', 'application.log', 'r') as f:
    for line in f:
        if 'ERROR' in line:
            process_error(line)

# Custom chunk size for specific use cases
with open_azure_blob(conn_str, 'data', 'large.bin', 'rb', chunk_size=1024*1024) as f:
    while True:
        chunk = f.read(1024 * 1024)  # Read 1MB at a time
        if not chunk:
            break
        process_chunk(chunk)
```

### How Streaming Works

**Traditional Approach (BAD - High Memory)**:
```python
# Downloads entire 1GB file into memory
blob_data = blob_client.download_blob()
content = blob_data.readall()  # 1GB in memory!
```

**Streaming Approach (GOOD - Low Memory)**:
```python
# Only loads 4MB at a time
with open_azure_blob(conn_str, 'data', 'large-file.txt', 'r') as f:
    for line in f:
        process(line)  # Max 4MB in memory at once
```

**Under the Hood**:
1. On open, fetches blob metadata (size, type) - ~1KB network call
2. Maintains a 4MB read buffer
3. Uses Azure's range-based download: `download_blob(offset=X, length=4MB)`
4. Automatically fetches next chunk when current buffer is exhausted
5. Supports seeking without re-downloading

### Append Mode - AppendBlob Support

Azure Blob Storage has three blob types:
- **BlockBlob** - General purpose (default for writes)
- **AppendBlob** - Optimized for append operations (logs, time-series)
- **PageBlob** - For random access (VHDs, databases)

The persistence layer automatically uses the best blob type:

```python
from flowlet.persistence import open_azure_blob

# Creates/uses AppendBlob automatically
with open_azure_blob(conn_str, 'logs', 'app.log', 'a') as f:
    f.write('2024-12-19 22:00:00 - Application started\n')
    f.write('2024-12-19 22:00:01 - Connected to database\n')

    # Check what mode is being used
    if f.is_append_blob:
        print("✅ Using efficient AppendBlob")
    elif f.is_degraded_append_mode:
        print("⚠️ Using degraded mode - consider converting to AppendBlob")
```

**Performance Comparison**:

| Blob Type   | Append Mode Behavior                              | Network Calls per Write |
|-------------|---------------------------------------------------|-------------------------|
| AppendBlob  | `append_block()` - Direct append to blob         | 1 (append only)         |
| BlockBlob   | Download entire blob ➜ Modify ➜ Upload all      | 2 (download + upload)   |

**Memory Usage**:

| Blob Type   | Memory Usage          |
|-------------|-----------------------|
| AppendBlob  | ~4MB (buffer size)    |
| BlockBlob   | Entire blob size      |

**Example**: Appending to a 1GB log file:
- **AppendBlob**: 4MB memory, 1 network call (~100ms)
- **BlockBlob (degraded)**: 1GB memory, 2 network calls (~30 seconds)

### File Modes

All standard Python file modes are supported:

```python
# Text modes
'r'   # Read text (UTF-8 by default)
'w'   # Write text (overwrites)
'a'   # Append text (creates AppendBlob)

# Binary modes
'rb'  # Read binary
'wb'  # Write binary
'ab'  # Append binary
```

### Supported Operations

```python
with open_azure_blob(conn_str, 'data', 'file.txt', 'r') as f:
    # Reading
    content = f.read()           # Read all
    content = f.read(1024)       # Read 1KB
    line = f.readline()          # Read one line
    lines = f.readlines()        # Read all lines

    # Iteration
    for line in f:
        process(line)

    # Position management
    f.seek(100)                  # Seek to byte 100
    pos = f.tell()               # Get current position

    # Writing
    f.write('data')              # Write string/bytes
    f.writelines(['a\n', 'b\n']) # Write multiple lines

    # Flushing
    f.flush()                    # Upload buffered data immediately

    # Properties
    print(f.name)                # Blob name
    print(f.closed)              # Check if closed
    print(f.readable())          # Check if readable
    print(f.writable())          # Check if writable
```

### Shared Client Configuration

For optimal performance when working with multiple blobs, share a single `BlobServiceClient`:

```python
from azure.storage.blob import BlobServiceClient
from flowlet.persistence import open_azure_blob

# Create shared client
client = BlobServiceClient.from_connection_string(connection_string)

# All operations share the same client
with open_azure_blob('', 'container', 'file1.txt', 'r', blob_service_client=client) as f:
    data1 = f.read()

with open_azure_blob('', 'container', 'file2.txt', 'r', blob_service_client=client) as f:
    data2 = f.read()
```

## AzureBlobPath - pathlib-Style Interface

### Overview

`AzureBlobPath` provides a path-oriented interface similar to `pathlib.Path`, making it intuitive to navigate and manipulate blob hierarchies.

### Basic Usage

```python
from flowlet.persistence import AzureBlobPath

# Create root path
root = AzureBlobPath.from_connection_string(
    connection_string='DefaultEndpointsProtocol=https;...',
    container='my-container'
)

# Navigate using / operator (just like pathlib!)
data_dir = root / 'data'
file_path = data_dir / '2024' / '12' / 'report.pdf'

# File operations
file_path.write_text('Hello, Azure!')
content = file_path.read_text()

# Check properties
print(file_path.name)      # 'report.pdf'
print(file_path.stem)      # 'report'
print(file_path.suffix)    # '.pdf'
print(file_path.parent)    # AzureBlobPath(.../2024/12)
print(str(file_path))      # 'az://my-container/data/2024/12/report.pdf'
```

### Path Navigation

```python
from flowlet.persistence import AzureBlobPath

root = AzureBlobPath.from_connection_string(conn_str, container='data')

# Path construction
reports = root / 'reports'
q4_reports = reports / '2024' / 'Q4'
summary = q4_reports / 'summary.pdf'

# Path properties
print(summary.parts)       # ('data', 'reports', '2024', 'Q4', 'summary.pdf')
print(summary.name)        # 'summary.pdf'
print(summary.stem)        # 'summary'
print(summary.suffix)      # '.pdf'
print(summary.parent)      # AzureBlobPath(..., path='reports/2024/Q4')
print(summary.as_posix())  # 'data/reports/2024/Q4/summary.pdf'
```

### Listing Blobs

```python
from flowlet.persistence import AzureBlobPath

root = AzureBlobPath.from_connection_string(conn_str, container='data')

# List immediate children (non-recursive)
for blob in root.iterdir():
    print(f"{blob.name} - {blob.get_size()} bytes")

# List all blobs recursively
for blob in root.iterdir(recursive=True):
    print(blob.as_posix())

# Glob patterns
for json_file in root.glob('**/*.json'):
    print(f"Found JSON: {json_file.as_posix()}")

for log_file in (root / 'logs').glob('2024-*.log'):
    print(f"Found log: {log_file.name}")
```

### File Operations

```python
from flowlet.persistence import AzureBlobPath

root = AzureBlobPath.from_connection_string(conn_str, container='data')
file = root / 'data' / 'file.txt'

# Reading
text = file.read_text(encoding='utf-8')
binary = file.read_bytes()

# Writing
file.write_text('Hello, World!', encoding='utf-8')
file.write_bytes(b'\x00\x01\x02')

# Opening (returns AzureBlobFile)
with file.open('r') as f:
    for line in f:
        process(line)

# Existence checks
if file.exists():
    print("File exists")

if file.is_file():
    print("Is a blob (file)")

if (root / 'logs').is_dir():
    print("Directory exists (has blobs under it)")
```

### Blob Operations

```python
from flowlet.persistence import AzureBlobPath

root = AzureBlobPath.from_connection_string(conn_str, container='data')

source = root / 'reports' / 'draft.pdf'
backup = root / 'backup' / 'draft.pdf'
archive = root / 'archive' / '2024' / 'draft.pdf'

# Copy
source.copy_to(backup, overwrite=True)

# Move (copy + delete)
source.move_to(archive, overwrite=True)

# Rename (move to same directory)
new_path = archive.rename('final_report.pdf')

# Delete
backup.delete(missing_ok=True)  # Won't raise if doesn't exist
```

### Directory Operations

**Note**: Azure Blob Storage doesn't have true directories. The persistence layer simulates directories using blob prefixes.

```python
from flowlet.persistence import AzureBlobPath

root = AzureBlobPath.from_connection_string(conn_str, container='data')

# Create "directory" (creates a marker blob)
logs_dir = root / 'logs' / '2024' / '12'
logs_dir.mkdir(parents=True, exist_ok=True)

# Delete directory and all contents
logs_dir.rmdir(recursive=True)

# Check if directory exists (has blobs under it)
if (root / 'reports').is_dir():
    print("Reports directory has content")
```

### Metadata and Properties

```python
from flowlet.persistence import AzureBlobPath

root = AzureBlobPath.from_connection_string(conn_str, container='data')
doc = root / 'document.pdf'

# Content type
doc.set_content_type('application/pdf')
content_type = doc.get_content_type()

# Custom metadata
doc.set_metadata({
    'author': 'John Doe',
    'department': 'Engineering',
    'version': '1.2.3',
    'created_date': '2024-12-19'
})

metadata = doc.get_metadata()
print(f"Author: {metadata['author']}")
print(f"Version: {metadata['version']}")

# Blob properties
properties = doc.stat()
print(f"Size: {doc.get_size()} bytes")
print(f"Last modified: {properties.last_modified}")
print(f"Blob type: {properties.blob_type}")
```

### Shared Client Configuration

Like `AzureBlobFile`, paths can share a `BlobServiceClient`:

```python
from azure.storage.blob import BlobServiceClient
from flowlet.persistence import AzureBlobPath

# Create shared client
client = BlobServiceClient.from_connection_string(connection_string)

# Create root path with shared client
root = AzureBlobPath.from_service_client(
    blob_service_client=client,
    container='my-container'
)

# All child paths automatically inherit the shared client
file1 = root / 'file1.txt'
file2 = root / 'dir' / 'file2.txt'

# All operations use the same client (efficient!)
file1.write_text('data1')
file2.write_text('data2')

for blob in root.iterdir():
    print(blob.read_text())
```

## Design Patterns and Best Practices

### Pattern 1: Processing Large Files

```python
from flowlet.persistence import open_azure_blob

def process_large_csv(conn_str: str, container: str, blob_name: str):
    """Process a large CSV file without loading it all into memory."""
    with open_azure_blob(conn_str, container, blob_name, 'r') as f:
        # Read header
        header = f.readline().strip().split(',')

        # Process line by line
        for line in f:
            fields = line.strip().split(',')
            row = dict(zip(header, fields))
            process_row(row)
```

### Pattern 2: Batch Operations with Shared Client

```python
from azure.storage.blob import BlobServiceClient
from flowlet.persistence import AzureBlobPath

def batch_process_blobs(conn_str: str, container: str, prefix: str):
    """Process multiple blobs efficiently with shared client."""
    # Create shared client
    client = BlobServiceClient.from_connection_string(conn_str)

    # Create root path
    root = AzureBlobPath.from_service_client(client, container)

    # Process all matching blobs
    for blob in (root / prefix).glob('**/*.json'):
        data = blob.read_text()
        result = process_json(data)

        # Write result next to original
        output = blob.parent / f"{blob.stem}_processed.json"
        output.write_text(result)
```

### Pattern 3: Log File Appending

```python
from flowlet.persistence import open_azure_blob
import datetime

class AzureLogger:
    """Efficient logging to Azure AppendBlob."""

    def __init__(self, conn_str: str, container: str, log_name: str):
        self.conn_str = conn_str
        self.container = container
        self.log_name = log_name

    def log(self, message: str, level: str = 'INFO'):
        """Append log entry efficiently."""
        timestamp = datetime.datetime.utcnow().isoformat()
        entry = f"{timestamp} [{level}] {message}\n"

        with open_azure_blob(self.conn_str, self.container, self.log_name, 'a') as f:
            f.write(entry)
            # Automatically uses append_block() for AppendBlob

# Usage
logger = AzureLogger(conn_str, 'logs', 'application.log')
logger.log('Application started')
logger.log('Processing batch job', level='INFO')
logger.log('Error occurred', level='ERROR')
```

### Pattern 4: Path-Based File Organization

```python
from flowlet.persistence import AzureBlobPath
import datetime

def organize_by_date(conn_str: str, container: str):
    """Organize files by year/month/day structure."""
    root = AzureBlobPath.from_connection_string(conn_str, container)

    # Create date-based structure
    now = datetime.datetime.utcnow()
    today_dir = root / 'data' / str(now.year) / f"{now.month:02d}" / f"{now.day:02d}"
    today_dir.mkdir(parents=True, exist_ok=True)

    # Save file in organized structure
    file_path = today_dir / 'report.json'
    file_path.write_text('{"status": "complete"}')

    return file_path
```

### Pattern 5: Temporary/Staged Processing

```python
from flowlet.persistence import AzureBlobPath

def staged_processing(conn_str: str, container: str):
    """Process files through staging directories."""
    root = AzureBlobPath.from_connection_string(conn_str, container)

    # Directory structure
    inbox = root / 'inbox'
    processing = root / 'processing'
    completed = root / 'completed'
    failed = root / 'failed'

    # Process files from inbox
    for file in inbox.iterdir():
        try:
            # Move to processing
            work_file = processing / file.name
            file.move_to(work_file)

            # Process
            data = work_file.read_text()
            result = process_data(data)

            # Move to completed
            final_file = completed / file.name
            work_file.move_to(final_file)

        except Exception as e:
            # Move to failed on error
            error_file = failed / file.name
            work_file.move_to(error_file)

            # Store error details as metadata
            error_file.set_metadata({
                'error': str(e),
                'timestamp': datetime.datetime.utcnow().isoformat()
            })
```

## Performance Optimization

### Memory Usage

**Streaming vs. Full Load**:

```python
# ❌ BAD: Loads entire 1GB file into memory
path = AzureBlobPath.from_connection_string(conn_str, 'data', 'large.csv')
content = path.read_text()  # 1GB in memory
process(content)

# ✅ GOOD: Streams in 4MB chunks
with path.open('r') as f:
    for line in f:
        process(line)  # Max 4MB in memory
```

### Network Efficiency

**Client Reuse**:

```python
from azure.storage.blob import BlobServiceClient
from flowlet.persistence import AzureBlobPath

# ❌ BAD: Creates new client for each operation
for i in range(100):
    path = AzureBlobPath.from_connection_string(conn_str, 'data', f'file{i}.txt')
    path.write_text(f'data {i}')  # 100 new clients created!

# ✅ GOOD: Reuse single client
client = BlobServiceClient.from_connection_string(conn_str)
root = AzureBlobPath.from_service_client(client, 'data')

for i in range(100):
    file = root / f'file{i}.txt'
    file.write_text(f'data {i}')  # Single shared client
```

### Append Performance

```python
# ❌ BAD: Using BlockBlob for appending (1GB file example)
# - Downloads 1GB
# - Appends 100 bytes
# - Uploads 1GB
# = 2GB network transfer, ~30 seconds

# ✅ GOOD: Using AppendBlob
# - Appends 100 bytes directly
# = 100 bytes network transfer, ~100ms
with open_azure_blob(conn_str, 'logs', 'app.log', 'a') as f:
    if not f.is_append_blob:
        # Convert to AppendBlob if needed
        pass
    f.write('New entry\n')
```

### Chunk Size Tuning

```python
# For frequent small reads (< 64KB)
with open_azure_blob(conn_str, 'data', 'file.txt', 'r', chunk_size=64*1024) as f:
    data = f.read(1024)  # Fetches 64KB chunk

# For large sequential reads
with open_azure_blob(conn_str, 'data', 'file.txt', 'r', chunk_size=16*1024*1024) as f:
    for line in f:
        process(line)  # Fetches 16MB chunks
```

## Error Handling

### Common Exceptions

```python
from flowlet.persistence import AzureBlobPath, open_azure_blob

# FileNotFoundError
try:
    path = AzureBlobPath.from_connection_string(conn_str, 'container', 'missing.txt')
    content = path.read_text()
except FileNotFoundError:
    print("Blob does not exist")

# Existence checks
path = AzureBlobPath.from_connection_string(conn_str, 'container', 'file.txt')
if path.exists():
    content = path.read_text()

# Safe delete
path.delete(missing_ok=True)  # Won't raise if missing
```

### Handling Azure SDK Exceptions

```python
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
from flowlet.persistence import AzureBlobPath

try:
    path = AzureBlobPath.from_connection_string(conn_str, 'container', 'file.txt')
    path.write_text('data')
except ResourceExistsError:
    print("Blob already exists")
except ResourceNotFoundError:
    print("Container not found")
except Exception as e:
    print(f"Unexpected error: {e}")
```

## Migration from Local Filesystem

The API is designed to minimize changes when migrating from local filesystem:

```python
# === LOCAL FILESYSTEM ===
from pathlib import Path

root = Path('data')
file = root / 'reports' / '2024' / 'summary.pdf'

# Operations
file.write_text('content')
content = file.read_text()
file.exists()
file.unlink(missing_ok=True)

for child in root.iterdir():
    print(child.name)

for pdf in root.glob('**/*.pdf'):
    process(pdf)

# === AZURE BLOB STORAGE ===
from flowlet.persistence import AzureBlobPath

root = AzureBlobPath.from_connection_string(conn_str, 'data')
file = root / 'reports' / '2024' / 'summary.pdf'

# Same operations!
file.write_text('content')
content = file.read_text()
file.exists()
file.unlink(missing_ok=True)

for child in root.iterdir():
    print(child.name)

for pdf in root.glob('**/*.pdf'):
    process(pdf)
```

## Limitations and Considerations

### Azure Blob Storage Constraints

1. **No True Directories**: Directories are simulated using blob prefixes
2. **Eventual Consistency**: List operations may not immediately reflect recent changes
3. **Blob Type Restrictions**:
   - AppendBlob: Max 195GB, max 50,000 append blocks
   - BlockBlob: Max 190.7TB, max 50,000 blocks
4. **Metadata Limits**: Max 8KB per blob
5. **Path Length**: Max 1024 characters for blob names

### Performance Considerations

1. **Latency**: Network operations are slower than local filesystem
2. **List Operations**: Can be slow for containers with millions of blobs
3. **Small Files**: Network overhead may dominate for files < 1KB
4. **Random Access**: Seeking downloads new chunks, less efficient than local files

### Best Practices

1. **Use AppendBlob for logs**: Up to 1000x faster for append-heavy workloads
2. **Share clients**: Reuse `BlobServiceClient` across operations
3. **Stream large files**: Don't load entire files into memory
4. **Batch operations**: Group related operations to minimize network calls
5. **Use metadata**: Store small metadata rather than separate config blobs
6. **Consider caching**: Cache frequently accessed small blobs locally

## API Reference

See the [README.md](../src/flowlet/persistence/README.md) for detailed API documentation.

## Examples

For complete working examples, see the `examples/` directory:

- `basic_file_operations.py` - Basic read/write operations
- `streaming_large_files.py` - Efficient handling of large files
- `append_logging.py` - Log file appending with AppendBlob
- `path_navigation.py` - Directory navigation and organization
- `batch_processing.py` - Batch operations with shared clients
