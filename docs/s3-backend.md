# Syncthing S3 Backend

Syncthing can store synced files in an S3-compatible object store (such as AWS S3, MinIO, or any S3-compatible service) instead of the local filesystem. This guide explains how to configure and use the S3 backend.

## Overview

The S3 backend implements the full Syncthing filesystem interface on top of an S3-compatible bucket. Files are stored as S3 objects, and POSIX metadata (permissions, ownership, timestamps) is preserved using custom S3 object metadata with the `X-Syncthing-` prefix.

### Key Features

- **Full filesystem emulation** — create, read, write, rename, and delete files and directories.
- **POSIX metadata preservation** — file permissions, UID/GID ownership, and atime/mtime/ctime are stored as S3 object metadata.
- **S3-compatible** — works with AWS S3, MinIO, and any other S3-compatible service.
- **Uses MinIO SDK** — the implementation uses the MinIO Go SDK (`minio-go/v7`) for all S3 operations.
- **Directory support** — directories are represented using zero-byte marker objects with the suffix `/.syncthing_dir_marker`.

### Metadata Preservation

POSIX file attributes are stored as custom S3 object metadata headers:

| Attribute         | S3 Metadata Key         | Format              |
|-------------------|-------------------------|---------------------|
| File permissions  | `X-Syncthing-Mode`      | Octal string        |
| Owner UID         | `X-Syncthing-Uid`       | Decimal string      |
| Group GID         | `X-Syncthing-Gid`       | Decimal string      |
| Access time       | `X-Syncthing-Atime`     | RFC3339Nano         |
| Modification time | `X-Syncthing-Mtime`     | RFC3339Nano         |
| Creation time     | `X-Syncthing-Ctime`     | RFC3339Nano         |

This metadata is set on every `PutObject` call and updated via server-side copy (`CopyObject`) when attributes change (e.g. `Chmod`, `Chtimes`, `Lchown`).

## Configuration

### URI Format

The S3 backend uses a URI-based configuration for endpoint and bucket:

```
s3://ENDPOINT/BUCKET[/PREFIX][?accessKey=ACCESS_KEY&secretKey=SECRET_KEY&useSSL=true]
```

| Parameter   | Description                                                    |
|-------------|----------------------------------------------------------------|
| `ENDPOINT`  | S3 endpoint hostname and port (e.g. `s3.amazonaws.com`, `localhost:9000`) |
| `BUCKET`    | S3 bucket name                                                 |
| `PREFIX`    | Optional key prefix to scope all objects under a subdirectory  |
| `accessKey` | S3 access key ID (optional if set via environment)             |
| `secretKey` | S3 secret access key (optional if set via environment)         |
| `useSSL`    | Set to `true` to use HTTPS (default: `false`, or `S3_USE_SSL`) |

### Credentials via environment (recommended)

Prefer **not** putting secrets in `config.xml`. Omit `accessKey` / `secretKey`
from the URI and set environment variables on the Syncthing process instead.

| Purpose        | Environment variables (first non-empty wins) |
|----------------|-----------------------------------------------|
| Access key     | `S3_ACCESS_KEY_ID`, `AWS_ACCESS_KEY_ID`, `MINIO_ACCESS_KEY`, `MINIO_ROOT_USER` |
| Secret key     | `S3_SECRET_ACCESS_KEY`, `AWS_SECRET_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_ROOT_PASSWORD` |
| Session token  | `S3_SESSION_TOKEN`, `AWS_SESSION_TOKEN` (optional) |
| Use TLS        | `S3_USE_SSL=true` (if `useSSL` is not in the URI) |

URI query parameters override the environment when both are set.

Examples below use MinIO’s **default** credentials:

- access key: `minioadmin`
- secret key: `minioadmin`

**Docker Compose example:**

```yaml
services:
  minio:
    image: quay.io/minio/minio
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"
      - "9001:9001"

  syncthing:
    image: quay.io/jkandasa/syncthing:s3
    environment:
      # Same as MinIO defaults (minioadmin / minioadmin)
      S3_ACCESS_KEY_ID: minioadmin
      S3_SECRET_ACCESS_KEY: minioadmin
    # folder path can be just: s3://minio:9000/syncthing
```

### Example URIs

**Local MinIO (credentials from env — recommended):**
```
s3://minio:9000/my-syncthing-bucket
```

**Local MinIO (credentials in URI — less secure; default MinIO user/password):**
```
s3://localhost:9000/my-syncthing-bucket?accessKey=minioadmin&secretKey=minioadmin
```

**AWS S3 (credentials from env, TLS on):**
```
s3://s3.us-east-1.amazonaws.com/my-syncthing-bucket?useSSL=true
```

**With prefix (scope files under a subdirectory):**
```
s3://localhost:9000/my-bucket/syncthing-data
```

### Syncthing Configuration

In your `config.xml`, set the folder `path` to the S3 URI and add a
**child element** `<filesystemType>s3</filesystemType>` (not an attribute —
attributes are ignored and the folder stays on the default `basic` filesystem).

```xml
<folder id="my-s3-folder" label="S3 Backed Folder"
  path="s3://minio:9000/my-bucket"
  type="sendreceive" rescanIntervalS="60" fsWatcherEnabled="false">
    <filesystemType>s3</filesystemType>
    <device id="DEVICE-ID-HERE" />
</folder>
```

Set credentials on the process (Docker `environment`, systemd `Environment=`, etc.).
For a stock MinIO server these are the defaults:

```bash
export S3_ACCESS_KEY_ID=minioadmin
export S3_SECRET_ACCESS_KEY=minioadmin
```

### GUI configuration

In **Add Folder** / **Edit Folder** → **General**:

1. Set **Filesystem Type** to **S3 / Object Storage**.
2. Fill **S3 Endpoint** (e.g. `minio:9000` or `localhost:9000`), **S3 Bucket**, optional **Prefix**.
3. Optionally enter **Access Key** / **Secret Key** as `minioadmin` / `minioadmin`
   (MinIO defaults), or leave them empty and set the same values via environment
   variables on the Syncthing process.
4. Enable **Use HTTPS (TLS)** for AWS and other TLS endpoints (leave off for local HTTP MinIO).
5. Save. Syncthing stores `filesystemType=s3` and a composed `s3://…` path.

**Watch for Changes** is disabled for S3 folders; use **Full Rescan Interval** instead.

You can still edit `config.xml` or the REST API directly:

```bash
# Example: update an existing folder (credentials stay in the environment)
curl -s -H "X-API-Key: $API_KEY" http://127.0.0.1:8384/rest/config/folders/my-s3-folder \
  | jq '.filesystemType = "s3" | .path = "s3://minio:9000/my-bucket" | .fsWatcherEnabled = false' \
  | curl -s -X PUT -H "X-API-Key: $API_KEY" -H "Content-Type: application/json" \
      --data-binary @- http://127.0.0.1:8384/rest/config/folders/my-s3-folder
```

> **Important:** If you put credentials in the URI in XML, ampersands (`&`) must be escaped as `&amp;`.

## Setup Guide

### Setting Up with MinIO (Local Development)

#### Step 1: Install MinIO

```bash
# Download MinIO server
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
sudo mv minio /usr/local/bin/
```

#### Step 2: Start MinIO

```bash
# Create data directory
mkdir -p /data/minio

# Start MinIO server
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
  minio server /data/minio --address :9000 --console-address :9001
```

MinIO is now running:
- **API:** `http://localhost:9000`
- **Console:** `http://localhost:9001`

#### Step 3: Create a Bucket

You can create a bucket using the MinIO Console (web UI) or the `mc` CLI:

```bash
# Install mc (MinIO Client)
wget https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
sudo mv mc /usr/local/bin/

# Configure mc alias
mc alias set local http://localhost:9000 minioadmin minioadmin

# Create a bucket
mc mb local/syncthing-data
```

#### Step 4: Configure Syncthing

Export MinIO’s default credentials for the Syncthing process:

```bash
export S3_ACCESS_KEY_ID=minioadmin
export S3_SECRET_ACCESS_KEY=minioadmin
```

Add the S3-backed folder to your Syncthing configuration (`~/.config/syncthing/config.xml`):

```xml
<folder id="s3-folder" label="My S3 Folder"
  path="s3://localhost:9000/syncthing-data"
  type="sendreceive" rescanIntervalS="60" fsWatcherEnabled="false">
    <filesystemType>s3</filesystemType>
    <device id="YOUR-DEVICE-ID" />
    <minDiskFree unit="%">1</minDiskFree>
</folder>
```

Or put the same default credentials in the URI (less secure):

```
s3://localhost:9000/syncthing-data?accessKey=minioadmin&secretKey=minioadmin
```

> **Note:** File system watching (`fsWatcherEnabled`) is not supported with S3. Set it to `false` and rely on periodic rescans by setting `rescanIntervalS` appropriately.

#### Step 5: Restart Syncthing

```bash
# Restart Syncthing to pick up the new configuration
syncthing serve
```

### Setting Up with AWS S3

#### Step 1: Create an S3 Bucket

```bash
aws s3 mb s3://my-syncthing-bucket --region us-east-1
```

#### Step 2: Create IAM Credentials

Create an IAM user with S3 access and note the access key and secret key.

Required IAM permissions:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetObjectMetadata",
        "s3:CopyObject"
      ],
      "Resource": [
        "arn:aws:s3:::my-syncthing-bucket",
        "arn:aws:s3:::my-syncthing-bucket/*"
      ]
    }
  ]
}
```

#### Step 3: Configure Syncthing

```xml
<folder id="aws-folder" label="AWS S3 Folder"
  path="s3://s3.us-east-1.amazonaws.com/my-syncthing-bucket?accessKey=AKIAIOSFODNN7EXAMPLE&amp;secretKey=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY&amp;useSSL=true"
  type="sendreceive" rescanIntervalS="300" fsWatcherEnabled="false">
    <filesystemType>s3</filesystemType>
    <device id="YOUR-DEVICE-ID" />
</folder>
```

## Performance notes (HDD-backed MinIO)

Full folder scans used to issue **one `ListObjects` per directory** (and more with
case-conflict detection). On HDD backends that is very expensive.

The S3 backend now:

1. **Skips case-conflict detection** for `filesystemType=s3` (object keys are
   case-sensitive; listing every directory twice is not worth it).
2. **Caches one recursive listing** of the folder prefix for a short TTL (2
   minutes) and serves `DirNames` / `Lstat` from that tree during a scan.
   Mutations (write/delete/rename/mkdir) invalidate the cache immediately.

Expected API shape during a scan: a **small number of paginated
`ListObjectsV2` calls** for the whole prefix, instead of hundreds of per-directory
lists. `HeadObject` is largely avoided for walk/stat when the cache is warm.

## Limitations

1. **No filesystem watching** — S3 does not support inotify-style notifications, so `fsWatcherEnabled` should be set to `false`. Use periodic rescans instead.
2. **In-memory file buffering** — files are buffered entirely in memory during read/write operations. Very large files may consume significant memory.
3. **No extended attributes (xattrs)** — S3 does not natively support xattrs. The backend returns `ErrXattrsNotSupported`.
4. **Eventual consistency** — depending on the S3 provider, recently written objects may not be immediately visible.
5. **No hard links** — S3 objects are independent; hard links are not supported.
6. **Symlinks stored as content** — symbolic links are emulated by storing the target path as the object's content, with the symlink mode bit set in metadata.
7. **Cached `Lstat` metadata** — while the list cache is warm, file mode/ownership come from defaults (size/mtime from the listing). Full user-metadata is applied on open/read paths that `Head`/`Get` the object.

## Verification

Run the included verification script to confirm the S3 backend works correctly with a local MinIO instance:

```bash
./script/verify-s3-backend.sh
```

This starts a temporary MinIO server, runs the full unit test suite, and performs 18 integration checks covering file operations, metadata preservation, directory handling, and more.

To run just the unit tests:

```bash
S3_TEST_ENDPOINT=localhost:9000 \
  S3_TEST_ACCESS_KEY=minioadmin \
  S3_TEST_SECRET_KEY=minioadmin \
  go test -v ./lib/fs/s3fs/
```

## Troubleshooting

### "bucket name missing in URI"
Make sure your URI includes the bucket name after the endpoint: `s3://endpoint/bucket-name`.

### "file does not exist" on folder start
Syncthing expects a `.stfolder` marker directory. Create the folder marker:
```bash
mc mb local/my-bucket/.stfolder
```
Or let Syncthing create it automatically by checking the "Create Marker" option.

### Connection refused
Verify that the S3 endpoint is reachable and the credentials are correct. For MinIO, ensure the server is running and the port matches your configuration.

### SSL certificate errors
For self-signed certificates (common with local MinIO), set `useSSL=false` in the URI, or configure your system's CA trust store.
