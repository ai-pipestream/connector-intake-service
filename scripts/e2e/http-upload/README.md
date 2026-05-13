# HTTP Upload E2E

These scripts show the raw HTTP upload path end to end:

1. get or reuse a datasource id and API key;
2. upload one file through `connector-intake-service`;
3. upload all files under a directory through the same path.

The public intake endpoint is:

```text
POST /uploads/raw
```

Intake validates the datasource/API key, derives a stable document id from the source path, and streams the body to `repository-service`. This path does not require a pipeline. The document is persisted for later processing.

## Configure

For the standard local smoke-test setup:

```bash
scripts/e2e/http-upload/setup-local.sh
```

This writes `scripts/e2e/http-upload/.env.local` with the default test datasource values:

```text
DS_ID=valid-datasource
API_KEY=valid-api-key
```

Those values are useful when the local environment is backed by test mocks. For a real local platform stack, run `prepare-datasource.sh --force-create` after setup; it creates or finds a datasource and writes a fresh API key to `.env.local`.

If you run it from a terminal, it asks for a source file or directory. You can also pass the path directly:

```bash
scripts/e2e/http-upload/setup-local.sh --source-file /path/to/document.pdf
scripts/e2e/http-upload/setup-local.sh --source-dir /path/to/corpus
```

If you leave the source path blank, it writes credentials only. You can provide the file or directory later when running an upload command.

Use `--force` to overwrite an existing local env file.

For manual setup instead:

Copy the example file and edit it locally:

```bash
cp scripts/e2e/http-upload/env.example scripts/e2e/http-upload/.env.local
```

Set `DS_ID` and `API_KEY` if you already have sample credentials. When both are present, `prepare-datasource.sh` reuses them and does not create a new datasource.

Set either:

```bash
INTAKE_E2E_SOURCE_FILE=/path/to/one/file
```

or:

```bash
INTAKE_E2E_SOURCE_DIR=/path/to/files
```

Do not commit `.env.local`. It may contain local paths and API keys.

## Reuse Or Create Datasource Credentials

If `.env.local` already contains `DS_ID` and `API_KEY`:

```bash
scripts/e2e/http-upload/prepare-datasource.sh
```

The script prints the datasource id and a masked API key.

If those values are empty, the script uses `grpcurl` to create a sample account, repository bucket, and datasource:

```bash
scripts/e2e/http-upload/prepare-datasource.sh
```

Use `--dry-run` to see what it would do:

```bash
scripts/e2e/http-upload/prepare-datasource.sh --dry-run
```

## Upload One File

```bash
scripts/e2e/http-upload/upload-one.sh
```

Or provide the file on the command line:

```bash
scripts/e2e/http-upload/upload-one.sh --file /path/to/document.pdf
```

Dry run:

```bash
scripts/e2e/http-upload/upload-one.sh --file /path/to/document.pdf --dry-run
```

The script sends a `curl` request with these headers:

- `x-datasource-id`
- `x-api-key`
- `x-source-path`
- `x-filename`
- `x-checksum-sha256`
- `content-type`

The source path is relative to `INTAKE_E2E_SOURCE_DIR` when that variable is set. This avoids sending machine-specific absolute paths.

## Upload A Directory

```bash
scripts/e2e/http-upload/upload-directory.sh
```

Or provide the directory on the command line:

```bash
scripts/e2e/http-upload/upload-directory.sh --dir /path/to/corpus
```

To test a small subset first:

```bash
INTAKE_E2E_MAX_FILES=10 scripts/e2e/http-upload/upload-directory.sh --dir /path/to/corpus
```

Dry run:

```bash
scripts/e2e/http-upload/upload-directory.sh --dir /path/to/corpus --dry-run
```

## AWS Note

If the deployed endpoint is behind M2M authentication, obtain the token outside these scripts and add the required `curl` header in the local environment or a local wrapper. The committed scripts cover the intake and repository path only.
