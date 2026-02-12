# target-notion

**target-notion** is a Singer Target for writing data to Notion (pages, databases, blocks, and comments). It can be run on [hotglue](https://hotglue.com), an embedded integration platform for running Singer Taps and Targets.

## Installation

```bash
pipx install target-notion
```

Or from source:

```bash
cd target-notion
poetry install
```

## Configuration

### Accepted Config Options

```bash
target-notion --about
```

### Config file example

```json
{
  "token": "ntn_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "output_record_url": false,
  "notion_api_version": "2025-09-03"
}
```

- **token** (required): Notion API integration token (Bearer) for a Private Integration. Information on how to create a token can be found [here](https://developers.notion.com/guides/get-started/authorization).
- **output_record_url**: If true, include the Notion page/database/data source URL in state.
- **notion_api_version**: Value used for the `Notion-Version` request header (default: `2025-09-03`).

## Streams (sinks)

- **Page**: `POST /v1/pages` (create) and `PATCH /v1/pages/{id}` (update when `id` is present).
- **Database**: `POST /v1/databases` (create) and `PATCH /v1/databases/{id}` (update when `id` is present).
- **DataSource**: `POST /v1/data_sources` (create) and `PATCH /v1/data_sources/{id}` (update when `id` is present).
- **Block**:
  - `PATCH /v1/blocks/{id}` for block updates.
  - `PATCH /v1/blocks/{id}/children` to add child blocks when payload contains `children`/`position`/`after`.
- **Comment**: create-only via `POST /v1/comments` (page/block/discussion comments).

Each sink expects records in the native Notion API shape for that endpoint. Check the [Notion API documentation](https://developers.notion.com/reference/intro) for more details.

## Usage

### Run with data

```bash
cat data.singer | target-notion --config config.json > state.json
```

### Run with a tap

```bash
tap-carbon-intensity | target-notion --config config.json
```

### Development

```bash
poetry install
poetry run target-notion --help
poetry run pytest
```
