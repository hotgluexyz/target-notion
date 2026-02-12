# Sample Singer Data

This folder contains Singer JSONL samples split by stream type.
Learn more about the Notion API and how to use the target-notion in the [Notion API documentation](https://developers.notion.com/reference/intro).

## Streams

### Page - `page.singer`
- Represents a page in Notion. It must always have a parent.
- `record.id`: if set, target calls `PATCH /pages/{id}` (must be an existing page ID).
- `record.parent.database_id`: used for page creation in a database; replace with an existing database ID.

### Block - `block.singer`
- Represents a block of content in Notion, like a paragraph or heading. It must always have a parent.
- `record.id`: required for all block operations.
- With `children` present, target calls `PATCH /blocks/{id}/children` (so `id` must be a parent block you can append to - will add new blocks).
- Without `children`, target calls `PATCH /blocks/{id}` to update that block.

### Comment - `comment.singer`
- Represents a comment on a page or block in Notion. It must always have a parent.
- Replace `record.parent.page_id` (or switch to `parent.block_id`) with a real target object.

### Database - `database.singer`
- Represents a database in Notion. It must always have a parent.
- `record.parent.page_id` must be an existing page where the database will be created.
- If you add `record.id`, target will update via `PATCH /databases/{id}` instead of create.

### DataSource - `datasource.singer`
- Represents a data source inside a database in Notion. It must always have a parent.
- `record.parent.database_id` must be an existing database ID.
- If you add `record.id`, target will update via `PATCH /data_sources/{id}` instead of create.
