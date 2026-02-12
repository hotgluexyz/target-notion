"""Notion target sink classes."""

from typing import Any, Optional, Tuple

from target_notion.client import NotionRecordSink
from hotglue_etl_exceptions import InvalidPayloadError


class Page(NotionRecordSink):
    """Notion pages sink: create or update page objects."""

    name = "Page"
    endpoint = "/pages"
    supports_updates = True


class Database(NotionRecordSink):
    """Notion database sink: create and update database objects."""

    name = "Database"
    endpoint = "/databases"
    supports_updates = True


class DataSource(NotionRecordSink):
    """Notion data source sink: create and update data source objects."""

    name = "DataSource"
    endpoint = "/data_sources"
    supports_updates = True


class Comment(NotionRecordSink):
    """Notion comment sink: create comments on pages, blocks, or discussions."""

    name = "Comment"
    endpoint = "/comments"
    supports_updates = False


class Block(NotionRecordSink):
    """Notion block sink: update blocks and append child blocks."""

    name = "Block"
    endpoint = "/blocks"
    create_method = "PATCH"
    update_method = "PATCH"
    supports_updates = True
    append_trigger_fields = ("children", "after", "position")

    def resolve_request(self, record: dict) -> Tuple[str, str, dict, bool]:
        block_id = record.get(self.primary_key)
        if not block_id:
            raise InvalidPayloadError("Record must include 'id' for block operations.")

        body = {k: v for k, v in record.items() if k != self.primary_key}
        is_append = any(field in body for field in self.append_trigger_fields)
        if is_append:
            return self.create_method, f"{self.endpoint}/{block_id}/children", body, False
        return self.update_method, f"{self.endpoint}/{block_id}", body, True

    def extract_response_id(self, data: Any) -> Optional[str]:
        if isinstance(data, dict):
            # PATCH /blocks/{block_id} response
            id_val = data.get(self.primary_key)
            if id_val:
                return id_val

            # PATCH /blocks/{block_id}/children response
            results = data.get("results") or []
            if isinstance(results, list) and results:
                first_child = results[0]
                if isinstance(first_child, dict) and first_child.get(self.primary_key):
                    return first_child[self.primary_key]
        return None
