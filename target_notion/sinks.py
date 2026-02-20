"""Notion target sink classes."""

from typing import Any, Optional, Tuple

from target_notion.client import NotionRecordSink
from hotglue_etl_exceptions import InvalidPayloadError
from functools import cached_property


class FallbackSink(NotionRecordSink):
    """Notion pages sink: create or update page objects. Updates existing page when title matches."""

    name = "Page"
    endpoint = "/pages"
    supports_updates = True

    @cached_property
    def data_source_id(self) -> str:
        database_id = self.config.get("database_id")
        database = self.request_api("GET", f"/databases/{database_id}").json()
        return database.get("data_sources")[0].get("id")

    @cached_property
    def title_property_name(self) -> Optional[str]:
        """Name of the first 'title' property in the data source schema."""
        for p in self.data_source_properties.values():
            if p.get("type") == "title":
                return p.get("name")
        return None

    @cached_property
    def data_source_properties(self) -> dict:
        data_source = self.request_api("GET", f"/data_sources/{self.data_source_id}").json()
        return data_source.get("properties")
    
    @cached_property
    def data_source_people(self) -> list:
        # Get all the people available in Notion and return a list of people with their id and name
        people = self.request_api("GET", f"/users").json()
        return [{"id": p.get("id"), "name": p.get("name")} for p in people.get("results")]

    def preprocess_record(self, record: dict, context: dict) -> dict:
        # Get the database info
        database_id = self.config.get("database_id")
        # Get the data source properties
        data_source_properties = self.data_source_properties

        # Using the data source properties, we need to remap the record to the proper Notion API shape
        properties_map = {}
        for p in data_source_properties.values():
            if record.get(p.get("name")) is None:
                continue

            property_type = p.get("type")

            if property_type in ["title", "rich_text"]:
                properties_map[p.get("name")] = {
                    property_type: [{ "text": { "content": record.get(p.get("name")) } }]
                }
            elif property_type in ["number"]:
                properties_map[p.get("name")] = {
                    property_type: record.get(p.get("name"))
                }
            elif property_type == "url":
                properties_map[p.get("name")] = {
                    "url": record.get(p.get("name"))
                }
            elif property_type in ["people"]:
                # We will match the name to a Notion user id
                people_name = record.get(p.get("name"))
                for person in self.data_source_people:
                    if person.get("name") == people_name:
                        properties_map[p.get("name")] = {
                            "people": [{ "id": person.get("id") }]
                        }
                        break
                else:
                    self.logger.warning(f"Person {people_name} not found in Notion, ignoring.")
            else:
                raise ValueError(f"Unsupported property type: {property_type}")

        payload = {
            "parent": { "database_id": database_id },
            "properties": properties_map
        }

        return payload

    def build_update_payload(self, record: dict) -> dict:
        """Page updates only send properties (parent cannot be changed)."""
        return {"properties": record["properties"]}

    def _find_page_id_by_title(self, title: str) -> Optional[str]:
        """Query the data source for a page with the given title; return its id or None."""
        if not title or not self.title_property_name:
            return None
        body = {
            "filter": {
                "property": self.title_property_name,
                "title": {"equals": title},
            },
            "page_size": 1,
        }
        response = self.request_api(
            "POST",
            endpoint=f"/data_sources/{self.data_source_id}/query",
            request_data=body,
        )
        data = response.json()
        results = data.get("results") or []
        if not results:
            return None
        first = results[0]
        if isinstance(first, dict) and first.get("object") == "page":
            return first.get("id")
        return None

    def resolve_request(self, record: dict) -> Tuple[str, str, dict, bool]:
        """If record has a title and a page with that title exists, update it; otherwise create."""
        properties = record.get("properties") or {}
        title_value = None
        if self.title_property_name:
            prop = properties.get(self.title_property_name)
            if isinstance(prop, dict):
                title_list = prop.get("title") or []
                if title_list and isinstance(title_list[0], dict):
                    text = title_list[0].get("text") or {}
                    title_value = text.get("content")

        existing_id = self._find_page_id_by_title(title_value) if title_value else None
        if existing_id:
            return (
                self.update_method,
                f"{self.endpoint}/{existing_id}",
                self.build_update_payload(record),
                True,
            )
        return self.create_method, self.endpoint, self.build_create_payload(record), False


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
