"""Notion API client and reusable sink helpers."""

from typing import Any, Dict, Optional, Tuple

import requests
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from hotglue_singer_sdk.target_sdk.client import HotglueSink
from hotglue_etl_exceptions import InvalidPayloadError, InvalidCredentialsError

DEFAULT_NOTION_API_VERSION = "2025-09-03"


class NotionSink(HotglueSink):
    """Notion API sink: uses SDK Rest for HTTP and adds Notion auth/headers."""

    base_url = "https://api.notion.com/v1"

    @property
    def http_headers(self) -> Dict[str, Any]:
        """Notion requires Bearer token and Notion-Version. Rest adds Content-Type."""
        token = self.config.get("token")
        version = self.config.get("notion_api_version", DEFAULT_NOTION_API_VERSION)
        return {
            "Authorization": f"Bearer {token}",
            "Notion-Version": version,
        }

    def response_error_message(self, response: requests.Response) -> str:
        """Extract Notion API error message from JSON body when present."""
        try:
            body = response.json()
            msg = body.get("message") or body.get("code") or response.text
        except Exception:
            msg = response.text
        return f"{response.status_code}: {msg}"

    def validate_response(self, response: requests.Response) -> None:
        """Use Notion error body for 4xx messages; delegate rest to SDK."""
        if response.status_code in (429,) or 500 <= response.status_code < 600:
            raise RetriableAPIError(self.response_error_message(response), response)
        if response.status_code in (401, 403):
            raise InvalidCredentialsError(self.response_error_message(response))
        if response.status_code in (400, 422):
            raise InvalidPayloadError(self.response_error_message(response))
        if 400 <= response.status_code < 500:
            raise FatalAPIError(self.response_error_message(response))


class NotionRecordSink(NotionSink):
    """Reusable sink for Notion endpoints with create/update semantics."""

    endpoint = ""
    create_method = "POST"
    update_method = "PATCH"
    supports_updates = False
    primary_key = "id"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return record

    def get_update_endpoint(self, record_id: str) -> str:
        return f"{self.endpoint}/{record_id}"

    def build_update_payload(self, record: dict) -> dict:
        return {k: v for k, v in record.items() if k != self.primary_key}

    def build_create_payload(self, record: dict) -> dict:
        return record

    def resolve_request(self, record: dict) -> Tuple[str, str, dict, bool]:
        record_id = record.get(self.primary_key)
        if record_id and self.supports_updates:
            return (
                self.update_method,
                self.get_update_endpoint(record_id),
                self.build_update_payload(record),
                True,
            )
        return self.create_method, self.endpoint, self.build_create_payload(record), False

    def extract_response_id(self, data: Any) -> Optional[str]:
        if isinstance(data, dict):
            id_val = data.get(self.primary_key)
            if id_val:
                return id_val
        return None

    def extract_record_url(self, data: Any) -> Optional[str]:
        if isinstance(data, dict):
            record_url = data.get("url")
            if record_url:
                return record_url
        return None

    def upsert_record(self, record: dict, context: dict) -> tuple:
        state_updates: Dict[str, Any] = {}
        method, endpoint, body, is_update = self.resolve_request(record)
        response = self.request_api(method, endpoint=endpoint, request_data=body)
        data = response.json()

        if is_update:
            state_updates["is_updated"] = True
        if self.config.get("output_record_url"):
            record_url = self.extract_record_url(data)
            if record_url:
                state_updates["record_url"] = record_url

        record_id = self.extract_response_id(data)
        return record_id, response.ok, state_updates
