"""Notion target class."""

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel
from hotglue_singer_sdk.target_sdk.target import TargetHotglue

from target_notion.sinks import Block, Comment, Database, DataSource, Page


class TargetNotion(TargetHotglue):
    """Singer target for Notion."""

    name = "target-notion"
    alerting_level = AlertingLevel.WARNING
    config_jsonschema = th.PropertiesList(
        th.Property(
            "token",
            th.StringType,
            required=True,
            description="Notion API token (Bearer).",
        ),
        th.Property(
            "output_record_url",
            th.BooleanType,
            default=False,
            description="Include Notion page URL in state when available.",
        ),
        th.Property(
            "notion_api_version",
            th.StringType,
            default="2025-09-03",
            description="Notion API version for the Notion-Version header (default: 2025-09-03).",
        ),
    ).to_dict()
    SINK_TYPES = [Page, Database, DataSource, Block, Comment]


if __name__ == "__main__":
    TargetNotion.cli()
