"""REST API load generator using Python Data Source API"""

import logging
from typing import Dict, Any

from ...core.base_generator import BaseActionGenerator
from ...models.config import Action


class RestApiLoadGenerator(BaseActionGenerator):
    """Generate REST API load actions.

    Supports both:
    - PySpark Data Source API (single-user clusters, DBR 15.4+)
    - Shared cluster compatible methods (mapInPandas, direct requests)

    Configuration options:
        source:
            type: rest_api
            endpoint: https://api.example.com/data
            method: GET  # or POST
            headers: {}  # Optional headers
            body: {}  # Optional body for POST requests
            authentication:
                type: bearer  # bearer, api_key, basic, or databricks_secret
                secret_scope: my-scope  # For databricks_secret
                secret_key: my-key
            pagination:
                type: offset  # offset, cursor, or page
                limit_param: limit
                offset_param: offset
                page_size: 100
            options:
                timeout: 30
                flatten_nested: true  # Convert nested objects to JSON strings
                infer_schema: true  # Auto-infer schema from response
            schema: |  # Optional explicit schema
                id INT,
                name STRING,
                email STRING
    """

    # Known configuration options for REST API source
    KNOWN_OPTIONS = {
        "timeout",
        "flatten_nested",
        "infer_schema",
        "batch_size",
        "retry_count",
        "retry_delay",
    }

    MANDATORY_FIELDS = {"endpoint"}

    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")
        self.add_import("from pyspark.sql import functions as F")
        self.add_import("import requests")
        self.add_import("import json")
        self.logger = logging.getLogger(__name__)

    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        """Generate REST API load code."""
        source_config = action.source
        if isinstance(source_config, str):
            raise ValueError("REST API source must be a configuration object")

        # Process source config through substitution manager if available
        if "substitution_manager" in context:
            source_config = context["substitution_manager"].substitute_yaml(
                source_config
            )

        # Validate mandatory fields
        self._validate_config(source_config, action.name)

        # Extract configuration
        endpoint = source_config.get("endpoint")
        method = source_config.get("method", "GET").upper()
        headers = source_config.get("headers", {})
        body = source_config.get("body", {})
        authentication = source_config.get("authentication", {})
        pagination = source_config.get("pagination", {})
        options = source_config.get("options", {})
        schema = source_config.get("schema")
        exclusion_list = source_config.get("exclusion_list", [])

        # Process options with defaults
        timeout = options.get("timeout", 30)
        flatten_nested = options.get("flatten_nested", True)
        infer_schema = options.get("infer_schema", True)

        # Handle operational metadata
        add_operational_metadata, metadata_columns = self._get_operational_metadata(
            action, context
        )

        # Apply context substitutions for metadata
        for col_name, expression in metadata_columns.items():
            metadata_columns[col_name] = expression.replace(
                "${source_table}", endpoint
            )

        # Determine read mode
        read_mode = action.readMode or "batch"

        template_context = {
            "action_name": action.name,
            "target_view": action.target,
            "endpoint": endpoint,
            "method": method,
            "headers": headers,
            "body": body,
            "authentication": authentication,
            "pagination": pagination,
            "timeout": timeout,
            "flatten_nested": flatten_nested,
            "infer_schema": infer_schema,
            "schema": schema,
            "exclusion_list": exclusion_list,
            "read_mode": read_mode,
            "description": action.description or f"REST API source: {endpoint}",
            "add_operational_metadata": add_operational_metadata,
            "metadata_columns": metadata_columns,
            "flowgroup": context.get("flowgroup"),
        }

        return self.render_template("load/rest_api.py.j2", template_context)

    def _validate_config(self, config: Dict[str, Any], action_name: str) -> None:
        """Validate REST API configuration."""
        missing = self.MANDATORY_FIELDS - set(config.keys())
        if missing:
            raise ValueError(
                f"REST API action '{action_name}' missing mandatory fields: {missing}"
            )

        # Validate authentication type if provided
        auth = config.get("authentication", {})
        if auth:
            auth_type = auth.get("type", "").lower()
            valid_types = {"bearer", "api_key", "basic", "databricks_secret", "oauth"}
            if auth_type and auth_type not in valid_types:
                raise ValueError(
                    f"REST API action '{action_name}' has invalid authentication type: "
                    f"'{auth_type}'. Valid types: {valid_types}"
                )

        # Validate pagination type if provided
        pagination = config.get("pagination", {})
        if pagination:
            pagination_type = pagination.get("type", "").lower()
            valid_pagination = {"offset", "cursor", "page", "link"}
            if pagination_type and pagination_type not in valid_pagination:
                raise ValueError(
                    f"REST API action '{action_name}' has invalid pagination type: "
                    f"'{pagination_type}'. Valid types: {valid_pagination}"
                )

        # Validate method
        method = config.get("method", "GET").upper()
        valid_methods = {"GET", "POST"}
        if method not in valid_methods:
            raise ValueError(
                f"REST API action '{action_name}' has invalid method: "
                f"'{method}'. Valid methods: {valid_methods}"
            )
