"""
Prompt templates for LLM-powered SQL generation.

These templates are designed to:
1. Provide clear context about the source data
2. Guide the LLM to generate valid Databricks SQL
3. Return structured, parseable responses
"""

# Main transformation prompt
TRANSFORM_PROMPT = """You are an expert Databricks SQL developer. Generate SQL to transform data based on the user's request.

## Source Table Information

{schema_context}

## Sample Data (first 5 rows)

{sample_data}

## User Request

{user_request}

## Target Layer

Create the output table in the `{target_schema}` schema.

## Rules

1. Use CREATE OR REPLACE TABLE {catalog}.{target_schema}.<table_name> AS SELECT ...
2. Generate a descriptive target table name based on the transformation
3. Handle NULL values appropriately
4. Use proper Databricks/Spark SQL syntax
5. Add SQL comments explaining key transformation steps
6. For deduplication, use ROW_NUMBER() with appropriate ordering
7. For name standardization, use INITCAP() for proper case
8. For email standardization, use LOWER()
9. Preserve data types appropriately

## Response Format

Return ONLY a valid JSON object with this exact structure (no markdown, no extra text):

{{
    "target_table": "{catalog}.{target_schema}.table_name",
    "sql": "CREATE OR REPLACE TABLE ... (full SQL statement)",
    "transformations_applied": ["list", "of", "transformations"],
    "explanation": "Brief description of what the SQL does",
    "estimated_impact": {{
        "rows_before": {row_count},
        "estimated_rows_after": <your estimate>,
        "columns_modified": ["list of modified columns"]
    }}
}}

Return ONLY the JSON object, nothing else."""


# Schema description template
SCHEMA_TEMPLATE = """Table: {full_name}
Total Rows: {row_count:,}

Columns:
{columns}"""


# Column description template  
COLUMN_TEMPLATE = "  - {name}: {data_type}"


# Prompt for parsing natural language to identify tables
TABLE_IDENTIFICATION_PROMPT = """Identify the source table from this user request.

User Request: {user_request}

Available Tables:
{available_tables}

Return ONLY a JSON object:
{{
    "source_schema": "schema_name",
    "source_table": "table_name",
    "target_schema": "silver or gold",
    "confidence": "high/medium/low"
}}

If the user doesn't specify a schema, assume "bronze" for source.
If the user doesn't specify a target, use "silver" for cleaning/staging, "gold" for aggregations/analytics.

Return ONLY the JSON object."""


# Prompt for schedule parsing
SCHEDULE_PARSE_PROMPT = """Convert this natural language schedule to a cron expression.

User Input: {schedule_text}

Return ONLY a JSON object:
{{
    "cron_expression": "0 6 * * 1",
    "human_readable": "Every Monday at 6:00 AM",
    "timezone": "UTC"
}}

Common patterns:
- "every day at 6am" → "0 6 * * *"
- "every Monday at 6am" → "0 6 * * 1"  
- "hourly" → "0 * * * *"
- "every Sunday at midnight" → "0 0 * * 0"

Return ONLY the JSON object."""


def build_schema_context(table_schema) -> str:
    """
    Build a formatted schema context string for the prompt.
    
    Args:
        table_schema: TableSchema object from DatabricksService
        
    Returns:
        Formatted string for prompt injection
    """
    columns_str = "\n".join(
        COLUMN_TEMPLATE.format(name=col.name, data_type=col.data_type)
        for col in table_schema.columns
    )
    
    return SCHEMA_TEMPLATE.format(
        full_name=table_schema.full_name,
        row_count=table_schema.row_count or 0,
        columns=columns_str
    )


def build_sample_data_context(query_result) -> str:
    """
    Build a formatted sample data string for the prompt.
    
    Args:
        query_result: QueryResult from DatabricksService.get_sample_data()
        
    Returns:
        Formatted string showing sample rows
    """
    if not query_result.rows:
        return "(No sample data available)"
    
    lines = []
    
    # Header
    lines.append(" | ".join(query_result.columns))
    lines.append("-" * 80)
    
    # Rows
    for row in query_result.rows:
        formatted_values = []
        for val in row:
            if val is None:
                formatted_values.append("NULL")
            else:
                formatted_values.append(str(val))
        lines.append(" | ".join(formatted_values))
    
    return "\n".join(lines)


def build_transform_prompt(
    user_request: str,
    table_schema,
    sample_data,
    target_schema: str = "silver",
    catalog: str = "workspace"
) -> str:
    """
    Build the complete transformation prompt.
    
    Args:
        user_request: User's natural language request
        table_schema: TableSchema object
        sample_data: QueryResult with sample rows
        target_schema: Target schema (silver/gold)
        catalog: Catalog name
        
    Returns:
        Complete prompt string
    """
    schema_context = build_schema_context(table_schema)
    sample_context = build_sample_data_context(sample_data)
    
    return TRANSFORM_PROMPT.format(
        schema_context=schema_context,
        sample_data=sample_context,
        user_request=user_request,
        target_schema=target_schema,
        catalog=catalog,
        row_count=table_schema.row_count or 0
    )