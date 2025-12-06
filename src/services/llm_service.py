"""
LLM Service

Handles all interactions with Claude API for SQL generation:
- Transform SQL generation
- Schedule parsing
- Natural language understanding
"""

import json
import re
from typing import Optional
from dataclasses import dataclass

import anthropic

from src.config.settings import get_anthropic_settings
from src.prompts.templates import (
    build_transform_prompt,
    TABLE_IDENTIFICATION_PROMPT,
    SCHEDULE_PARSE_PROMPT,
)


@dataclass
class TransformPlan:
    """Represents a planned transformation."""
    target_table: str
    sql: str
    transformations_applied: list[str]
    explanation: str
    estimated_rows_after: Optional[int] = None
    columns_modified: Optional[list[str]] = None
    
    def format_for_display(self) -> str:
        """Format the plan for user display."""
        lines = [
            "📋 **Transformation Plan**",
            "",
            f"**Target Table:** `{self.target_table}`",
            "",
            "**Transformations:**",
        ]
        
        for t in self.transformations_applied:
            lines.append(f"  • {t}")
        
        lines.extend([
            "",
            f"**Explanation:** {self.explanation}",
            "",
        ])
        
        if self.estimated_rows_after:
            lines.append(f"**Estimated Output Rows:** {self.estimated_rows_after:,}")
        
        if self.columns_modified:
            lines.append(f"**Columns Modified:** {', '.join(self.columns_modified)}")
        
        lines.extend([
            "",
            "**Generated SQL:**",
            "```sql",
            self.sql,
            "```",
        ])
        
        return "\n".join(lines)


@dataclass
class TableIdentification:
    """Result of table identification from natural language."""
    source_schema: str
    source_table: str
    target_schema: str
    confidence: str


@dataclass
class SchedulePlan:
    """Parsed schedule information."""
    cron_expression: str
    human_readable: str
    timezone: str = "UTC"


class LLMService:
    """
    Service for LLM-powered SQL generation using Claude.
    
    Usage:
        service = LLMService()
        plan = service.generate_transform_sql(
            user_request="Clean and dedupe customers",
            table_schema=schema,
            sample_data=sample
        )
    """
    
    def __init__(self):
        """Initialize with settings from environment."""
        self.settings = get_anthropic_settings()
        self.client = anthropic.Anthropic(api_key=self.settings.api_key)
    
    def _call_claude(self, prompt: str, max_tokens: int = 4096) -> str:
        """
        Make a call to Claude API.
        
        Args:
            prompt: The prompt to send
            max_tokens: Maximum tokens in response
            
        Returns:
            Response text from Claude
        """
        message = self.client.messages.create(
            model=self.settings.model,
            max_tokens=max_tokens,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        return message.content[0].text
    
    def _parse_json_response(self, response: str) -> dict:
        """
        Parse JSON from Claude's response, handling common issues.
        
        Args:
            response: Raw response text from Claude
            
        Returns:
            Parsed JSON as dictionary
            
        Raises:
            ValueError: If JSON parsing fails
        """
        # Clean the response
        text = response.strip()
        
        # Remove markdown code blocks if present
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]
        
        # Remove any leading/trailing whitespace
        text = text.strip()
        
        # Try to find JSON object boundaries
        if not text.startswith("{"):
            # Try to find the start of JSON
            match = re.search(r'\{', text)
            if match:
                text = text[match.start():]
        
        if not text.endswith("}"):
            # Try to find the end of JSON
            match = re.search(r'\}(?!.*\})', text)
            if match:
                text = text[:match.end()]
        
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON response: {e}\nResponse was: {response[:500]}")
    
    def generate_transform_sql(
        self,
        user_request: str,
        table_schema,
        sample_data,
        target_schema: str = "silver",
        catalog: str = "workspace"
    ) -> TransformPlan:
        """
        Generate SQL transformation from natural language request.
        
        Args:
            user_request: Natural language description of transformation
            table_schema: TableSchema object from DatabricksService
            sample_data: QueryResult with sample rows
            target_schema: Target schema (silver/gold)
            catalog: Catalog name
            
        Returns:
            TransformPlan with SQL and metadata
        """
        # Build the prompt
        prompt = build_transform_prompt(
            user_request=user_request,
            table_schema=table_schema,
            sample_data=sample_data,
            target_schema=target_schema,
            catalog=catalog
        )
        
        # Call Claude
        response = self._call_claude(prompt)
        
        # Parse the response
        data = self._parse_json_response(response)
        
        # Extract estimated impact if present
        estimated_rows = None
        columns_modified = None
        if "estimated_impact" in data:
            impact = data["estimated_impact"]
            estimated_rows = impact.get("estimated_rows_after")
            columns_modified = impact.get("columns_modified")
        
        return TransformPlan(
            target_table=data["target_table"],
            sql=data["sql"],
            transformations_applied=data.get("transformations_applied", []),
            explanation=data.get("explanation", ""),
            estimated_rows_after=estimated_rows,
            columns_modified=columns_modified
        )
    
    def identify_table(
        self,
        user_request: str,
        available_tables: list[str]
    ) -> TableIdentification:
        """
        Identify which table the user is referring to.
        
        Args:
            user_request: User's natural language request
            available_tables: List of available table names
            
        Returns:
            TableIdentification with parsed table info
        """
        tables_str = "\n".join(f"  - {t}" for t in available_tables)
        
        prompt = TABLE_IDENTIFICATION_PROMPT.format(
            user_request=user_request,
            available_tables=tables_str
        )
        
        response = self._call_claude(prompt, max_tokens=256)
        data = self._parse_json_response(response)
        
        return TableIdentification(
            source_schema=data["source_schema"],
            source_table=data["source_table"],
            target_schema=data["target_schema"],
            confidence=data["confidence"]
        )
    
    def parse_schedule(self, schedule_text: str) -> SchedulePlan:
        """
        Parse natural language schedule to cron expression.
        
        Args:
            schedule_text: Natural language schedule description
            
        Returns:
            SchedulePlan with cron expression
        """
        prompt = SCHEDULE_PARSE_PROMPT.format(schedule_text=schedule_text)
        
        response = self._call_claude(prompt, max_tokens=256)
        data = self._parse_json_response(response)
        
        return SchedulePlan(
            cron_expression=data["cron_expression"],
            human_readable=data["human_readable"],
            timezone=data.get("timezone", "UTC")
        )