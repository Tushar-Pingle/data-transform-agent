"""
Transform Agent

The main orchestrator that:
1. Understands user requests (questions vs transforms vs commands)
2. Coordinates between services
3. Manages the confirm/execute flow
4. Tracks conversation state
"""

import re
from typing import Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from src.services.databricks_service import DatabricksService, TableSchema
from src.services.llm_service import LLMService, TransformPlan


class AgentState(Enum):
    """Current state of the agent conversation."""
    IDLE = "idle"
    AWAITING_CONFIRMATION = "awaiting"
    EXECUTING = "executing"


class UserIntent(Enum):
    """Type of user request."""
    QUESTION = "question"           # Asking about data/tables
    TRANSFORM = "transform"         # Requesting a transformation
    COMMAND = "command"             # Utility command
    UNCLEAR = "unclear"             # Can't determine


@dataclass
class PendingTransform:
    """A transform waiting for user confirmation."""
    plan: TransformPlan
    source_table: str
    source_schema: TableSchema
    user_request: str


@dataclass 
class TransformResult:
    """Result of an executed transform."""
    success: bool
    target_table: str
    rows_created: int
    message: str
    sql_executed: str


class TransformAgent:
    """
    Conversational agent for data transformations.
    
    Now with intent detection - can answer questions AND do transforms!
    """
    
    def __init__(self):
        """Initialize the agent with required services."""
        self.db = DatabricksService()
        self.llm = LLMService()
        
        self.state = AgentState.IDLE
        self.pending_transform: Optional[PendingTransform] = None
        self.last_result: Optional[TransformResult] = None
        self.last_mentioned_table: Optional[Tuple[str, str]] = None  # (table, schema)
        
        self.db.connect()
    
    def chat(self, user_message: str) -> str:
        """Process a user message and return a response."""
        message = user_message.strip()
        message_lower = message.lower()
        
        # Handle confirmation state first
        if self.state == AgentState.AWAITING_CONFIRMATION:
            if message_lower in ["confirm", "yes", "y", "execute", "run"]:
                return self._execute_pending_transform()
            elif message_lower in ["cancel", "no", "n", "abort"]:
                return self._cancel_pending_transform()
            elif message_lower in ["show sql", "sql"]:
                return self._show_pending_sql()
            else:
                return self._handle_confirmation_unclear(user_message)
        
        # Handle explicit commands
        if message_lower in ["help", "?"]:
            return self._show_help()
        
        if message_lower in ["show tables", "list tables", "tables"]:
            return self._list_tables()
        
        if message_lower.startswith("describe ") or message_lower.startswith("desc "):
            table_name = message.split(" ", 1)[1].strip()
            return self._describe_table(table_name)
        
        if message_lower in ["status", "state"]:
            return self._show_status()
        
        # Detect intent for other messages
        intent = self._detect_intent(message)
        
        if intent == UserIntent.QUESTION:
            return self._handle_question(message)
        elif intent == UserIntent.TRANSFORM:
            return self._handle_transform_request(message)
        else:
            # Try to be helpful
            return self._handle_unclear_intent(message)
    
    def _detect_intent(self, message: str) -> UserIntent:
        """
        Detect whether the user is asking a question or requesting a transform.
        """
        message_lower = message.lower()
        
        # Question indicators
        question_patterns = [
            r'^what\s',
            r'^which\s',
            r'^how\s',
            r'^can you (tell|show|list|explain)',
            r'^tell me',
            r'^show me',
            r'^list\s',
            r'^do(es)?\s.*\?',
            r'^is\s.*\?',
            r'^are\s.*\?',
            r'\?$',  # Ends with question mark
            r'what (are|is) (the|in)',
            r'how many',
            r'tell me about',
            r'explain',
            r'describe',  # When not a command
        ]
        
        for pattern in question_patterns:
            if re.search(pattern, message_lower):
                return UserIntent.QUESTION
        
        # Transform indicators
        transform_patterns = [
            r'clean\s',
            r'transform\s',
            r'convert\s',
            r'create\s',
            r'remove\s.*(null|duplicate|dupe)',
            r'dedupe',
            r'deduplicate',
            r'standardize',
            r'normalize',
            r'aggregate',
            r'save (to|into)',
            r'move (to|into)',
            r'filter\s',
            r'(remove|drop|delete)\s.*rows',
            r'(add|create)\s.*column',
            r'rename\s',
            r'cast\s',
            r'join\s',
            r'merge\s',
        ]
        
        for pattern in transform_patterns:
            if re.search(pattern, message_lower):
                return UserIntent.TRANSFORM
        
        # If mentions a table with action-like words, probably a transform
        if self._identify_table(message)[0]:
            action_words = ['clean', 'fix', 'update', 'change', 'modify', 'process']
            if any(word in message_lower for word in action_words):
                return UserIntent.TRANSFORM
        
        # Default to question if short or seems conversational
        if len(message.split()) < 5:
            return UserIntent.UNCLEAR
        
        return UserIntent.UNCLEAR
    
    def _handle_question(self, question: str) -> str:
        """Handle a question about data/tables."""
        question_lower = question.lower()
        
        # Try to identify which table they're asking about
        table, schema = self._identify_table(question)
        
        # Check for "this table" or "that table" references
        if not table and self.last_mentioned_table:
            if any(word in question_lower for word in ['this table', 'that table', 'the table', 'it']):
                table, schema = self.last_mentioned_table
        
        # Questions about columns
        if any(phrase in question_lower for phrase in [
            'column', 'columns', 'field', 'fields', 'schema', 
            'what are the', 'what is in', 'structure'
        ]):
            if table:
                self.last_mentioned_table = (table, schema)
                return self._describe_table_columns(table, schema)
            else:
                return self._ask_which_table("see the columns of")
        
        # Questions about data/rows
        if any(phrase in question_lower for phrase in [
            'how many rows', 'row count', 'how much data', 'sample', 
            'show me data', 'what does', 'look like', 'preview'
        ]):
            if table:
                self.last_mentioned_table = (table, schema)
                return self._show_table_preview(table, schema)
            else:
                return self._ask_which_table("preview")
        
        # Questions about tables available
        if any(phrase in question_lower for phrase in [
            'what tables', 'which tables', 'available tables', 
            'list tables', 'show tables', 'tables do'
        ]):
            return self._list_tables()
        
        # Questions about a specific table (general)
        if table:
            self.last_mentioned_table = (table, schema)
            return self._describe_table(f"{schema}.{table}")
        
        # Questions about capabilities
        if any(phrase in question_lower for phrase in [
            'what can you', 'how do i', 'how to', 'help me'
        ]):
            return self._show_help()
        
        # Generic question - provide helpful response
        return self._handle_generic_question(question)
    
    def _describe_table_columns(self, table: str, schema: str) -> str:
        """Describe just the columns of a table."""
        try:
            ts = self.db.get_table_schema(table, schema)
            
            lines = [
                f"## 📋 Columns in `{schema}.{table}`\n",
                f"**Total Columns:** {len(ts.columns)}\n",
            ]
            
            for col in ts.columns:
                nullable = "nullable" if col.nullable else "not null"
                lines.append(f"  • **{col.name}** (`{col.data_type}`) - {nullable}")
            
            lines.append(f"\n**Rows in table:** {ts.row_count:,}")
            lines.append("\n*Tip: Say `describe " + f"{schema}.{table}" + "` to see sample data too.*")
            
            return "\n".join(lines)
            
        except Exception as e:
            return f"❌ Error getting columns: {str(e)}"
    
    def _show_table_preview(self, table: str, schema: str) -> str:
        """Show a preview of table data."""
        try:
            ts = self.db.get_table_schema(table, schema)
            sample = self.db.get_sample_data(table, schema, limit=5)
            
            lines = [
                f"## 📊 Preview: `{schema}.{table}`\n",
                f"**Total Rows:** {ts.row_count:,}\n",
                "### Sample Data (5 rows):",
                "```",
            ]
            
            # Header
            lines.append(" | ".join(c.name for c in ts.columns))
            lines.append("-" * 70)
            
            # Rows
            for row in sample.rows:
                values = [str(v)[:15] if v is not None else "NULL" for v in row]
                lines.append(" | ".join(values))
            
            lines.append("```")
            
            return "\n".join(lines)
            
        except Exception as e:
            return f"❌ Error previewing table: {str(e)}"
    
    def _ask_which_table(self, action: str) -> str:
        """Ask user to specify which table."""
        tables = []
        for schema in ["bronze", "silver", "gold"]:
            try:
                for t in self.db.list_tables(schema):
                    tables.append(f"`{schema}.{t}`")
            except:
                continue
        
        tables_str = ", ".join(tables) if tables else "No tables found"
        
        return f"""Which table would you like to {action}?

**Available tables:** {tables_str}

**Example:** "What columns are in raw_customers?"
"""
    
    def _handle_generic_question(self, question: str) -> str:
        """Handle a question we don't specifically understand."""
        return f"""I'm not sure how to answer that question. Here's what I can help with:

**Questions I can answer:**
  • "What columns are in raw_customers?"
  • "How many rows are in bronze.raw_customers?"
  • "Show me a preview of the customers table"
  • "What tables are available?"

**Transformations I can do:**
  • "Clean raw_customers, remove nulls, dedupe by contact_id"
  • "Standardize emails to lowercase"

**Commands:**
  • `show tables` - List all tables
  • `describe <table>` - See table details
  • `help` - Full help guide
"""
    
    def _handle_unclear_intent(self, message: str) -> str:
        """Handle messages where we can't determine intent."""
        table, schema = self._identify_table(message)
        
        if table:
            self.last_mentioned_table = (table, schema)
            return f"""I found table `{schema}.{table}` in your message. What would you like to do?

**Options:**
  • "Show me the columns" - See table structure
  • "Preview the data" - See sample rows
  • "Clean it and remove nulls" - Start a transformation

Or be more specific about what you'd like to know or do!
"""
        else:
            return self._handle_generic_question(message)
    
    def _handle_transform_request(self, user_request: str) -> str:
        """Process a natural language transform request."""
        try:
            # Step 1: Identify the source table
            source_table, source_schema_name = self._identify_table(user_request)
            
            # Check for "this table" reference
            if not source_table and self.last_mentioned_table:
                if any(word in user_request.lower() for word in ['this table', 'that table', 'the table', 'it ']):
                    source_table, source_schema_name = self.last_mentioned_table
            
            if not source_table:
                return self._ask_for_table_clarification(user_request)
            
            self.last_mentioned_table = (source_table, source_schema_name)
            
            # Step 2: Get table schema and sample data
            table_schema = self.db.get_table_schema(
                table=source_table,
                schema=source_schema_name
            )
            sample_data = self.db.get_sample_data(
                table=source_table,
                schema=source_schema_name,
                limit=5
            )
            
            # Step 3: Determine target layer
            target_schema = self._determine_target_layer(user_request, source_schema_name)
            
            # Step 4: Generate transform plan via LLM
            plan = self.llm.generate_transform_sql(
                user_request=user_request,
                table_schema=table_schema,
                sample_data=sample_data,
                target_schema=target_schema,
                catalog=self.db.settings.catalog
            )
            
            # Step 5: Store pending transform and ask for confirmation
            self.pending_transform = PendingTransform(
                plan=plan,
                source_table=f"{source_schema_name}.{source_table}",
                source_schema=table_schema,
                user_request=user_request
            )
            self.state = AgentState.AWAITING_CONFIRMATION
            
            return self._format_plan_for_confirmation(plan, table_schema)
            
        except Exception as e:
            return f"❌ **Error processing request:**\n\n{str(e)}\n\nPlease try rephrasing or use `help` to see available commands."
    
    def _identify_table(self, user_request: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract table name from user request."""
        request_lower = user_request.lower()
        
        for schema in ["bronze", "silver", "gold"]:
            try:
                tables = self.db.list_tables(schema)
                for table in tables:
                    if table.lower() in request_lower:
                        return table, schema
                    clean_name = table.lower().replace("raw_", "").replace("stg_", "")
                    if clean_name in request_lower and len(clean_name) > 2:
                        return table, schema
            except:
                continue
        
        pattern = r'(\w+)\.(\w+)'
        match = re.search(pattern, user_request)
        if match:
            schema, table = match.groups()
            if self.db.table_exists(table, schema):
                return table, schema
        
        return None, None
    
    def _ask_for_table_clarification(self, user_request: str) -> str:
        """Ask user to clarify which table they want to transform."""
        available = []
        for schema in ["bronze", "silver", "gold"]:
            try:
                tables = self.db.list_tables(schema)
                for table in tables:
                    available.append(f"{schema}.{table}")
            except:
                continue
        
        if available:
            tables_list = "\n".join(f"  • `{t}`" for t in available)
            return f"""I couldn't identify which table you want to transform.

**Available tables:**
{tables_list}

**Please try again with the table name, for example:**
  • "Clean bronze.raw_customers and remove nulls"
  • "Transform raw_customers to silver"
"""
        else:
            return "❌ No tables found. Please create some tables first."
    
    def _determine_target_layer(self, user_request: str, source_schema: str) -> str:
        """Determine the target layer based on request and source."""
        request_lower = user_request.lower()
        
        if "gold" in request_lower:
            return "gold"
        if "silver" in request_lower:
            return "silver"
        
        if any(word in request_lower for word in ["aggregate", "summary", "report", "metrics"]):
            return "gold"
        
        if source_schema == "bronze":
            return "silver"
        elif source_schema == "silver":
            return "gold"
        else:
            return "silver"
    
    def _format_plan_for_confirmation(self, plan: TransformPlan, source_schema: TableSchema) -> str:
        """Format the transform plan for user review."""
        lines = [
            "## 📋 Transform Plan\n",
            f"**Source:** `{source_schema.full_name}` ({source_schema.row_count:,} rows)\n",
            f"**Target:** `{plan.target_table}`\n",
            "### Transformations:",
        ]
        
        for t in plan.transformations_applied:
            lines.append(f"  • {t}")
        
        lines.append(f"\n**Explanation:** {plan.explanation}\n")
        
        if plan.estimated_rows_after:
            lines.append(f"**Estimated Output:** ~{plan.estimated_rows_after:,} rows\n")
        
        lines.extend([
            "### Generated SQL:",
            "```sql",
            plan.sql,
            "```\n",
            "---",
            "**Commands:**",
            "  • `confirm` - Execute this transform",
            "  • `cancel` - Abort and start over",
            "  • `show sql` - Show SQL again",
        ])
        
        return "\n".join(lines)
    
    def _execute_pending_transform(self) -> str:
        """Execute the pending transform."""
        if not self.pending_transform:
            self.state = AgentState.IDLE
            return "❌ No pending transform to execute."
        
        self.state = AgentState.EXECUTING
        plan = self.pending_transform.plan
        
        try:
            self.db.execute_command(plan.sql)
            
            result = self.db.execute_query(f"SELECT COUNT(*) FROM {plan.target_table}")
            row_count = result.rows[0][0] if result.rows else 0
            
            self.last_result = TransformResult(
                success=True,
                target_table=plan.target_table,
                rows_created=row_count,
                message="Transform executed successfully",
                sql_executed=plan.sql
            )
            
            self.pending_transform = None
            self.state = AgentState.IDLE
            
            return self._format_success_message(plan.target_table, row_count)
            
        except Exception as e:
            self.state = AgentState.IDLE
            self.pending_transform = None
            return f"❌ **Execution failed:**\n\n```\n{str(e)}\n```\n\nPlease check the SQL and try again."
    
    def _format_success_message(self, target_table: str, row_count: int) -> str:
        """Format success message after execution."""
        return f"""## ✅ Transform Complete!

**Created:** `{target_table}`
**Rows:** {row_count:,}

You can now:
- Query the new table in Databricks
- Use it as a source for further transformations
- Ask me to create another transform

**Example:** "What columns are in {target_table.split('.')[-1]}?"
"""
    
    def _cancel_pending_transform(self) -> str:
        """Cancel the pending transform."""
        self.pending_transform = None
        self.state = AgentState.IDLE
        return "🚫 Transform cancelled. Ready for a new request."
    
    def _show_pending_sql(self) -> str:
        """Show the SQL of pending transform."""
        if not self.pending_transform:
            return "No pending transform."
        
        return f"""## Generated SQL
```sql
{self.pending_transform.plan.sql}
```

**Commands:** `confirm` to execute, `cancel` to abort.
"""
    
    def _handle_confirmation_unclear(self, user_message: str) -> str:
        """Handle unclear response during confirmation."""
        return """I have a transform ready to execute. Please respond with:

  • `confirm` or `yes` - Execute the transform
  • `cancel` or `no` - Cancel and start over
  • `show sql` - View the SQL again
"""
    
    def _list_tables(self) -> str:
        """List all available tables."""
        lines = ["## 📋 Available Tables\n"]
        
        for schema in ["bronze", "silver", "gold"]:
            try:
                tables = self.db.list_tables(schema)
                if tables:
                    lines.append(f"### {schema.upper()}")
                    for table in tables:
                        try:
                            ts = self.db.get_table_schema(table, schema, include_row_count=True)
                            lines.append(f"  • `{schema}.{table}` ({ts.row_count:,} rows)")
                        except:
                            lines.append(f"  • `{schema}.{table}`")
                    lines.append("")
            except:
                continue
        
        return "\n".join(lines)
    
    def _describe_table(self, table_ref: str) -> str:
        """Describe a specific table."""
        parts = table_ref.replace("`", "").split(".")
        
        if len(parts) == 2:
            schema, table = parts
        elif len(parts) == 1:
            table = parts[0]
            schema = None
            for s in ["bronze", "silver", "gold"]:
                if self.db.table_exists(table, s):
                    schema = s
                    break
            if not schema:
                return f"❌ Table `{table}` not found in bronze, silver, or gold schemas."
        else:
            return f"❌ Invalid table reference: `{table_ref}`"
        
        try:
            self.last_mentioned_table = (table, schema)
            ts = self.db.get_table_schema(table, schema)
            sample = self.db.get_sample_data(table, schema, limit=3)
            
            lines = [
                f"## 📊 {ts.full_name}\n",
                f"**Rows:** {ts.row_count:,}\n",
                "### Columns:",
            ]
            
            for col in ts.columns:
                lines.append(f"  • `{col.name}`: {col.data_type}")
            
            lines.append("\n### Sample Data (3 rows):")
            lines.append("```")
            lines.append(" | ".join(c.name for c in ts.columns))
            lines.append("-" * 60)
            
            for row in sample.rows:
                values = [str(v)[:20] if v is not None else "NULL" for v in row]
                lines.append(" | ".join(values))
            
            lines.append("```")
            
            return "\n".join(lines)
            
        except Exception as e:
            return f"❌ Error describing table: {str(e)}"
    
    def _show_status(self) -> str:
        """Show current agent status."""
        lines = [
            "## 🔄 Agent Status\n",
            f"**State:** {self.state.value}",
            f"**Connected to:** {self.db.settings.host}",
            f"**Catalog:** {self.db.settings.catalog}",
        ]
        
        if self.pending_transform:
            lines.append(f"\n**Pending Transform:**")
            lines.append(f"  • Source: {self.pending_transform.source_table}")
            lines.append(f"  • Target: {self.pending_transform.plan.target_table}")
        
        if self.last_result:
            lines.append(f"\n**Last Result:**")
            lines.append(f"  • {'✅ Success' if self.last_result.success else '❌ Failed'}")
            lines.append(f"  • Table: {self.last_result.target_table}")
            lines.append(f"  • Rows: {self.last_result.rows_created:,}")
        
        return "\n".join(lines)
    
    def _show_help(self) -> str:
        """Show help message."""
        return """## 🤖 Data Transform Agent - Help

### Ask Questions
  • "What columns are in raw_customers?"
  • "How many rows are in the customers table?"
  • "Show me a preview of the data"
  • "What tables are available?"

### Transform Data
  • "Clean raw_customers, remove nulls, dedupe by contact_id"
  • "Standardize emails to lowercase"
  • "Aggregate sales by region, save to gold"

### Commands
  • `show tables` - List all available tables
  • `describe <table>` - Show table schema and sample data
  • `status` - Show agent status
  • `help` - Show this message

### During Confirmation
  • `confirm` / `yes` - Execute the transform
  • `cancel` / `no` - Cancel and start over
  • `show sql` - View the generated SQL
"""
    
    def close(self):
        """Clean up resources."""
        self.db.close()