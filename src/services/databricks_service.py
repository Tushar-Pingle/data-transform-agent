"""
Databricks Service

Handles all interactions with Databricks SQL Warehouse:
- Connection management
- Schema exploration
- Query execution
- Table operations
"""

from typing import Optional
from dataclasses import dataclass, field
from databricks import sql as databricks_sql
from databricks.sql.client import Connection, Cursor

from src.config.settings import get_databricks_settings


@dataclass
class TableColumn:
    """Represents a column in a table."""
    name: str
    data_type: str
    nullable: bool = True
    comment: Optional[str] = None


@dataclass
class TableSchema:
    """Represents a table's schema information."""
    catalog: str
    schema: str
    table: str
    columns: list[TableColumn] = field(default_factory=list)
    row_count: Optional[int] = None
    
    @property
    def full_name(self) -> str:
        """Return fully qualified table name."""
        return f"{self.catalog}.{self.schema}.{self.table}"


@dataclass
class QueryResult:
    """Represents the result of a SQL query."""
    columns: list[str]
    rows: list[tuple]
    row_count: int
    
    def to_dict_list(self) -> list[dict]:
        """Convert results to list of dictionaries."""
        return [dict(zip(self.columns, row)) for row in self.rows]


class DatabricksService:
    """
    Service class for Databricks SQL Warehouse operations.
    
    Usage:
        service = DatabricksService()
        service.connect()
        
        schemas = service.list_schemas()
        tables = service.list_tables("bronze")
        schema = service.get_table_schema("bronze.raw_customers")
        
        service.close()
    
    Or use as context manager:
        with DatabricksService() as service:
            schemas = service.list_schemas()
    """
    
    def __init__(self):
        """Initialize the service with settings from environment."""
        self.settings = get_databricks_settings()
        self._connection: Optional[Connection] = None
    
    def connect(self) -> "DatabricksService":
        """
        Establish connection to Databricks SQL Warehouse.
        
        Returns:
            self for method chaining
        """
        if self._connection is not None:
            return self
        
        self._connection = databricks_sql.connect(
            server_hostname=self.settings.host,
            http_path=self.settings.http_path,
            access_token=self.settings.token,
        )
        return self
    
    def close(self) -> None:
        """Close the database connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
    
    def __enter__(self) -> "DatabricksService":
        """Context manager entry."""
        return self.connect()
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
    
    def _get_cursor(self) -> Cursor:
        """Get a cursor, ensuring connection exists."""
        if self._connection is None:
            self.connect()
        return self._connection.cursor()
    
    def execute_query(self, sql: str) -> QueryResult:
        """
        Execute a SQL query and return results.
        
        Args:
            sql: SQL query to execute
            
        Returns:
            QueryResult with columns, rows, and row count
        """
        cursor = self._get_cursor()
        try:
            cursor.execute(sql)
            
            # Get column names from description
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Fetch all rows
            rows = cursor.fetchall()
            
            return QueryResult(
                columns=columns,
                rows=rows,
                row_count=len(rows)
            )
        finally:
            cursor.close()
    
    def execute_command(self, sql: str) -> bool:
        """
        Execute a SQL command (CREATE, INSERT, etc.) that doesn't return results.
        
        Args:
            sql: SQL command to execute
            
        Returns:
            True if successful
        """
        cursor = self._get_cursor()
        try:
            cursor.execute(sql)
            return True
        finally:
            cursor.close()
    
    def list_catalogs(self) -> list[str]:
        """
        List all available catalogs.
        
        Returns:
            List of catalog names
        """
        result = self.execute_query("SHOW CATALOGS")
        return [row[0] for row in result.rows]
    
    def list_schemas(self, catalog: Optional[str] = None) -> list[str]:
        """
        List all schemas in a catalog.
        
        Args:
            catalog: Catalog name (defaults to configured catalog)
            
        Returns:
            List of schema names
        """
        catalog = catalog or self.settings.catalog
        result = self.execute_query(f"SHOW SCHEMAS IN {catalog}")
        return [row[0] for row in result.rows]
    
    def list_tables(self, schema: str, catalog: Optional[str] = None) -> list[str]:
        """
        List all tables in a schema.
        
        Args:
            schema: Schema name
            catalog: Catalog name (defaults to configured catalog)
            
        Returns:
            List of table names
        """
        catalog = catalog or self.settings.catalog
        result = self.execute_query(f"SHOW TABLES IN {catalog}.{schema}")
        # SHOW TABLES returns: database, tableName, isTemporary
        return [row[1] for row in result.rows]
    
    def table_exists(self, table: str, schema: str, catalog: Optional[str] = None) -> bool:
        """
        Check if a table exists.
        
        Args:
            table: Table name
            schema: Schema name
            catalog: Catalog name (defaults to configured catalog)
            
        Returns:
            True if table exists
        """
        tables = self.list_tables(schema, catalog)
        return table in tables
    
    def get_table_schema(
        self, 
        table: str, 
        schema: str, 
        catalog: Optional[str] = None,
        include_row_count: bool = True
    ) -> TableSchema:
        """
        Get detailed schema information for a table.
        
        Args:
            table: Table name
            schema: Schema name  
            catalog: Catalog name (defaults to configured catalog)
            include_row_count: Whether to count rows (can be slow for large tables)
            
        Returns:
            TableSchema with column details
        """
        catalog = catalog or self.settings.catalog
        full_name = f"{catalog}.{schema}.{table}"
        
        # Get column information
        result = self.execute_query(f"DESCRIBE TABLE {full_name}")
        
        columns = []
        for row in result.rows:
            # DESCRIBE returns: col_name, data_type, comment
            col_name = row[0]
            
            # Skip partition info and other metadata rows
            if col_name.startswith("#") or col_name == "":
                continue
                
            columns.append(TableColumn(
                name=col_name,
                data_type=row[1] if len(row) > 1 else "unknown",
                comment=row[2] if len(row) > 2 and row[2] else None
            ))
        
        # Get row count if requested
        row_count = None
        if include_row_count:
            count_result = self.execute_query(f"SELECT COUNT(*) FROM {full_name}")
            row_count = count_result.rows[0][0] if count_result.rows else 0
        
        return TableSchema(
            catalog=catalog,
            schema=schema,
            table=table,
            columns=columns,
            row_count=row_count
        )
    
    def get_sample_data(
        self, 
        table: str, 
        schema: str, 
        catalog: Optional[str] = None,
        limit: int = 5
    ) -> QueryResult:
        """
        Get sample rows from a table.
        
        Args:
            table: Table name
            schema: Schema name
            catalog: Catalog name (defaults to configured catalog)
            limit: Number of rows to return
            
        Returns:
            QueryResult with sample data
        """
        catalog = catalog or self.settings.catalog
        full_name = f"{catalog}.{schema}.{table}"
        
        return self.execute_query(f"SELECT * FROM {full_name} LIMIT {limit}")
    
    def create_schema_if_not_exists(self, schema: str, catalog: Optional[str] = None) -> bool:
        """
        Create a schema if it doesn't exist.
        
        Args:
            schema: Schema name to create
            catalog: Catalog name (defaults to configured catalog)
            
        Returns:
            True if successful
        """
        catalog = catalog or self.settings.catalog
        return self.execute_command(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    
    def format_table_info(self, table_schema: TableSchema) -> str:
        """
        Format table schema as a readable string (for LLM context).
        
        Args:
            table_schema: TableSchema object
            
        Returns:
            Formatted string description
        """
        lines = [
            f"Table: {table_schema.full_name}",
            f"Row Count: {table_schema.row_count:,}" if table_schema.row_count else "Row Count: Unknown",
            "",
            "Columns:",
        ]
        
        for col in table_schema.columns:
            nullable = "nullable" if col.nullable else "not null"
            comment = f" -- {col.comment}" if col.comment else ""
            lines.append(f"  - {col.name}: {col.data_type} ({nullable}){comment}")
        
        return "\n".join(lines)