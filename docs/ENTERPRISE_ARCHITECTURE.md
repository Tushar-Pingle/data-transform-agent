# 🏗️ Enterprise Data Transform Agent - Architecture & POC Blueprint

## Executive Summary

This document outlines the complete architecture for an **enterprise-grade LLM-powered Data Transform Agent** that can:

1. **Understand complex data landscapes** through metadata catalogs
2. **Auto-discover relationships** between tables (ERD awareness)
3. **Translate business language** to technical SQL (semantic layer)
4. **Handle multi-table transformations** with proper joins
5. **Support the full Bronze → Silver → Gold pipeline**

---

## 📊 Current State vs Target State

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CURRENT STATE (Phase 1)                            │
│                              "Working Demo"                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────┐      ┌──────────┐      ┌──────────┐      ┌──────────┐       │
│  │  User    │ ───▶ │   LLM    │ ───▶ │  Single  │ ───▶ │  Output  │       │
│  │  Prompt  │      │  Service │      │  Table   │      │  Table   │       │
│  └──────────┘      └──────────┘      │   SQL    │      └──────────┘       │
│                                       └──────────┘                          │
│                                                                              │
│  ⚠️ Limitations:                                                            │
│     • Single table only                                                     │
│     • No relationship awareness                                             │
│     • No column descriptions                                                │
│     • No business glossary                                                  │
│     • Manual layer specification                                            │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘

                                    ▼

┌─────────────────────────────────────────────────────────────────────────────┐
│                          TARGET STATE (Phase 2+)                             │
│                          "Enterprise Platform"                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────┐      ┌──────────────────────────────────────────────────┐    │
│  │  User    │      │              INTELLIGENT AGENT                    │    │
│  │  Prompt  │ ───▶ │  ┌────────────────────────────────────────────┐  │    │
│  └──────────┘      │  │           METADATA CATALOG                  │  │    │
│                    │  │  • Table Registry (descriptions, owners)   │  │    │
│                    │  │  • Column Catalog (types, descriptions)    │  │    │
│                    │  │  • Relationship Map (FK/PK, join paths)    │  │    │
│                    │  │  • Business Glossary (terms → SQL)         │  │    │
│                    │  │  • Data Quality Rules                      │  │    │
│                    │  └────────────────────────────────────────────┘  │    │
│                    │                       │                           │    │
│                    │                       ▼                           │    │
│                    │  ┌────────────────────────────────────────────┐  │    │
│                    │  │           QUERY PLANNER                     │  │    │
│                    │  │  • Identify relevant tables                │  │    │
│                    │  │  • Determine join paths                    │  │    │
│                    │  │  • Plan aggregations                       │  │    │
│                    │  │  • Optimize query                          │  │    │
│                    │  └────────────────────────────────────────────┘  │    │
│                    │                       │                           │    │
│                    │                       ▼                           │    │
│                    │  ┌────────────────────────────────────────────┐  │    │
│                    │  │           SQL GENERATOR                     │  │    │
│                    │  │  • Multi-table SQL                         │  │    │
│                    │  │  • CTEs, UNIONs, JOINs                     │  │    │
│                    │  │  • Window functions                        │  │    │
│                    │  │  • Proper aggregations                     │  │    │
│                    │  └────────────────────────────────────────────┘  │    │
│                    └──────────────────────────────────────────────────┘    │
│                                       │                                     │
│                                       ▼                                     │
│                    ┌──────────────────────────────────────────────────┐    │
│                    │              OUTPUT                               │    │
│                    │  • Validated SQL                                 │    │
│                    │  • Execution plan                                │    │
│                    │  • Lineage documentation                         │    │
│                    │  • Scheduled job (optional)                      │    │
│                    └──────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🏛️ Enterprise Architecture

### Complete System Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                    USER INTERFACE LAYER                                  │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐                      │
│   │   Gradio Chat   │   │   REST API      │   │   Slack Bot     │                      │
│   │   Interface     │   │   Endpoints     │   │   Integration   │                      │
│   └────────┬────────┘   └────────┬────────┘   └────────┬────────┘                      │
│            │                     │                     │                                │
│            └─────────────────────┼─────────────────────┘                                │
│                                  ▼                                                       │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                 ORCHESTRATION LAYER                                      │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐  │
│   │                           TRANSFORM AGENT (Orchestrator)                         │  │
│   ├─────────────────────────────────────────────────────────────────────────────────┤  │
│   │                                                                                  │  │
│   │   1. Parse user intent                                                          │  │
│   │   2. Query metadata catalog                                                     │  │
│   │   3. Identify relevant tables & relationships                                   │  │
│   │   4. Build context for LLM                                                      │  │
│   │   5. Generate SQL via LLM                                                       │  │
│   │   6. Validate SQL                                                               │  │
│   │   7. Execute or schedule                                                        │  │
│   │   8. Update lineage                                                             │  │
│   │                                                                                  │  │
│   └─────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                   │
                   ┌───────────────┼───────────────┐
                   ▼               ▼               ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                   SERVICE LAYER                                          │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  ┌───────────────────┐  ┌───────────────────┐  ┌───────────────────┐                   │
│  │  MetadataService  │  │    LLMService     │  │ DatabricksService │                   │
│  ├───────────────────┤  ├───────────────────┤  ├───────────────────┤                   │
│  │ • Table registry  │  │ • SQL generation  │  │ • Query execution │                   │
│  │ • Column catalog  │  │ • Intent parsing  │  │ • Schema reading  │                   │
│  │ • Relationships   │  │ • Schedule parse  │  │ • Job scheduling  │                   │
│  │ • Business gloss. │  │ • Validation      │  │ • Lineage write   │                   │
│  │ • Data profiling  │  │                   │  │                   │                   │
│  └─────────┬─────────┘  └─────────┬─────────┘  └─────────┬─────────┘                   │
│            │                      │                      │                              │
│            ▼                      ▼                      ▼                              │
│  ┌───────────────────┐  ┌───────────────────┐  ┌───────────────────┐                   │
│  │ ValidationService │  │  LineageService   │  │ SchedulerService  │                   │
│  ├───────────────────┤  ├───────────────────┤  ├───────────────────┤                   │
│  │ • SQL syntax      │  │ • Track sources   │  │ • Cron parsing    │                   │
│  │ • Schema compat.  │  │ • Track targets   │  │ • Job creation    │                   │
│  │ • Business rules  │  │ • Column lineage  │  │ • Monitoring      │                   │
│  │ • Data quality    │  │ • Impact analysis │  │ • Alerting        │                   │
│  └───────────────────┘  └───────────────────┘  └───────────────────┘                   │
│                                                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                   DATA LAYER                                             │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│  │                           METADATA CATALOG (Storage)                             │   │
│  ├─────────────────────────────────────────────────────────────────────────────────┤   │
│  │                                                                                  │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │   │
│  │  │   Tables     │  │   Columns    │  │ Relationships│  │   Glossary   │        │   │
│  │  │   Registry   │  │   Catalog    │  │     Map      │  │    Terms     │        │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘        │   │
│  │                                                                                  │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │   │
│  │  │    Data      │  │   Lineage    │  │    Quality   │  │   Transform  │        │   │
│  │  │   Profiles   │  │    Graph     │  │    Rules     │  │   History    │        │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘        │   │
│  │                                                                                  │   │
│  └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│  │                           DATABRICKS LAKEHOUSE                                   │   │
│  ├─────────────────────────────────────────────────────────────────────────────────┤   │
│  │                                                                                  │   │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐              │   │
│  │  │   BRONZE LAYER   │  │   SILVER LAYER   │  │    GOLD LAYER    │              │   │
│  │  │                  │  │                  │  │                  │              │   │
│  │  │  • Raw data      │  │  • Cleaned       │  │  • Aggregated    │              │   │
│  │  │  • As-is ingest  │  │  • Standardized  │  │  • Business-ready│              │   │
│  │  │  • All history   │  │  • Documented    │  │  • Denormalized  │              │   │
│  │  └──────────────────┘  └──────────────────┘  └──────────────────┘              │   │
│  │                                                                                  │   │
│  └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Enhanced Project Structure

```
data-transform-agent/
├── src/
│   ├── __init__.py
│   │
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py                    # Environment configuration
│   │
│   ├── models/                            # 🆕 Data models
│   │   ├── __init__.py
│   │   ├── metadata.py                    # Table, Column, Relationship models
│   │   ├── glossary.py                    # Business term models
│   │   ├── lineage.py                     # Lineage node/edge models
│   │   └── transform.py                   # Transform request/plan models
│   │
│   ├── catalog/                           # 🆕 Metadata Catalog
│   │   ├── __init__.py
│   │   ├── table_registry.py              # Table metadata management
│   │   ├── column_catalog.py              # Column descriptions, tags
│   │   ├── relationship_map.py            # FK/PK, join paths
│   │   ├── business_glossary.py           # Business terms → SQL
│   │   ├── data_profiler.py               # Auto-profile tables
│   │   └── catalog_sync.py                # Sync with Databricks Unity Catalog
│   │
│   ├── services/
│   │   ├── __init__.py
│   │   ├── databricks_service.py          # Enhanced with metadata reading
│   │   ├── llm_service.py                 # Enhanced with multi-table context
│   │   ├── metadata_service.py            # 🆕 Metadata operations
│   │   ├── validation_service.py          # 🆕 SQL & business rule validation
│   │   ├── lineage_service.py             # 🆕 Track data lineage
│   │   └── scheduler_service.py           # 🆕 Job scheduling
│   │
│   ├── agents/
│   │   ├── __init__.py
│   │   ├── transform_agent.py             # Enhanced orchestrator
│   │   ├── discovery_agent.py             # 🆕 Find relevant tables
│   │   └── query_planner.py               # 🆕 Plan multi-table queries
│   │
│   ├── prompts/
│   │   ├── __init__.py
│   │   ├── templates.py                   # Enhanced with metadata context
│   │   ├── multi_table_prompts.py         # 🆕 Multi-table SQL generation
│   │   ├── discovery_prompts.py           # 🆕 Table discovery prompts
│   │   └── glossary_prompts.py            # 🆕 Business term resolution
│   │
│   └── tools/
│       ├── __init__.py
│       ├── schema_tools.py
│       ├── validation_tools.py
│       ├── sql_parser.py                  # 🆕 Parse and analyze SQL
│       └── join_detector.py               # 🆕 Auto-detect join paths
│
├── catalog_data/                          # 🆕 Metadata storage (JSON/YAML)
│   ├── tables/                            # Table definitions
│   ├── relationships/                     # FK/PK mappings
│   ├── glossary/                          # Business terms
│   └── profiles/                          # Data profiles
│
├── scripts/
│   ├── init_sample_data.py
│   ├── test_connections.py
│   ├── init_catalog.py                    # 🆕 Initialize metadata catalog
│   ├── sync_catalog.py                    # 🆕 Sync from Databricks
│   └── profile_tables.py                  # 🆕 Auto-profile all tables
│
├── tests/
│   ├── test_metadata_service.py           # 🆕
│   ├── test_multi_table_queries.py        # 🆕
│   ├── test_relationship_detection.py    # 🆕
│   └── ...
│
├── app.py                                 # Gradio application
├── api.py                                 # 🆕 REST API (FastAPI)
└── requirements.txt
```

---

## 📋 Metadata Catalog Design

### 1. Table Registry

```python
# src/models/metadata.py

from dataclasses import dataclass, field
from typing import Optional, List, Dict
from enum import Enum
from datetime import datetime


class TableLayer(Enum):
    """Data layer classification."""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class TableType(Enum):
    """Type of table."""
    FACT = "fact"           # Transactional/event data
    DIMENSION = "dimension"  # Reference/lookup data
    AGGREGATE = "aggregate"  # Pre-aggregated data
    SNAPSHOT = "snapshot"    # Point-in-time snapshots
    RAW = "raw"             # Unprocessed data


@dataclass
class TableMetadata:
    """Complete metadata for a table."""
    
    # Identity
    catalog: str
    schema: str
    table: str
    
    # Classification
    layer: TableLayer
    table_type: TableType
    domain: str                          # e.g., "Sales", "Customer", "Product"
    
    # Documentation
    description: str
    business_owner: Optional[str] = None
    technical_owner: Optional[str] = None
    
    # Schema info
    grain: Optional[str] = None          # e.g., "One row per transaction"
    primary_key: Optional[List[str]] = None
    
    # Operational
    refresh_frequency: Optional[str] = None  # e.g., "daily", "hourly"
    last_updated: Optional[datetime] = None
    row_count: Optional[int] = None
    
    # Quality
    quality_score: Optional[float] = None    # 0-100
    sla_hours: Optional[int] = None          # Expected freshness
    
    # Tags for discovery
    tags: List[str] = field(default_factory=list)
    
    @property
    def full_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"
    
    def matches_search(self, query: str) -> bool:
        """Check if table matches a search query."""
        query_lower = query.lower()
        searchable = [
            self.table,
            self.description,
            self.domain,
            *self.tags
        ]
        return any(query_lower in s.lower() for s in searchable if s)
```

### 2. Column Catalog

```python
# src/models/metadata.py (continued)

class ColumnType(Enum):
    """Semantic type of column."""
    PRIMARY_KEY = "primary_key"
    FOREIGN_KEY = "foreign_key"
    MEASURE = "measure"           # Numeric values to aggregate
    DIMENSION = "dimension"       # Categorical values to group by
    TIMESTAMP = "timestamp"       # Date/time columns
    IDENTIFIER = "identifier"     # Business identifiers
    DERIVED = "derived"           # Calculated columns
    ATTRIBUTE = "attribute"       # Descriptive attributes


class DataClassification(Enum):
    """Data sensitivity classification."""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    PII = "pii"
    PHI = "phi"                   # Protected Health Information


@dataclass
class ColumnMetadata:
    """Complete metadata for a column."""
    
    # Identity
    table_full_name: str
    column_name: str
    
    # Technical
    data_type: str
    nullable: bool = True
    
    # Semantic
    column_type: Optional[ColumnType] = None
    classification: DataClassification = DataClassification.INTERNAL
    
    # Documentation
    description: Optional[str] = None
    business_name: Optional[str] = None   # Friendly name for business users
    
    # Relationships
    foreign_key_to: Optional[str] = None  # e.g., "silver.dim_regions.state_code"
    
    # Value mapping (for coded values)
    value_mapping: Optional[Dict[str, str]] = None  # e.g., {"A": "TV", "B": "Mobile"}
    
    # Statistics (from profiling)
    distinct_count: Optional[int] = None
    null_percentage: Optional[float] = None
    sample_values: Optional[List[str]] = None
    
    # Tags
    tags: List[str] = field(default_factory=list)
```

### 3. Relationship Map

```python
# src/models/metadata.py (continued)

class JoinType(Enum):
    """Type of join relationship."""
    ONE_TO_ONE = "1:1"
    ONE_TO_MANY = "1:N"
    MANY_TO_ONE = "N:1"
    MANY_TO_MANY = "N:M"


@dataclass
class Relationship:
    """Defines a relationship between two tables."""
    
    # Source (FK side)
    source_table: str            # Full table name
    source_column: str
    
    # Target (PK side)
    target_table: str            # Full table name
    target_column: str
    
    # Relationship properties
    join_type: JoinType
    relationship_name: Optional[str] = None  # e.g., "customer_orders"
    
    # Documentation
    description: Optional[str] = None
    
    # Validation
    enforced: bool = False       # Is this a real FK constraint?
    validated: bool = False      # Has the relationship been validated?
    
    def get_join_clause(self, source_alias: str = "s", target_alias: str = "t") -> str:
        """Generate SQL join clause."""
        return f"{source_alias}.{self.source_column} = {target_alias}.{self.target_column}"


@dataclass
class JoinPath:
    """A path of joins to connect two tables."""
    
    start_table: str
    end_table: str
    relationships: List[Relationship]
    
    @property
    def hop_count(self) -> int:
        return len(self.relationships)
    
    def to_sql_joins(self) -> str:
        """Generate SQL JOIN clauses for this path."""
        joins = []
        for i, rel in enumerate(self.relationships):
            alias = f"t{i+1}"
            join = f"JOIN {rel.target_table} {alias} ON {rel.get_join_clause(f't{i}', alias)}"
            joins.append(join)
        return "\n".join(joins)
```

### 4. Business Glossary

```python
# src/models/glossary.py

from dataclasses import dataclass, field
from typing import Optional, List
from enum import Enum


class TermType(Enum):
    """Type of business term."""
    METRIC = "metric"           # Calculated values (e.g., "total sales")
    DIMENSION = "dimension"     # Grouping concepts (e.g., "region")
    FILTER = "filter"           # Filter conditions (e.g., "active customers")
    ENTITY = "entity"           # Business entities (e.g., "customer")
    TIME_PERIOD = "time_period" # Time concepts (e.g., "last quarter")


@dataclass
class BusinessTerm:
    """A business term with its technical mapping."""
    
    # Identity
    term: str                    # e.g., "total sales"
    term_type: TermType
    
    # Technical mapping
    sql_expression: str          # e.g., "SUM(total_amount)"
    source_tables: List[str]     # Tables needed for this term
    source_columns: List[str]    # Columns used
    
    # Documentation
    definition: str              # Business definition
    example: Optional[str] = None
    
    # Aliases (other ways to refer to this)
    aliases: List[str] = field(default_factory=list)  # e.g., ["revenue", "sales amount"]
    
    # Context
    domain: Optional[str] = None  # e.g., "Sales", "Finance"
    
    def matches(self, query: str) -> bool:
        """Check if query matches this term or its aliases."""
        query_lower = query.lower()
        all_terms = [self.term] + self.aliases
        return any(t.lower() in query_lower for t in all_terms)


# Example glossary entries
SAMPLE_GLOSSARY = [
    BusinessTerm(
        term="total sales",
        term_type=TermType.METRIC,
        sql_expression="SUM(total_amount)",
        source_tables=["silver.sales_*"],
        source_columns=["total_amount"],
        definition="The sum of all transaction amounts",
        aliases=["revenue", "sales amount", "total revenue"]
    ),
    BusinessTerm(
        term="units sold",
        term_type=TermType.METRIC,
        sql_expression="SUM(units_sold)",
        source_tables=["silver.sales_*"],
        source_columns=["units_sold"],
        definition="Total number of units sold",
        aliases=["quantity sold", "items sold"]
    ),
    BusinessTerm(
        term="region",
        term_type=TermType.DIMENSION,
        sql_expression="dim_regions.region_name",
        source_tables=["silver.dim_regions"],
        source_columns=["region_name"],
        definition="Geographic region name",
        aliases=["area", "territory"]
    ),
    BusinessTerm(
        term="active customers",
        term_type=TermType.FILTER,
        sql_expression="is_active = true",
        source_tables=["silver.customers"],
        source_columns=["is_active"],
        definition="Customers with active status",
        aliases=["current customers"]
    ),
]
```

### 5. Data Profile

```python
# src/models/metadata.py (continued)

@dataclass
class ColumnProfile:
    """Statistical profile of a column."""
    
    column_name: str
    data_type: str
    
    # Completeness
    total_count: int
    null_count: int
    null_percentage: float
    
    # Uniqueness
    distinct_count: int
    uniqueness_ratio: float      # distinct_count / total_count
    is_unique: bool              # 100% unique?
    
    # For numeric columns
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    mean_value: Optional[float] = None
    median_value: Optional[float] = None
    std_dev: Optional[float] = None
    
    # For string columns
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None
    
    # For all columns
    top_values: Optional[List[tuple]] = None  # [(value, count), ...]
    sample_values: Optional[List[str]] = None
    
    # Quality indicators
    has_nulls: bool = False
    has_duplicates: bool = False
    potential_pk: bool = False   # Could this be a primary key?
    potential_fk: bool = False   # Could this be a foreign key?


@dataclass
class TableProfile:
    """Statistical profile of an entire table."""
    
    table_full_name: str
    profiled_at: datetime
    
    # Basic stats
    row_count: int
    column_count: int
    
    # Column profiles
    columns: List[ColumnProfile]
    
    # Table-level insights
    potential_primary_keys: List[str]
    potential_foreign_keys: List[str]
    
    # Quality score
    completeness_score: float    # % of non-null values
    uniqueness_issues: List[str] # Columns with unexpected duplicates
    
    def get_join_candidates(self) -> List[str]:
        """Get columns that might be used for joins."""
        candidates = []
        for col in self.columns:
            if col.potential_fk or col.potential_pk:
                candidates.append(col.column_name)
            # Also check for common naming patterns
            if any(pattern in col.column_name.lower() 
                   for pattern in ['_id', '_code', '_key', '_fk', '_pk']):
                if col.column_name not in candidates:
                    candidates.append(col.column_name)
        return candidates
```

---

## 🔧 Core Services Implementation

### 1. Metadata Service

```python
# src/services/metadata_service.py

"""
Metadata Service

Central service for all metadata operations:
- Table/column discovery
- Relationship management
- Business glossary
- Data profiling
"""

from typing import List, Optional, Dict
from pathlib import Path
import json
import yaml

from src.models.metadata import (
    TableMetadata, ColumnMetadata, Relationship, 
    JoinPath, TableLayer, ColumnType
)
from src.models.glossary import BusinessTerm
from src.services.databricks_service import DatabricksService


class MetadataService:
    """
    Service for managing the metadata catalog.
    
    The catalog can be:
    1. Stored locally (JSON/YAML files) - for development
    2. Stored in Databricks tables - for production
    3. Synced from Unity Catalog - for enterprise
    """
    
    def __init__(
        self, 
        catalog_path: str = "catalog_data",
        databricks_service: Optional[DatabricksService] = None
    ):
        self.catalog_path = Path(catalog_path)
        self.db = databricks_service
        
        # In-memory caches
        self._tables: Dict[str, TableMetadata] = {}
        self._columns: Dict[str, List[ColumnMetadata]] = {}
        self._relationships: List[Relationship] = []
        self._glossary: List[BusinessTerm] = []
        
        # Load catalog on init
        self._load_catalog()
    
    # ─────────────────────────────────────────────────────────────────
    # TABLE OPERATIONS
    # ─────────────────────────────────────────────────────────────────
    
    def get_table(self, full_name: str) -> Optional[TableMetadata]:
        """Get metadata for a specific table."""
        return self._tables.get(full_name)
    
    def find_tables(
        self,
        query: Optional[str] = None,
        layer: Optional[TableLayer] = None,
        domain: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> List[TableMetadata]:
        """
        Find tables matching criteria.
        
        Args:
            query: Text search across name, description, tags
            layer: Filter by data layer
            domain: Filter by business domain
            tags: Filter by tags (any match)
        """
        results = list(self._tables.values())
        
        if query:
            results = [t for t in results if t.matches_search(query)]
        
        if layer:
            results = [t for t in results if t.layer == layer]
        
        if domain:
            results = [t for t in results if t.domain.lower() == domain.lower()]
        
        if tags:
            results = [t for t in results 
                      if any(tag in t.tags for tag in tags)]
        
        return results
    
    def get_tables_in_schema(self, schema: str) -> List[TableMetadata]:
        """Get all tables in a schema."""
        return [t for t in self._tables.values() if t.schema == schema]
    
    def register_table(self, metadata: TableMetadata) -> None:
        """Register or update table metadata."""
        self._tables[metadata.full_name] = metadata
        self._save_table(metadata)
    
    # ─────────────────────────────────────────────────────────────────
    # COLUMN OPERATIONS
    # ─────────────────────────────────────────────────────────────────
    
    def get_columns(self, table_full_name: str) -> List[ColumnMetadata]:
        """Get all column metadata for a table."""
        return self._columns.get(table_full_name, [])
    
    def get_column(self, table_full_name: str, column_name: str) -> Optional[ColumnMetadata]:
        """Get metadata for a specific column."""
        columns = self.get_columns(table_full_name)
        for col in columns:
            if col.column_name == column_name:
                return col
        return None
    
    def find_columns_by_type(
        self, 
        column_type: ColumnType,
        schema: Optional[str] = None
    ) -> List[ColumnMetadata]:
        """Find all columns of a specific type."""
        results = []
        for table_name, columns in self._columns.items():
            if schema and not table_name.split('.')[1] == schema:
                continue
            for col in columns:
                if col.column_type == column_type:
                    results.append(col)
        return results
    
    def find_foreign_keys(self, table_full_name: str) -> List[ColumnMetadata]:
        """Find all foreign key columns in a table."""
        columns = self.get_columns(table_full_name)
        return [c for c in columns if c.column_type == ColumnType.FOREIGN_KEY]
    
    # ─────────────────────────────────────────────────────────────────
    # RELATIONSHIP OPERATIONS
    # ─────────────────────────────────────────────────────────────────
    
    def get_relationships_for_table(self, table_full_name: str) -> List[Relationship]:
        """Get all relationships involving a table."""
        return [r for r in self._relationships 
                if r.source_table == table_full_name or r.target_table == table_full_name]
    
    def find_join_path(
        self, 
        from_table: str, 
        to_table: str,
        max_hops: int = 3
    ) -> Optional[JoinPath]:
        """
        Find the shortest join path between two tables.
        
        Uses BFS to find the shortest path through the relationship graph.
        """
        if from_table == to_table:
            return JoinPath(from_table, to_table, [])
        
        # BFS to find shortest path
        visited = {from_table}
        queue = [(from_table, [])]
        
        while queue:
            current_table, path = queue.pop(0)
            
            if len(path) >= max_hops:
                continue
            
            # Find all relationships from current table
            for rel in self._relationships:
                next_table = None
                if rel.source_table == current_table:
                    next_table = rel.target_table
                elif rel.target_table == current_table:
                    # Reverse the relationship
                    next_table = rel.source_table
                
                if next_table and next_table not in visited:
                    new_path = path + [rel]
                    
                    if next_table == to_table:
                        return JoinPath(from_table, to_table, new_path)
                    
                    visited.add(next_table)
                    queue.append((next_table, new_path))
        
        return None  # No path found
    
    def get_related_tables(
        self, 
        table_full_name: str, 
        max_hops: int = 2
    ) -> List[tuple]:
        """
        Get all tables related to a given table within N hops.
        
        Returns: List of (table_name, hop_count, join_path)
        """
        results = []
        visited = {table_full_name}
        queue = [(table_full_name, 0, [])]
        
        while queue:
            current, hops, path = queue.pop(0)
            
            if hops >= max_hops:
                continue
            
            for rel in self._relationships:
                next_table = None
                if rel.source_table == current:
                    next_table = rel.target_table
                elif rel.target_table == current:
                    next_table = rel.source_table
                
                if next_table and next_table not in visited:
                    visited.add(next_table)
                    new_path = path + [rel]
                    results.append((next_table, hops + 1, new_path))
                    queue.append((next_table, hops + 1, new_path))
        
        return results
    
    # ─────────────────────────────────────────────────────────────────
    # BUSINESS GLOSSARY OPERATIONS
    # ─────────────────────────────────────────────────────────────────
    
    def resolve_business_term(self, term: str) -> Optional[BusinessTerm]:
        """Find a business term by name or alias."""
        for bt in self._glossary:
            if bt.matches(term):
                return bt
        return None
    
    def extract_terms_from_query(self, user_query: str) -> List[BusinessTerm]:
        """Extract all business terms mentioned in a query."""
        found = []
        for bt in self._glossary:
            if bt.matches(user_query):
                found.append(bt)
        return found
    
    # ─────────────────────────────────────────────────────────────────
    # SYNC WITH DATABRICKS
    # ─────────────────────────────────────────────────────────────────
    
    def sync_from_databricks(self, schemas: List[str] = None) -> Dict[str, int]:
        """
        Sync metadata from Databricks Unity Catalog.
        
        Reads table/column info and auto-detects relationships.
        """
        if not self.db:
            raise ValueError("DatabricksService required for sync")
        
        stats = {"tables": 0, "columns": 0, "relationships": 0}
        
        schemas = schemas or ["bronze", "silver", "gold"]
        
        for schema in schemas:
            tables = self.db.list_tables(schema)
            
            for table_name in tables:
                # Get table schema from Databricks
                table_schema = self.db.get_table_schema(table_name, schema)
                
                # Create/update table metadata
                table_meta = TableMetadata(
                    catalog=self.db.settings.catalog,
                    schema=schema,
                    table=table_name,
                    layer=TableLayer(schema) if schema in ['bronze', 'silver', 'gold'] else TableLayer.SILVER,
                    table_type=self._infer_table_type(table_name),
                    domain=self._infer_domain(table_name),
                    description=f"Auto-discovered table: {table_name}",
                    row_count=table_schema.row_count
                )
                self.register_table(table_meta)
                stats["tables"] += 1
                
                # Create column metadata
                columns = []
                for col in table_schema.columns:
                    col_meta = ColumnMetadata(
                        table_full_name=table_meta.full_name,
                        column_name=col.name,
                        data_type=col.data_type,
                        column_type=self._infer_column_type(col.name, col.data_type),
                        description=col.comment
                    )
                    columns.append(col_meta)
                    stats["columns"] += 1
                
                self._columns[table_meta.full_name] = columns
        
        # Auto-detect relationships
        new_rels = self._auto_detect_relationships()
        stats["relationships"] = len(new_rels)
        
        self._save_catalog()
        return stats
    
    def _auto_detect_relationships(self) -> List[Relationship]:
        """
        Auto-detect potential FK relationships based on naming patterns.
        
        Looks for columns with matching names like:
        - sales.state_code → dim_regions.state_code
        - orders.customer_id → customers.customer_id
        """
        detected = []
        
        # Build lookup of potential PK columns
        pk_candidates = {}  # column_name -> [(table, column_meta), ...]
        
        for table_name, columns in self._columns.items():
            for col in columns:
                # Check if this looks like a PK
                if col.column_type == ColumnType.PRIMARY_KEY or \
                   col.column_name.endswith('_id') or \
                   col.column_name.endswith('_code') or \
                   col.column_name.endswith('_key'):
                    if col.column_name not in pk_candidates:
                        pk_candidates[col.column_name] = []
                    pk_candidates[col.column_name].append((table_name, col))
        
        # Find matching columns in different tables
        for column_name, tables in pk_candidates.items():
            if len(tables) > 1:
                # Multiple tables have this column - potential relationship
                # Prefer dimension tables as the target
                dim_tables = [(t, c) for t, c in tables if 'dim' in t.lower()]
                fact_tables = [(t, c) for t, c in tables if 'dim' not in t.lower()]
                
                if dim_tables and fact_tables:
                    target_table, target_col = dim_tables[0]
                    for source_table, source_col in fact_tables:
                        rel = Relationship(
                            source_table=source_table,
                            source_column=column_name,
                            target_table=target_table,
                            target_column=column_name,
                            join_type=JoinType.MANY_TO_ONE,
                            description=f"Auto-detected: {column_name}"
                        )
                        detected.append(rel)
                        self._relationships.append(rel)
        
        return detected
    
    def _infer_table_type(self, table_name: str) -> TableType:
        """Infer table type from naming conventions."""
        name_lower = table_name.lower()
        if name_lower.startswith('dim_') or name_lower.startswith('d_'):
            return TableType.DIMENSION
        elif name_lower.startswith('fact_') or name_lower.startswith('f_'):
            return TableType.FACT
        elif name_lower.startswith('agg_') or name_lower.startswith('summary_'):
            return TableType.AGGREGATE
        elif name_lower.startswith('raw_'):
            return TableType.RAW
        else:
            return TableType.FACT  # Default assumption
    
    def _infer_domain(self, table_name: str) -> str:
        """Infer business domain from table name."""
        name_lower = table_name.lower()
        
        domain_patterns = {
            'sales': ['sales', 'order', 'transaction', 'revenue'],
            'customer': ['customer', 'client', 'user', 'account'],
            'product': ['product', 'item', 'sku', 'inventory'],
            'finance': ['finance', 'payment', 'invoice', 'billing'],
            'marketing': ['campaign', 'marketing', 'lead', 'promotion'],
            'reference': ['dim_', 'lookup', 'reference', 'region', 'state'],
        }
        
        for domain, patterns in domain_patterns.items():
            if any(p in name_lower for p in patterns):
                return domain.capitalize()
        
        return "General"
    
    def _infer_column_type(self, column_name: str, data_type: str) -> ColumnType:
        """Infer semantic column type from name and data type."""
        name_lower = column_name.lower()
        
        # Check for keys
        if name_lower.endswith('_id') or name_lower.endswith('_pk'):
            return ColumnType.PRIMARY_KEY
        if name_lower.endswith('_fk'):
            return ColumnType.FOREIGN_KEY
        if name_lower.endswith('_code') or name_lower.endswith('_key'):
            return ColumnType.IDENTIFIER
        
        # Check for timestamps
        if 'timestamp' in data_type.lower() or 'date' in data_type.lower():
            return ColumnType.TIMESTAMP
        if any(t in name_lower for t in ['_at', '_date', '_time', 'created', 'updated']):
            return ColumnType.TIMESTAMP
        
        # Check for measures (numeric columns that are likely aggregatable)
        if any(t in data_type.lower() for t in ['int', 'decimal', 'double', 'float']):
            if any(m in name_lower for m in ['amount', 'price', 'quantity', 'count', 
                                              'total', 'sum', 'units', 'value']):
                return ColumnType.MEASURE
        
        return ColumnType.ATTRIBUTE
    
    # ─────────────────────────────────────────────────────────────────
    # PERSISTENCE
    # ─────────────────────────────────────────────────────────────────
    
    def _load_catalog(self) -> None:
        """Load catalog from disk."""
        # Implementation depends on storage format (JSON, YAML, or DB)
        pass
    
    def _save_catalog(self) -> None:
        """Save catalog to disk."""
        # Implementation depends on storage format
        pass
    
    def _save_table(self, metadata: TableMetadata) -> None:
        """Save a single table's metadata."""
        pass
```

### 2. Enhanced LLM Service (Multi-Table Support)

```python
# src/services/llm_service.py (enhanced)

"""
Enhanced LLM Service with Multi-Table Support

Handles:
- Single table transformations
- Multi-table joins
- Business term resolution
- Complex aggregations
"""

from typing import List, Optional
from dataclasses import dataclass

from src.models.metadata import TableMetadata, ColumnMetadata, JoinPath
from src.models.glossary import BusinessTerm
from src.prompts.multi_table_prompts import (
    build_multi_table_prompt,
    build_discovery_prompt
)


@dataclass
class QueryContext:
    """
    Complete context for SQL generation.
    
    Contains all the information the LLM needs to generate correct SQL.
    """
    
    # User's request
    user_request: str
    
    # Tables involved
    primary_table: TableMetadata
    related_tables: List[TableMetadata]
    
    # Column details
    all_columns: dict[str, List[ColumnMetadata]]  # table_name -> columns
    
    # Relationships
    join_paths: List[JoinPath]
    
    # Business context
    resolved_terms: List[BusinessTerm]
    
    # Sample data
    sample_data: dict[str, list]  # table_name -> sample rows
    
    # Target
    target_schema: str
    target_catalog: str


class EnhancedLLMService(LLMService):
    """
    Extended LLM service with multi-table and metadata awareness.
    """
    
    def generate_multi_table_sql(self, context: QueryContext) -> TransformPlan:
        """
        Generate SQL that may involve multiple tables.
        
        Args:
            context: Complete query context with all metadata
            
        Returns:
            TransformPlan with multi-table SQL
        """
        # Build the enhanced prompt
        prompt = build_multi_table_prompt(context)
        
        # Call Claude
        response = self._call_claude(prompt)
        
        # Parse response
        data = self._parse_json_response(response)
        
        return TransformPlan(
            target_table=data["target_table"],
            sql=data["sql"],
            transformations_applied=data.get("transformations_applied", []),
            explanation=data.get("explanation", ""),
            source_tables=data.get("source_tables", []),
            join_summary=data.get("join_summary", "")
        )
    
    def discover_relevant_tables(
        self,
        user_request: str,
        available_tables: List[TableMetadata],
        glossary_terms: List[BusinessTerm]
    ) -> List[str]:
        """
        Use LLM to identify which tables are relevant for a request.
        
        Args:
            user_request: User's natural language request
            available_tables: All tables in the catalog
            glossary_terms: Business terms that might be mentioned
            
        Returns:
            List of table names that should be used
        """
        prompt = build_discovery_prompt(
            user_request=user_request,
            tables=available_tables,
            terms=glossary_terms
        )
        
        response = self._call_claude(prompt, max_tokens=512)
        data = self._parse_json_response(response)
        
        return data.get("relevant_tables", [])
```

### 3. Query Planner

```python
# src/agents/query_planner.py

"""
Query Planner

Orchestrates the process of:
1. Understanding user intent
2. Finding relevant tables
3. Planning join paths
4. Preparing context for SQL generation
"""

from typing import List, Optional, Tuple
from dataclasses import dataclass

from src.services.metadata_service import MetadataService
from src.services.llm_service import EnhancedLLMService, QueryContext
from src.services.databricks_service import DatabricksService
from src.models.metadata import TableMetadata, TableLayer
from src.models.glossary import BusinessTerm


@dataclass
class QueryPlan:
    """A planned query ready for execution."""
    
    # Source analysis
    primary_table: TableMetadata
    supporting_tables: List[TableMetadata]
    join_strategy: str  # Description of how tables connect
    
    # Transformations
    required_joins: List[str]  # SQL join clauses
    required_aggregations: List[str]
    filters: List[str]
    
    # Target
    target_table_name: str
    target_schema: str
    
    # Generated SQL
    sql: str
    explanation: str


class QueryPlanner:
    """
    Plans complex multi-table queries.
    
    Workflow:
    1. Parse user intent
    2. Resolve business terms
    3. Discover relevant tables
    4. Find join paths
    5. Build query context
    6. Generate SQL
    """
    
    def __init__(
        self,
        metadata_service: MetadataService,
        llm_service: EnhancedLLMService,
        databricks_service: DatabricksService
    ):
        self.metadata = metadata_service
        self.llm = llm_service
        self.db = databricks_service
    
    def plan_query(self, user_request: str) -> QueryPlan:
        """
        Create a complete query plan from a natural language request.
        
        Args:
            user_request: Natural language description of desired output
            
        Returns:
            QueryPlan with SQL and explanation
        """
        # Step 1: Extract business terms
        terms = self.metadata.extract_terms_from_query(user_request)
        
        # Step 2: Determine target layer
        target_schema = self._determine_target_layer(user_request)
        
        # Step 3: Find relevant tables
        source_schema = self._determine_source_layer(target_schema)
        available_tables = self.metadata.find_tables(layer=source_schema)
        
        relevant_table_names = self.llm.discover_relevant_tables(
            user_request=user_request,
            available_tables=available_tables,
            glossary_terms=terms
        )
        
        # Step 4: Get table metadata
        tables = [self.metadata.get_table(name) for name in relevant_table_names]
        tables = [t for t in tables if t is not None]
        
        if not tables:
            raise ValueError("No relevant tables found for this request")
        
        # Step 5: Identify primary table
        primary_table = self._identify_primary_table(tables, user_request)
        supporting_tables = [t for t in tables if t != primary_table]
        
        # Step 6: Find join paths
        join_paths = []
        for table in supporting_tables:
            path = self.metadata.find_join_path(
                from_table=primary_table.full_name,
                to_table=table.full_name
            )
            if path:
                join_paths.append(path)
        
        # Step 7: Get column metadata
        all_columns = {}
        for table in tables:
            all_columns[table.full_name] = self.metadata.get_columns(table.full_name)
        
        # Step 8: Get sample data
        sample_data = {}
        for table in tables:
            result = self.db.get_sample_data(table.table, table.schema, limit=3)
            sample_data[table.full_name] = result.to_dict_list()
        
        # Step 9: Build context
        context = QueryContext(
            user_request=user_request,
            primary_table=primary_table,
            related_tables=supporting_tables,
            all_columns=all_columns,
            join_paths=join_paths,
            resolved_terms=terms,
            sample_data=sample_data,
            target_schema=target_schema,
            target_catalog=self.db.settings.catalog
        )
        
        # Step 10: Generate SQL
        plan = self.llm.generate_multi_table_sql(context)
        
        return QueryPlan(
            primary_table=primary_table,
            supporting_tables=supporting_tables,
            join_strategy=self._describe_join_strategy(join_paths),
            required_joins=[p.to_sql_joins() for p in join_paths],
            required_aggregations=self._extract_aggregations(plan.sql),
            filters=self._extract_filters(plan.sql),
            target_table_name=plan.target_table,
            target_schema=target_schema,
            sql=plan.sql,
            explanation=plan.explanation
        )
    
    def _determine_target_layer(self, user_request: str) -> str:
        """Determine target layer from request context."""
        request_lower = user_request.lower()
        
        # Gold layer indicators
        if any(word in request_lower for word in [
            'aggregate', 'summary', 'report', 'analytics', 'dashboard',
            'metrics', 'kpi', 'total', 'by region', 'by product', 'group by'
        ]):
            return 'gold'
        
        # Silver layer indicators
        if any(word in request_lower for word in [
            'clean', 'standardize', 'dedupe', 'normalize', 'transform'
        ]):
            return 'silver'
        
        # Default to silver
        return 'silver'
    
    def _determine_source_layer(self, target_layer: str) -> TableLayer:
        """Determine source layer based on target."""
        if target_layer == 'gold':
            return TableLayer.SILVER
        elif target_layer == 'silver':
            return TableLayer.BRONZE
        else:
            return TableLayer.BRONZE
    
    def _identify_primary_table(
        self, 
        tables: List[TableMetadata], 
        user_request: str
    ) -> TableMetadata:
        """Identify the primary/fact table from a list."""
        # Prefer fact tables
        fact_tables = [t for t in tables if t.table_type.value == 'fact']
        if len(fact_tables) == 1:
            return fact_tables[0]
        
        # Otherwise, prefer tables mentioned first or with most rows
        return max(tables, key=lambda t: t.row_count or 0)
    
    def _describe_join_strategy(self, join_paths) -> str:
        """Create human-readable description of join strategy."""
        if not join_paths:
            return "Single table query, no joins required"
        
        descriptions = []
        for path in join_paths:
            desc = f"{path.start_table} → {path.end_table} ({path.hop_count} hop(s))"
            descriptions.append(desc)
        
        return "; ".join(descriptions)
    
    def _extract_aggregations(self, sql: str) -> List[str]:
        """Extract aggregation functions from SQL."""
        import re
        pattern = r'(SUM|COUNT|AVG|MIN|MAX|STDDEV)\s*\([^)]+\)'
        matches = re.findall(pattern, sql, re.IGNORECASE)
        return list(set(matches))
    
    def _extract_filters(self, sql: str) -> List[str]:
        """Extract WHERE conditions from SQL."""
        import re
        pattern = r'WHERE\s+(.+?)(?:GROUP BY|ORDER BY|LIMIT|$)'
        match = re.search(pattern, sql, re.IGNORECASE | re.DOTALL)
        if match:
            return [match.group(1).strip()]
        return []
```

---

## 📝 Enhanced Prompt Templates

```python
# src/prompts/multi_table_prompts.py

"""
Prompt templates for multi-table SQL generation.
"""

MULTI_TABLE_SYSTEM_PROMPT = """You are an expert Databricks SQL developer with deep knowledge of:
- Data warehouse design (star schema, snowflake schema)
- Medallion architecture (bronze → silver → gold)
- Complex SQL including CTEs, window functions, and aggregations
- Join optimization and query planning

You understand business terminology and can translate it to technical SQL."""


MULTI_TABLE_TRANSFORM_PROMPT = """## Task
Generate Databricks SQL to fulfill the user's request using the available tables.

## User Request
{user_request}

## Available Tables

{tables_context}

## Relationships Between Tables

{relationships_context}

## Business Term Mappings

{glossary_context}

## Sample Data

{sample_data_context}

## Target
- Catalog: {target_catalog}
- Schema: {target_schema}
- Generate a descriptive table name based on the output

## Requirements

1. Use CREATE OR REPLACE TABLE {target_catalog}.{target_schema}.<table_name> AS
2. Use CTEs (WITH clauses) for complex logic - make it readable
3. Include all necessary JOINs based on the relationships
4. Apply appropriate aggregations based on the request
5. Handle NULL values appropriately
6. Add SQL comments explaining the logic
7. Use table aliases for clarity

## Response Format

Return ONLY a JSON object:
{{
    "target_table": "{target_catalog}.{target_schema}.table_name",
    "sql": "WITH ... CREATE OR REPLACE TABLE ...",
    "source_tables": ["list", "of", "source", "tables"],
    "join_summary": "Brief description of how tables are joined",
    "transformations_applied": ["list", "of", "transformations"],
    "explanation": "What this query produces",
    "columns_output": ["list", "of", "output", "columns"]
}}

Return ONLY the JSON, no markdown or extra text."""


TABLE_CONTEXT_TEMPLATE = """### {table_name}
- Type: {table_type}
- Domain: {domain}
- Description: {description}
- Row Count: {row_count:,}

Columns:
{columns}
"""


COLUMN_CONTEXT_TEMPLATE = """  - {name} ({data_type}): {description}{tags}"""


def build_multi_table_prompt(context) -> str:
    """Build the complete multi-table prompt."""
    
    # Build tables context
    tables_parts = []
    all_tables = [context.primary_table] + context.related_tables
    
    for table in all_tables:
        columns = context.all_columns.get(table.full_name, [])
        columns_str = "\n".join(
            COLUMN_CONTEXT_TEMPLATE.format(
                name=c.column_name,
                data_type=c.data_type,
                description=c.description or "No description",
                tags=f" [{c.column_type.value}]" if c.column_type else ""
            )
            for c in columns
        )
        
        tables_parts.append(TABLE_CONTEXT_TEMPLATE.format(
            table_name=table.full_name,
            table_type=table.table_type.value,
            domain=table.domain,
            description=table.description,
            row_count=table.row_count or 0,
            columns=columns_str
        ))
    
    tables_context = "\n".join(tables_parts)
    
    # Build relationships context
    if context.join_paths:
        rel_parts = []
        for path in context.join_paths:
            for rel in path.relationships:
                rel_parts.append(
                    f"- {rel.source_table}.{rel.source_column} → "
                    f"{rel.target_table}.{rel.target_column} ({rel.join_type.value})"
                )
        relationships_context = "\n".join(rel_parts)
    else:
        relationships_context = "No explicit relationships defined. Infer from column names."
    
    # Build glossary context
    if context.resolved_terms:
        glossary_parts = []
        for term in context.resolved_terms:
            glossary_parts.append(
                f"- \"{term.term}\" = {term.sql_expression} "
                f"(from: {', '.join(term.source_tables)})"
            )
        glossary_context = "\n".join(glossary_parts)
    else:
        glossary_context = "No specific business terms identified."
    
    # Build sample data context
    sample_parts = []
    for table_name, rows in context.sample_data.items():
        if rows:
            sample_parts.append(f"### {table_name}")
            # Show first row as example
            sample_parts.append(str(rows[0]))
    sample_data_context = "\n".join(sample_parts) if sample_parts else "No sample data available."
    
    return MULTI_TABLE_TRANSFORM_PROMPT.format(
        user_request=context.user_request,
        tables_context=tables_context,
        relationships_context=relationships_context,
        glossary_context=glossary_context,
        sample_data_context=sample_data_context,
        target_catalog=context.target_catalog,
        target_schema=context.target_schema
    )


DISCOVERY_PROMPT = """## Task
Identify which tables are needed to fulfill this user request.

## User Request
{user_request}

## Available Tables

{tables_list}

## Business Terms That Might Be Referenced

{terms_list}

## Instructions

1. Analyze what data the user needs
2. Identify the primary table (main source of data)
3. Identify supporting tables (for joins, lookups)
4. Consider the business terms and which tables contain that data

## Response Format

Return ONLY a JSON object:
{{
    "relevant_tables": ["full.table.name", "other.table.name"],
    "primary_table": "full.table.name",
    "reasoning": "Brief explanation of why these tables are needed"
}}

Return ONLY the JSON."""


def build_discovery_prompt(user_request: str, tables, terms) -> str:
    """Build prompt for table discovery."""
    
    tables_list = "\n".join(
        f"- {t.full_name}: {t.description} (Domain: {t.domain})"
        for t in tables
    )
    
    terms_list = "\n".join(
        f"- \"{t.term}\": {t.definition} (Tables: {', '.join(t.source_tables)})"
        for t in terms
    ) if terms else "No specific terms identified."
    
    return DISCOVERY_PROMPT.format(
        user_request=user_request,
        tables_list=tables_list,
        terms_list=terms_list
    )
```

---

## 📊 Example: End-to-End Multi-Table Query

### Scenario

User asks: *"Show me total sales by product across regions for Q1 2024"*

### Step-by-Step Execution

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 1: Parse Business Terms                                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Input: "Show me total sales by product across regions for Q1 2024"         │
│                                                                              │
│  Extracted Terms:                                                            │
│    • "total sales" → SUM(total_amount) from sales_*                         │
│    • "product" → product_name/product_code from sales_*                     │
│    • "regions" → region_name from dim_regions                               │
│    • "Q1 2024" → created_at BETWEEN '2024-01-01' AND '2024-03-31'          │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 2: Discover Relevant Tables                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  LLM analyzes terms + available tables                                      │
│                                                                              │
│  Identified Tables:                                                          │
│    • silver.sales_tv (Primary - has sales data)                             │
│    • silver.sales_mobile (Primary - has sales data)                         │
│    • silver.sales_laptop (Primary - has sales data)                         │
│    • silver.dim_regions (Supporting - has region names)                     │
│    • silver.dim_products (Supporting - has product names)                   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 3: Find Join Paths                                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Relationship Map Query:                                                     │
│                                                                              │
│  sales_tv.state_code ───────┐                                               │
│  sales_mobile.state_code ───┼──► dim_regions.state_code                     │
│  sales_laptop.state_code ───┘                                               │
│                                                                              │
│  sales_tv.product_code ─────┐                                               │
│  sales_mobile.product_code ─┼──► dim_products.product_code                  │
│  sales_laptop.product_code ─┘                                               │
│                                                                              │
│  Join Strategy:                                                              │
│    1. UNION ALL the three sales tables                                      │
│    2. JOIN to dim_regions on state_code                                     │
│    3. JOIN to dim_products on product_code                                  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 4: Build Context & Generate SQL                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Context includes:                                                           │
│    • All 5 table schemas with column descriptions                           │
│    • Sample data from each table                                            │
│    • Relationship definitions                                                │
│    • Business term mappings                                                  │
│                                                                              │
│  LLM generates:                                                              │
│                                                                              │
│  ```sql                                                                      │
│  CREATE OR REPLACE TABLE workspace.gold.regional_product_sales_q1_2024 AS   │
│                                                                              │
│  -- Combine all product sales into unified view                             │
│  WITH all_sales AS (                                                        │
│      SELECT                                                                  │
│          'TV' as product_category,                                          │
│          product_code,                                                       │
│          state_code,                                                         │
│          units_sold,                                                         │
│          total_amount,                                                       │
│          created_at                                                          │
│      FROM workspace.silver.sales_tv                                         │
│      WHERE created_at >= '2024-01-01' AND created_at < '2024-04-01'        │
│                                                                              │
│      UNION ALL                                                               │
│                                                                              │
│      SELECT                                                                  │
│          'Mobile' as product_category,                                      │
│          product_code,                                                       │
│          state_code,                                                         │
│          units_sold,                                                         │
│          total_amount,                                                       │
│          created_at                                                          │
│      FROM workspace.silver.sales_mobile                                     │
│      WHERE created_at >= '2024-01-01' AND created_at < '2024-04-01'        │
│                                                                              │
│      UNION ALL                                                               │
│                                                                              │
│      SELECT                                                                  │
│          'Laptop' as product_category,                                      │
│          product_code,                                                       │
│          state_code,                                                         │
│          units_sold,                                                         │
│          total_amount,                                                       │
│          created_at                                                          │
│      FROM workspace.silver.sales_laptop                                     │
│      WHERE created_at >= '2024-01-01' AND created_at < '2024-04-01'        │
│  ),                                                                          │
│                                                                              │
│  -- Aggregate by region and product                                         │
│  aggregated AS (                                                             │
│      SELECT                                                                  │
│          s.state_code,                                                       │
│          s.product_category,                                                │
│          s.product_code,                                                     │
│          SUM(s.units_sold) as total_units_sold,                             │
│          SUM(s.total_amount) as total_sales,                                │
│          AVG(s.total_amount / NULLIF(s.units_sold, 0)) as avg_unit_price,  │
│          COUNT(*) as transaction_count                                      │
│      FROM all_sales s                                                        │
│      GROUP BY s.state_code, s.product_category, s.product_code             │
│  )                                                                           │
│                                                                              │
│  -- Final output with dimension attributes                                  │
│  SELECT                                                                      │
│      a.state_code,                                                           │
│      r.state_name,                                                           │
│      r.region_name,                                                          │
│      a.product_code,                                                         │
│      p.product_name,                                                         │
│      a.product_category,                                                     │
│      a.total_units_sold,                                                     │
│      a.total_sales,                                                          │
│      a.avg_unit_price,                                                       │
│      a.transaction_count                                                     │
│  FROM aggregated a                                                           │
│  JOIN workspace.silver.dim_regions r ON a.state_code = r.state_code        │
│  JOIN workspace.silver.dim_products p ON a.product_code = p.product_code   │
│  ORDER BY r.region_name, a.total_sales DESC                                 │
│  ```                                                                         │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STEP 5: Validate & Execute                                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Validation Checks:                                                          │
│    ✅ SQL syntax valid                                                       │
│    ✅ All source tables exist                                                │
│    ✅ All columns exist                                                      │
│    ✅ Join columns have compatible types                                     │
│    ✅ No circular joins                                                      │
│    ✅ Aggregations are valid                                                 │
│                                                                              │
│  Execution:                                                                  │
│    → Preview (DRY RUN with LIMIT 10)                                        │
│    → User confirms                                                           │
│    → Execute CREATE TABLE                                                    │
│    → Update lineage                                                          │
│                                                                              │
│  Result:                                                                     │
│    ✅ Created gold.regional_product_sales_q1_2024                           │
│    ✅ 150 rows (50 states × 3 products, filtered)                           │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🗓️ Implementation Phases

### Phase 1: Foundation (Weeks 1-4) ✅ CURRENT
- Single table transforms
- Basic LLM SQL generation
- Gradio UI
- Manual layer specification

### Phase 2: Metadata Awareness (Weeks 5-6)
| Task | Effort | Priority |
|------|--------|----------|
| Create metadata models | 2 days | P0 |
| Implement MetadataService (basic) | 3 days | P0 |
| Sync from Databricks | 2 days | P0 |
| Add column descriptions to prompts | 1 day | P0 |
| Auto-detect relationships | 2 days | P1 |

### Phase 3: Multi-Table Support (Weeks 7-8)
| Task | Effort | Priority |
|------|--------|----------|
| Implement QueryPlanner | 3 days | P0 |
| Multi-table prompt templates | 2 days | P0 |
| Join path finder | 2 days | P0 |
| Enhanced LLM service | 2 days | P0 |
| Testing & validation | 3 days | P0 |

### Phase 4: Business Intelligence (Weeks 9-10)
| Task | Effort | Priority |
|------|--------|----------|
| Business glossary implementation | 3 days | P1 |
| Term extraction from queries | 2 days | P1 |
| Semantic layer basics | 3 days | P1 |
| Data profiling service | 2 days | P2 |

### Phase 5: Enterprise Features (Weeks 11-12)
| Task | Effort | Priority |
|------|--------|----------|
| Lineage tracking | 3 days | P1 |
| Validation service | 2 days | P1 |
| REST API (FastAPI) | 2 days | P2 |
| Job scheduling | 2 days | P2 |
| Documentation & polish | 3 days | P0 |

---

## 📈 Success Metrics

| Metric | Phase 1 | Phase 2+ Target |
|--------|---------|-----------------|
| Tables supported per query | 1 | 5+ |
| Join auto-detection | ❌ | ✅ |
| Business term resolution | ❌ | ✅ |
| Column descriptions in context | ❌ | ✅ |
| Source → Target lineage | ❌ | ✅ |
| Query complexity | Simple | Complex CTEs, window functions |
| User input required | High (specify everything) | Low (infers from context) |

---

## 🎯 Summary

This POC blueprint provides:

1. **Complete data models** for metadata catalog
2. **Service architecture** for enterprise scale
3. **Multi-table query planning** workflow
4. **Enhanced prompt templates** with full context
5. **Implementation phases** with timelines
6. **Real example** of complex query generation

The key insight: **The agent becomes smarter by having better context**, not by writing more complex code. The metadata catalog IS the intelligence layer.

---
