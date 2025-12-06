#!/usr/bin/env python3
"""
Test script for DatabricksService.

Run: python scripts/test_databricks_service.py
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from src.services.databricks_service import DatabricksService


def main():
    print("=" * 50)
    print(" Testing DatabricksService")
    print("=" * 50)
    
    # Use context manager for automatic cleanup
    with DatabricksService() as db:
        
        # Test 1: List catalogs
        print("\n📁 Available Catalogs:")
        catalogs = db.list_catalogs()
        for cat in catalogs:
            print(f"   - {cat}")
        
        # Test 2: List schemas
        print(f"\n📂 Schemas in '{db.settings.catalog}':")
        schemas = db.list_schemas()
        for schema in schemas:
            print(f"   - {schema}")
        
        # Test 3: List tables in default schema
        print(f"\n📋 Tables in 'default' schema:")
        tables = db.list_tables("default")
        if tables:
            for table in tables:
                print(f"   - {table}")
        else:
            print("   (no tables yet)")
        
        # Test 4: Execute a simple query
        print("\n🔍 Test Query (SELECT 1+1):")
        result = db.execute_query("SELECT 1+1 as result")
        print(f"   Result: {result.rows[0][0]}")
        
        print("\n" + "=" * 50)
        print("✅ DatabricksService is working!")
        print("=" * 50)


if __name__ == "__main__":
    main()