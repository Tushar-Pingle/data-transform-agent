#!/usr/bin/env python3
"""
Initialize sample data for the Data Transform Agent.

Creates:
- bronze, silver, gold schemas
- Sample bronze.raw_customers table with intentional data quality issues

Run: python scripts/init_sample_data.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from src.services.databricks_service import DatabricksService


# Sample data with intentional quality issues for transformation demos
SAMPLE_CUSTOMERS = """
INSERT INTO {catalog}.bronze.raw_customers VALUES
    (1, 'John', 'DOE', 'john.doe@email.com', '555-0101', '2024-01-15 10:30:00'),
    (1, 'John', 'DOE', 'john.doe@email.com', '555-0101', '2024-01-15 10:30:00'),
    (2, 'jane', 'SMITH', 'JANE.SMITH@EMAIL.COM', '555-0102', '2024-01-16 11:45:00'),
    (3, 'Bob', 'Johnson', 'bob.j@email.com', NULL, '2024-01-17 09:15:00'),
    (4, 'ALICE', 'williams', 'alice.w@email.com', '555-0104', '2024-01-18 14:20:00'),
    (5, 'Charlie', 'BROWN', 'charlie.brown@email.com', '555-0105', '2024-01-19 16:00:00'),
    (NULL, 'Invalid', 'User', 'invalid@email.com', '555-0106', '2024-01-20 08:30:00'),
    (6, 'Diana', 'Miller', 'diana.m@email.com', '555-0107', '2024-01-21 12:00:00'),
    (7, 'Edward', 'DAVIS', 'edward.davis@email.com', '', '2024-01-22 10:45:00'),
    (8, 'fiona', 'Garcia', 'FIONA.GARCIA@email.com', '555-0109', '2024-01-23 15:30:00'),
    (9, 'George', 'martinez', 'george.m@email.com', '555-0110', '2024-01-24 09:00:00'),
    (10, 'Hannah', 'ANDERSON', 'hannah.a@email.com', '555-0111', '2024-01-25 11:15:00'),
    (5, 'Charlie', 'BROWN', 'charlie.brown@email.com', '555-0105', '2024-01-19 16:00:00'),
    (11, NULL, 'Wilson', 'unknown@email.com', '555-0112', '2024-01-26 14:45:00'),
    (12, 'Jack', NULL, 'jack@email.com', '555-0113', '2024-01-27 10:00:00')
"""


def main():
    print("=" * 60)
    print(" Initializing Sample Data")
    print("=" * 60)
    
    with DatabricksService() as db:
        catalog = db.settings.catalog
        
        # Step 1: Create schemas
        print("\n📁 Creating schemas...")
        for schema in ["bronze", "silver", "gold"]:
            db.create_schema_if_not_exists(schema)
            print(f"   ✅ {catalog}.{schema}")
        
        # Step 2: Create raw_customers table
        print("\n📋 Creating bronze.raw_customers table...")
        
        create_table_sql = f"""
        CREATE OR REPLACE TABLE {catalog}.bronze.raw_customers (
            contact_id INT,
            first_name STRING,
            last_name STRING,
            email STRING,
            phone STRING,
            created_at TIMESTAMP
        )
        """
        db.execute_command(create_table_sql)
        print("   ✅ Table created")
        
        # Step 3: Insert sample data
        print("\n📝 Inserting sample data (with intentional quality issues)...")
        insert_sql = SAMPLE_CUSTOMERS.format(catalog=catalog)
        db.execute_command(insert_sql)
        print("   ✅ Sample data inserted")
        
        # Step 4: Verify
        print("\n🔍 Verifying data...")
        schema = db.get_table_schema("raw_customers", "bronze")
        print(f"\n{db.format_table_info(schema)}")
        
        # Show sample
        print("\n📊 Sample data (first 5 rows):")
        sample = db.get_sample_data("raw_customers", "bronze", limit=5)
        
        # Print header
        print("   " + " | ".join(f"{col:15}" for col in sample.columns))
        print("   " + "-" * 100)
        
        # Print rows
        for row in sample.rows:
            formatted = []
            for val in row:
                if val is None:
                    formatted.append("NULL".ljust(15))
                else:
                    formatted.append(str(val)[:15].ljust(15))
            print("   " + " | ".join(formatted))
        
        # Summary of data quality issues
        print("\n⚠️  Data Quality Issues (for demo purposes):")
        print("   - Duplicate rows (contact_id 1 and 5)")
        print("   - NULL contact_id (row 7)")
        print("   - Inconsistent name casing (UPPER, lower, Mixed)")
        print("   - Inconsistent email casing")
        print("   - NULL/empty phone numbers")
        print("   - NULL first_name and last_name")
        
        print("\n" + "=" * 60)
        print("✅ Sample data ready! You can now test transformations.")
        print("=" * 60)
        print(f"\nTry: 'Clean bronze.raw_customers and save to silver'")


if __name__ == "__main__":
    main()