#!/usr/bin/env python3
"""
Test script for LLMService.

Run: python scripts/test_llm_service.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from src.services.databricks_service import DatabricksService
from src.services.llm_service import LLMService


def main():
    print("=" * 60)
    print(" Testing LLMService - SQL Generation")
    print("=" * 60)
    
    # Initialize services
    llm = LLMService()
    
    with DatabricksService() as db:
        # Get schema and sample data for raw_customers
        print("\n📊 Fetching table schema and sample data...")
        
        table_schema = db.get_table_schema("raw_customers", "bronze")
        sample_data = db.get_sample_data("raw_customers", "bronze", limit=5)
        
        print(f"   Table: {table_schema.full_name}")
        print(f"   Rows: {table_schema.row_count}")
        print(f"   Columns: {len(table_schema.columns)}")
        
        # Test 1: Generate transformation SQL
        print("\n" + "=" * 60)
        print(" Test 1: Generate Transformation SQL")
        print("=" * 60)
        
        user_request = """
        Clean the raw_customers table:
        - Remove rows where contact_id is NULL
        - Remove duplicate rows (keep first occurrence based on contact_id)
        - Standardize first_name and last_name to proper case (e.g., John, Smith)
        - Standardize email to lowercase
        - Save to silver layer
        """
        
        print(f"\n📝 User Request:\n{user_request}")
        print("\n⏳ Generating SQL (calling Claude)...")
        
        plan = llm.generate_transform_sql(
            user_request=user_request,
            table_schema=table_schema,
            sample_data=sample_data,
            target_schema="silver",
            catalog=db.settings.catalog
        )
        
        print("\n✅ SQL Generated!")
        print("\n" + plan.format_for_display())
        
        # Test 2: Parse a schedule
        print("\n" + "=" * 60)
        print(" Test 2: Parse Schedule")
        print("=" * 60)
        
        schedule_text = "Run every Monday at 6am"
        print(f"\n📅 Schedule Request: '{schedule_text}'")
        print("⏳ Parsing schedule...")
        
        schedule = llm.parse_schedule(schedule_text)
        
        print(f"\n✅ Parsed Schedule:")
        print(f"   Cron: {schedule.cron_expression}")
        print(f"   Human Readable: {schedule.human_readable}")
        print(f"   Timezone: {schedule.timezone}")
        
        # Summary
        print("\n" + "=" * 60)
        print("✅ LLMService is working!")
        print("=" * 60)
        print("\nNext: Build the TransformAgent to orchestrate everything!")


if __name__ == "__main__":
    main()