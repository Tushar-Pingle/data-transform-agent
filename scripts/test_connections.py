#!/usr/bin/env python3
"""
Test script to verify all external connections are working.

Run this after setting up your .env file:
    python scripts/test_connections.py
"""

import sys
import os
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def print_header(text: str):
    """Print a formatted header."""
    print(f"\n{'='*50}")
    print(f" {text}")
    print('='*50)


def print_status(success: bool, message: str):
    """Print status with emoji."""
    emoji = "✅" if success else "❌"
    print(f"  {emoji} {message}")


def test_environment_variables() -> bool:
    """Check if all required environment variables are set."""
    print_header("1. Checking Environment Variables")
    
    required_vars = [
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN", 
        "DATABRICKS_WAREHOUSE_ID",
        "ANTHROPIC_API_KEY",
    ]
    
    all_set = True
    for var in required_vars:
        value = os.getenv(var)
        if value:
            # Mask sensitive values
            masked = value[:8] + "..." if len(value) > 8 else "***"
            print_status(True, f"{var} = {masked}")
        else:
            print_status(False, f"{var} is NOT SET")
            all_set = False
    
    return all_set


def test_databricks_connection() -> bool:
    """Test connection to Databricks SQL Warehouse."""
    print_header("2. Testing Databricks Connection")
    
    try:
        from databricks import sql as databricks_sql
        from src.config.settings import get_databricks_settings
        
        settings = get_databricks_settings()
        
        print(f"  Connecting to: {settings.host}")
        print(f"  Warehouse ID: {settings.warehouse_id}")
        
        connection = databricks_sql.connect(
            server_hostname=settings.host,
            http_path=settings.http_path,
            access_token=settings.token,
        )
        
        cursor = connection.cursor()
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        
        cursor.close()
        connection.close()
        
        if result and result[0] == 1:
            print_status(True, "Successfully executed test query")
            return True
        else:
            print_status(False, f"Unexpected result: {result}")
            return False
            
    except Exception as e:
        print_status(False, f"Connection failed: {e}")
        return False


def test_databricks_schemas() -> bool:
    """Test access to catalog and list schemas."""
    print_header("3. Testing Databricks Catalog Access")
    
    try:
        from databricks import sql as databricks_sql
        from src.config.settings import get_databricks_settings
        
        settings = get_databricks_settings()
        
        connection = databricks_sql.connect(
            server_hostname=settings.host,
            http_path=settings.http_path,
            access_token=settings.token,
        )
        
        cursor = connection.cursor()
        
        # Try to list schemas
        cursor.execute(f"SHOW SCHEMAS IN {settings.catalog}")
        schemas = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        connection.close()
        
        schema_list = ", ".join(schemas[:5])
        if len(schemas) > 5:
            schema_list += "..."
            
        print_status(True, f"Found {len(schemas)} schemas: {schema_list}")
        return True
            
    except Exception as e:
        print_status(False, f"Catalog access failed: {e}")
        return False


def test_anthropic_connection() -> bool:
    """Test connection to Anthropic Claude API."""
    print_header("4. Testing Anthropic (Claude) Connection")
    
    try:
        import anthropic
        from src.config.settings import get_anthropic_settings
        
        settings = get_anthropic_settings()
        
        print(f"  Using model: {settings.model}")
        
        client = anthropic.Anthropic(api_key=settings.api_key)
        
        # Simple test message
        message = client.messages.create(
            model=settings.model,
            max_tokens=50,
            messages=[
                {"role": "user", "content": "Say 'Connected!' and nothing else."}
            ]
        )
        
        response_text = message.content[0].text.strip()
        print_status(True, f"Claude responded: {response_text}")
        return True
            
    except anthropic.AuthenticationError:
        print_status(False, "Invalid API key")
        return False
    except Exception as e:
        print_status(False, f"Connection failed: {e}")
        return False


def main():
    """Run all connection tests."""
    print("\n" + "🔧 "*15)
    print("  DATA TRANSFORM AGENT - CONNECTION TEST")
    print("🔧 "*15)
    
    # Test 1: Environment Variables
    env_ok = test_environment_variables()
    
    if not env_ok:
        print("\n" + "="*50)
        print("❌ SETUP INCOMPLETE")
        print("="*50)
        print("\nPlease set all required environment variables:")
        print("  1. Copy .env.example to .env")
        print("  2. Fill in your Databricks and Anthropic credentials")
        print("  3. Run this script again")
        return 1
    
    # Test 2: Databricks Connection
    db_ok = test_databricks_connection()
    
    # Test 3: Databricks Schemas
    schemas_ok = False
    if db_ok:
        schemas_ok = test_databricks_schemas()
    
    # Test 4: Anthropic Connection
    anthropic_ok = test_anthropic_connection()
    
    # Summary
    print("\n" + "="*50)
    if db_ok and schemas_ok and anthropic_ok:
        print("🎉 ALL CONNECTIONS SUCCESSFUL!")
        print("="*50)
        print("\nYou're ready to start building the Data Transform Agent.")
        print("Next step: python scripts/init_sample_data.py")
        return 0
    else:
        print("❌ SOME CONNECTIONS FAILED")
        print("="*50)
        print("\nPlease fix the errors above and try again.")
        return 1


if __name__ == "__main__":
    sys.exit(main())