#!/usr/bin/env python3
"""
Data Transform Agent - Gradio Application

A conversational AI tool for transforming data using natural language.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

import gradio as gr
from src.agents.transform_agent import TransformAgent


# Global agent instance
agent = None


def initialize_agent():
    """Initialize or reinitialize the agent."""
    global agent
    try:
        if agent:
            agent.close()
        agent = TransformAgent()
        return True, "Agent initialized successfully"
    except Exception as e:
        return False, f"Failed to initialize agent: {str(e)}"


def chat(message: str, history: list) -> str:
    """
    Process a chat message.
    
    Args:
        message: User's input message
        history: Conversation history (managed by Gradio)
        
    Returns:
        Agent's response string
    """
    global agent
    
    if not message.strip():
        return "Please enter a message."
    
    if agent is None:
        success, msg = initialize_agent()
        if not success:
            return f"❌ {msg}"
    
    try:
        response = agent.chat(message)
        return response
    except Exception as e:
        return f"❌ **Error:** {str(e)}\n\nTry `help` for available commands."


def create_app():
    """Create and configure the Gradio app."""
    
    # Initialize agent on startup
    initialize_agent()
    
    # Example prompts
    examples = [
        "show tables",
        "describe raw_customers",
        "What columns are in raw_customers?",
        "Clean raw_customers - remove nulls, dedupe by contact_id, standardize names",
        "help",
    ]
    
    # Create the chat interface
    demo = gr.ChatInterface(
        fn=chat,
        title="🔄 Data Transform Agent",
        description="Transform your data using natural language. Powered by Claude AI and Databricks.\n\n**Try:** 'show tables' or 'Clean raw_customers, remove nulls'",
        examples=examples,
    )
    
    return demo


# Main entry point
if __name__ == "__main__":
    print("=" * 60)
    print(" 🔄 Data Transform Agent")
    print(" Starting Gradio server...")
    print("=" * 60)
    
    app = create_app()
    
    print("\n✅ Agent ready!")
    print("📍 Open http://localhost:7860 in your browser\n")
    
    app.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=True
    )