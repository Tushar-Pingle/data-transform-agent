# 🔄 Data Transform Agent

> Transform your data using natural language — powered by LLMs and Databricks

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🎯 What is this?

Data Transform Agent is an AI-powered tool that transforms data across Bronze → Silver → Gold layers using **natural language** instead of manual SQL coding.

**Traditional Way:**
```sql
CREATE OR REPLACE TABLE silver.customers AS
SELECT DISTINCT contact_id, INITCAP(first_name)...
-- 50+ lines of manual SQL
```

**With This Tool:**
```
You: "Clean raw_customers - remove nulls, dedupe, camelCase columns"
Agent: ✅ Created silver.customers (12,847 rows)
```

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| Data Platform | Databricks (Free Edition) |
| LLM | Claude API (Anthropic) |
| Frontend | Gradio |
| Backend | Python 3.10+ |

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- Databricks account ([Free Edition](https://www.databricks.com/try-databricks))
- Anthropic API key ([Console](https://console.anthropic.com/))

### Installation
```bash
# 1. Clone the repo
git clone https://github.com/yourusername/data-transform-agent.git
cd data-transform-agent

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env with your credentials

# 5. Test connections
python scripts/test_connections.py

# 6. Run the app
python app.py
```

## 📁 Project Structure
```
data-transform-agent/
├── src/
│   ├── config/          # Configuration management
│   ├── services/        # Databricks & LLM services
│   ├── agents/          # Transform orchestration
│   └── prompts/         # LLM prompt templates
├── scripts/             # Setup and utility scripts
├── tests/               # Test suite
├── app.py               # Main Gradio application
└── requirements.txt
```

## 🗺️ Roadmap

- [x] Week 1: Foundation - prompt → SQL → execute
- [ ] Week 2: Safety - validation, confirmation, rollback  
- [ ] Week 3: Advanced - scheduling, lineage
- [ ] Week 4: Polish and deployment

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

---

Built as a portfolio project demonstrating Data Engineering + AI integration.