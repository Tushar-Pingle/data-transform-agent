# Known Issues & Improvements

## 🐛 Bugs to Fix

### 1. Empty String vs NULL Handling
**Priority:** High  
**Found:** Day 4 testing  
**Problem:** SQL `WHERE phone IS NOT NULL` doesn't filter empty strings `''`  
**Solution:** Update LLM prompt to generate:
```sql
WHERE column IS NOT NULL AND TRIM(column) != ''
```
**File:** `src/prompts/templates.py`

### 2. Source Table Selection
**Priority:** High  
**Found:** Day 4 testing  
**Problem:** When user says "filter silver.clean_customers", agent goes back to bronze.raw_customers  
**Expected:** Should use the existing silver table as source  
**Solution:** 
- Check if referenced table exists in silver/gold first
- Add logic to prefer existing tables over bronze source
- Ask clarifying question if ambiguous
**File:** `src/agents/transform_agent.py`, `src/services/llm_service.py`

### 3. Overwrite vs Create New Table
**Priority:** Medium  
**Found:** Day 4 testing  
**Problem:** Sometimes creates new table instead of overwriting existing  
**Solution:** 
- Parse user intent for "overwrite" vs "create new"
- Add explicit confirmation: "This will overwrite existing table. Confirm?"
**File:** `src/agents/transform_agent.py`

---

## 🚀 Enhancements (Week 2)

### 1. SQL Validation Layer
**Priority:** High  
**Description:** Validate SQL before execution using sqlglot
- Syntax validation
- Table/column existence check
- Dangerous operation detection (DROP, TRUNCATE)
**File:** `src/tools/validation_tools.py`

### 2. DRY RUN Mode
**Priority:** High  
**Description:** Preview results before executing
- Execute with LIMIT 10
- Show before/after row counts
- Show sample of transformed data
**File:** `src/agents/transform_agent.py`

### 3. Transform History
**Priority:** Medium  
**Description:** Track all executed transforms
- Save SQL to history
- Enable rollback capability
- Show recent transforms
**File:** `src/models/history.py`

### 4. Better Error Messages
**Priority:** Medium  
**Description:** Parse Databricks errors into user-friendly messages
- Column not found → suggest similar columns
- Table not found → list available tables
- Syntax error → highlight problem area
**File:** `src/services/databricks_service.py`

---

## 🎨 UI Improvements (Week 2+)

### 1. Gradio 6 Compatibility
**Priority:** High  
**Problem:** Current simple UI works but isn't polished
**Solution:** 
- Research Gradio 6 proper syntax
- Add sidebar with quick actions
- Better formatting for SQL display
- Syntax highlighting
**File:** `app.py`

### 2. Progress Indicators
**Priority:** Low  
**Description:** Show loading state during:
- Schema fetching
- SQL generation
- Query execution

### 3. Result Visualization
**Priority:** Low  
**Description:** 
- Show data preview in table format
- Row count comparisons
- Simple charts for aggregations

---

## 📊 Testing Needed

### Unit Tests
- [ ] `test_databricks_service.py` - Connection, queries, schema
- [ ] `test_llm_service.py` - SQL generation, parsing
- [ ] `test_transform_agent.py` - Intent detection, flow

### Integration Tests
- [ ] End-to-end transform flow
- [ ] Error handling scenarios
- [ ] Edge cases (empty tables, special characters)

### Test Cases to Add
1. Table with all NULL column
2. Table with special characters in names
3. Very large table (performance)
4. Concurrent transforms
5. Network timeout handling

---

## 📅 Priority Order for Week 2

1. **SQL Validation** - Prevent bad SQL from running
2. **DRY RUN Mode** - Preview before execute
3. **Empty String Handling** - Fix the NULL vs '' issue
4. **Source Table Logic** - Use existing silver/gold tables
5. **Error Messages** - Better user feedback
6. **Transform History** - Track and rollback
7. **UI Polish** - Gradio 6 proper implementation

---

## 💡 Future Ideas (Week 3+)

1. **Job Scheduling** - "Run every Monday 6am"
2. **Multi-table JOINs** - Cross-table transforms
3. **Data Lineage** - Track source → target
4. **Column Descriptions** - Add metadata
5. **Data Profiling** - Auto-detect issues
6. **REST API** - Headless mode

---

*Last Updated: Day 4 of Week 1*