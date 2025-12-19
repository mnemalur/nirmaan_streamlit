# Dynamic Dimension Analysis - Three Approaches

## Problem
Hardcoding table/column names in dimension queries makes the code brittle and schema-dependent. We need a way to dynamically discover schema and generate SQL queries.

## Solution Options

### **Approach 1: Genie-Based SQL Generation** ⭐ (Recommended)
**Use Genie (AB/BI) to generate dimension queries**

**Pros:**
- Genie already knows your schema (trained on patient encounter model)
- No need to discover schema manually
- Leverages existing Genie infrastructure
- Most accurate SQL generation

**Cons:**
- Requires Genie API calls (slower than direct SQL)
- May need to create separate conversations for each dimension

**Implementation:**
```python
# Use Genie to generate SQL for each dimension
for dimension in dimensions:
    prompt = f"Generate SQL to analyze {dimension} for cohort table {cohort_table}"
    sql = genie_service.generate_sql(prompt)
    execute(sql)
```

**Best for:** When Genie is already set up and you want the most accurate queries.

---

### **Approach 2: Schema Discovery + LLM SQL Generation**
**Discover schema metadata, then use LLM to generate queries**

**Pros:**
- Fast schema discovery using INFORMATION_SCHEMA
- LLM can generate queries based on discovered schema
- More flexible than hardcoded queries
- Can cache schema info

**Cons:**
- Requires LLM calls (cost/latency)
- LLM may generate incorrect SQL
- Need to validate generated SQL

**Implementation:**
```python
# 1. Discover schema
schema_context = schema_service.get_schema_context(catalog, schema)

# 2. Use LLM to generate SQL
prompt = f"Given schema: {schema_context}, generate SQL for {dimension}"
sql = llm.generate_sql(prompt)

# 3. Execute
execute(sql)
```

**Best for:** When you want flexibility and don't want to rely on Genie for every query.

---

### **Approach 3: Hybrid - Schema Discovery + Template-Based Generation**
**Discover schema, then use templates with discovered column names**

**Pros:**
- Fast (no LLM/Genie calls)
- More reliable than LLM-generated SQL
- Still dynamic based on schema

**Cons:**
- Requires maintaining SQL templates
- Less flexible than LLM/Genie approach
- May need updates when schema changes

**Implementation:**
```python
# 1. Discover schema
columns = schema_service.discover_columns(table="patdemo")
age_col = find_column(columns, pattern="age")
gender_col = find_column(columns, pattern="gender")

# 2. Use template with discovered columns
sql = f"""
SELECT 
    CASE WHEN {age_col} < 18 THEN '<18' ...
    END as age_group,
    COUNT(*) as count
FROM {cohort_table} c
JOIN {patient_table} p ON c.{join_key} = p.{join_key}
GROUP BY age_group
"""
```

**Best for:** When you want a balance of speed and reliability.

---

## Recommended Implementation Strategy

### Phase 1: Start with Genie (Approach 1)
- Use Genie to generate dimension queries
- Genie already knows your schema
- Fastest to implement

### Phase 2: Add Schema Discovery (Approach 2/3)
- Add schema discovery service for validation
- Use discovered schema to validate Genie queries
- Fallback to LLM if Genie fails

### Phase 3: Optimize (Approach 3)
- Cache common dimension queries
- Use templates for frequently-used dimensions
- Only use Genie/LLM for new or complex dimensions

---

## Code Structure

```
services/
├── schema_discovery.py          # NEW: Discovers Unity Catalog schema
├── dynamic_dimension_analysis.py  # NEW: Uses schema + LLM/Genie
└── dimension_analysis.py        # EXISTING: Hardcoded queries (fallback)
```

## Usage Example

```python
# Option 1: Use Genie (recommended)
dynamic_service = DynamicDimensionAnalysisService()
results = dynamic_service.analyze_dimensions_dynamically(
    cohort_table="pasrt_uat.pas_temp_cohort.cohort_20241217_143022",
    has_medrec_key=True,
    use_genie=True  # Use Genie to generate SQL
)

# Option 2: Use LLM
results = dynamic_service.analyze_dimensions_dynamically(
    cohort_table="...",
    has_medrec_key=True,
    use_genie=False  # Use LLM instead
)

# Option 3: Use hardcoded (fallback)
dimension_service = DimensionAnalysisService()
results = dimension_service.analyze_dimensions(
    cohort_table="...",
    has_medrec_key=True
)
```

## Next Steps

1. **Test Genie approach**: Try generating dimension queries with Genie
2. **Add schema discovery**: Implement schema discovery service
3. **Integrate into UI**: Add option to use dynamic vs hardcoded queries
4. **Monitor performance**: Compare Genie vs LLM vs hardcoded approaches
