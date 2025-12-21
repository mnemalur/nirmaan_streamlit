# Explore Cohort & Adjust Criteria Implementation Plan

## Current State
- User gets **counts only** (aggregated: patients, visits, sites) from Genie SQL execution
- No actual patient rows/materialized cohort table yet
- User sees counts and can decide: **Explore** OR **Adjust Criteria**

## Two Pathways After Seeing Counts

### Pathway 1: Explore Cohort
**User says**: "explore", "analyze", "deep dive", "yes" (to explore)

**Flow**:
1. **Materialize cohort** - Create cohort table with actual patient rows from Genie SQL
2. **Run dimension analysis** - Execute parallel queries across 9 dimensions
3. **Display visualizations** - Show charts/graphs quickly

### Pathway 2: Adjust Criteria
**User says**: "adjust", "refine", "change", "modify", "different"

**Flow**:
1. **Capture refinement input** - Ask user what they want to change (e.g., "too many patients", "add age filter", "more specific")
2. **Refine criteria** - Use user input + existing criteria to build refined criteria
3. **Generate new SQL** - Send refined criteria to Genie
4. **Get new counts** - Execute SQL and get updated counts
5. **Show new counts** - Display new counts (back to analysis_decision state)

## Visual Flow Diagram

```
User sees counts (patients, visits, sites)
         │
         ├─ "explore" / "analyze" / "deep dive"
         │         │
         │         └─→ [explore_cohort node]
         │                   │
         │                   ├─→ Materialize cohort table (if not exists)
         │                   │
         │                   ├─→ Run dimension analysis (9 dimensions in parallel)
         │                   │
         │                   └─→ Display visualizations (tabs: Demographics, Visits, Sites)
         │
         └─ "adjust" / "refine" / "change"
                   │
                   └─→ [refine_criteria node]
                             │
                             ├─→ Capture user refinement input
                             │   ("too many patients", "add age filter", etc.)
                             │
                             ├─→ Build refined criteria (LLM combines original + refinement)
                             │
                             └─→ Restart flow:
                                 interpret_intent → search_codes → confirm_codes → 
                                 prepare_criteria → generate_sql → get_counts → 
                                 [back to analysis_decision]
```

## Step-by-Step Implementation Plan

### Step 1: Detect User Intent (Explore vs Adjust)
**Location**: `services/cohort_agent.py` - `_classify_query()` method

**Current**: 
- Line 198: Detects "explore" → routes to "analysis" step ✅
- Line 202: Detects "refine" → routes to "refine" step ✅

**Action Needed**:
- ✅ Already handles both pathways
- Ensure "refine" pathway captures user refinement input properly

### Step 2: Add Nodes to LangGraph
**Location**: `services/cohort_agent.py` - `_build_graph()` method

**Action Needed**:
- Add node: `"explore_cohort"` → calls `_explore_cohort()` method
- Add node: `"refine_criteria"` → calls `_refine_criteria()` method (captures user input)
- Update routing: 
  - When `current_step == "analysis"` → route to `"explore_cohort"`
  - When `current_step == "refine"` → route to `"refine_criteria"`
- Flow: 
  - `explore_cohort` → END (shows dimension results)
  - `refine_criteria` → `interpret_intent` → `search_codes` → ... (restart flow with refined criteria)

### Step 3: Create `_refine_criteria()` Method
**Location**: `services/cohort_agent.py`

**Method Flow**:
1. Get user's refinement input from `state.get("user_query")`
2. Get existing criteria from state (`criteria_analysis`, `selected_codes`, etc.)
3. Use LLM/intent_service to understand what user wants to change:
   - "too many patients" → add more restrictive filters
   - "add age filter" → extract age requirement
   - "more specific" → narrow down conditions
   - "exclude X" → add exclusion
4. Build refined criteria combining:
   - Original criteria
   - User's refinement input
   - Existing selected codes (if any)
5. Store refined criteria in state
6. Set `waiting_for = None` and `current_step = "new_cohort"` to restart flow

**Returns**: Updated state with refined criteria

### Step 4: Create `_explore_cohort()` Method
**Location**: `services/cohort_agent.py`

**Method Flow**:
1. Check if cohort table already exists in state
2. If NOT exists:
   - Get SQL from state (`state.get("sql")`)
   - Call `cohort_manager.materialize_cohort(session_id, sql)` 
   - Store cohort_table in state
3. If exists OR after materialization:
   - Get cohort_table from state
   - Detect structure (has_medrec_key, has_pat_key)
   - Call `dimension_service.analyze_dimensions(cohort_table, has_medrec_key, use_dynamic=True)`
   - Store dimension results in state

**Returns**: Updated state with cohort_table and dimension_results

### Step 5: Materialize Cohort (if not exists)
**Location**: `services/cohort_manager.py` - `materialize_cohort()` method

**Status**: ✅ Already exists (line 24)
- Takes `session_id` and `cohort_sql`
- Creates Delta table: `CREATE OR REPLACE TABLE ... AS {cohort_sql}`
- Returns: `{cohort_table, cohort_path, cohort_id, count}`

**Action Needed**: 
- Ensure it's called from `_explore_cohort()` when cohort_table doesn't exist

### Step 6: Run Dimension Analysis
**Location**: `services/dimension_analysis.py` - `analyze_dimensions()` method

**Status**: ✅ Already exists (line 348)
- Takes: `cohort_table`, `has_medrec_key`, `use_dynamic=True`
- Uses `DynamicDimensionAnalysisService` to:
  - Generate all 9 dimension SQL queries in parallel (LLM)
  - Execute all queries in parallel (ThreadPoolExecutor)
- Returns: `{dimensions: {...}, errors: {...}, cohort_table: ...}`

**9 Dimensions**:
1. **Patient Demographics**: gender, race, ethnicity, age_groups
2. **Visit Characteristics**: visit_level, admit_type, admit_source
3. **Site Characteristics**: urban_rural, teaching, bed_count

**Action Needed**:
- Call from `_explore_cohort()` after cohort is materialized

### Step 7: Handle Refine Criteria in UI
**Location**: `app.py` - `process_query_conversational()` function

**Action Needed**:
- When `current_step == "refine"`:
  - Show message: "I understand you want to adjust your criteria. What would you like to change?"
  - Examples: "The counts are too high/low", "Add age filter", "Make it more specific", etc.
  - Wait for user input
  - Process through `_refine_criteria()` node
  - Restart flow with refined criteria

### Step 8: Display Dimension Results in UI
**Location**: `app.py` - `process_query_conversational()` function

**Current**: Line 1136 mentions "Explore this cohort" but doesn't handle it

**Action Needed**:
- When `current_step == "analysis"` or `waiting_for == "exploring"`:
  - Check if `dimension_results` in result_state
  - Call `display_dimension_results_compact(dimension_results)`
  - Show loading spinner while materializing/analyzing
  - Display success message with cohort count

### Step 9: Update UI Response Flow
**Location**: `app.py` - `process_query_conversational()`

**Current Flow** (analysis_decision):
1. Intro message
2. Counts (blue ribbon)
3. SQL expander
4. "What would you like to do next?" → "Explore this cohort"

**New Flow** (when user chooses explore):
1. "Great! Let me explore this cohort for you..."
2. Show spinner: "Materializing cohort and analyzing dimensions..."
3. Display dimension visualizations (tabs: Demographics, Visit Characteristics, Site Characteristics)
4. Show cohort table info (count, table name)

### Step 10: Handle Edge Cases
- **SQL not available**: Show error, ask user to regenerate
- **Materialization fails**: Show error, suggest refining criteria
- **Dimension analysis fails**: Show partial results, log errors
- **Cohort already exists**: Skip materialization, go straight to analysis

## Implementation Order

### Phase 1: Core Functionality
1. ✅ Add `_refine_criteria()` method to `CohortAgent` (captures user input, builds refined criteria)
2. ✅ Add `_explore_cohort()` method to `CohortAgent`
3. ✅ Add "refine_criteria" and "explore_cohort" nodes to LangGraph workflow
4. ✅ Update routing: "refine" → "refine_criteria", "analysis" → "explore_cohort"
5. ✅ Call `materialize_cohort()` if cohort_table doesn't exist (in explore path)
6. ✅ Call `analyze_dimensions()` after materialization (in explore path)
7. ✅ Restart flow with refined criteria (in refine path)

### Phase 2: UI Integration
8. ✅ Update `process_query_conversational()` to handle "refine" step
9. ✅ Update `process_query_conversational()` to handle "analysis" step
10. ✅ Display loading states (materializing, analyzing)
11. ✅ Call `display_dimension_results_compact()` with results
12. ✅ Show success/error messages for both pathways

### Phase 3: Polish
13. ✅ Handle edge cases (no SQL, materialization fails, refinement fails, etc.)
14. ✅ Add progress indicators for both pathways
15. ✅ Cache cohort_table in state to avoid re-materialization
16. ✅ Preserve context when refining (keep original criteria visible)

## Key Files to Modify

1. **`services/cohort_agent.py`**:
   - Add `_refine_criteria()` method (captures user input, builds refined criteria)
   - Add `_explore_cohort()` method
   - Add "refine_criteria" and "explore_cohort" nodes to graph
   - Update routing logic for both pathways

2. **`app.py`**:
   - Handle "refine" step in `process_query_conversational()` (ask what to change)
   - Handle "analysis" step in `process_query_conversational()` (explore cohort)
   - Display dimension results (for explore path)
   - Show loading states for both pathways

3. **State Management**:
   - Add `cohort_table` to AgentState (if not already) ✅ Already exists
   - Add `dimension_results` to AgentState
   - Add `refined_criteria` to AgentState (to preserve refinement context)
   - Persist cohort_table across turns

## Dependencies
- ✅ `cohort_manager.materialize_cohort()` - exists
- ✅ `dimension_service.analyze_dimensions()` - exists
- ✅ `display_dimension_results_compact()` - exists
- ✅ Dimension analysis services initialized

## Testing Checklist

### Explore Pathway:
- [ ] User says "explore" → cohort materializes
- [ ] Dimension analysis runs in parallel
- [ ] Visualizations display correctly
- [ ] Handles missing SQL gracefully
- [ ] Handles materialization errors
- [ ] Reuses existing cohort_table if available
- [ ] Shows appropriate loading states

### Refine Pathway:
- [ ] User says "adjust" → asks what to change
- [ ] User provides refinement input → criteria refined
- [ ] Refined criteria sent to Genie → new SQL generated
- [ ] New counts displayed → back to analysis_decision state
- [ ] Original criteria context preserved
- [ ] Handles unclear refinement input gracefully

