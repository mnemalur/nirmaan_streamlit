# Clinical Criteria Real-World Analysis

## Research Findings

### Average Number of Conditions in Clinical Criteria

Based on research on clinical trial eligibility criteria and feasibility analysis:

1. **Clinical Trial Eligibility Criteria**:
   - **Average: 10-20 criteria per trial**
   - One study of 352,110 trials: **~10.4 criteria per trial**
   - Another study of 1,578 trials: **~17.3 criteria per trial**
   - Alzheimer's trials: **Median 15, Average 17.5** exclusion criteria per trial

2. **Real-World Evidence (RWE) Feasibility Analysis**:
   - Typically involves **3-7 conditions** per analysis
   - Often includes comorbidities and related conditions
   - May include multiple drugs, procedures, and demographic filters

3. **Multi-Condition Prevalence**:
   - 32.9% of adults have 2+ conditions
   - 20.7% have 3+ conditions
   - 12.3% have 4+ conditions
   - Older adults (65+): 73% have 2+ conditions

### Typical Criteria Structure

**Example Real-World Criteria** (multi-line, complex):
```
Find patients with:
- Type 2 diabetes mellitus (T2DM) diagnosed in the past 5 years
- Heart failure with reduced ejection fraction (HFrEF)
- Chronic kidney disease stage 3 or 4
- Age 65-85 years
- Currently on ACE inhibitors or ARBs
- Exclude patients with:
  - Active cancer
  - End-stage renal disease
  - Recent hospitalization for heart failure (within 30 days)
```

**Breakdown**:
- **Conditions**: 3 (T2DM, HFrEF, CKD)
- **Demographics**: 1 (Age 65-85)
- **Drugs**: 2 (ACE inhibitors, ARBs)
- **Exclusions**: 3 (cancer, ESRD, recent HF hospitalization)
- **Timeframes**: 2 (past 5 years, within 30 days)

**Total**: ~11 distinct criteria elements

---

## Impact on Code Selection UI

### Current Implementation

**Vector Search Limits**:
- 10 codes per condition (limit=10)
- If user has 3 conditions → **30 codes**
- If user has 5 conditions → **50 codes**
- If user has 7 conditions → **70 codes**

**Current UI**:
- Shows ALL codes as inline checkboxes
- No pagination
- No filtering
- No bulk selection per condition

### Real-World Scenarios

#### Scenario 1: Simple (1-2 conditions)
- **Conditions**: 2
- **Codes**: 20 codes
- **UI Impact**: ✅ Manageable with current UI

#### Scenario 2: Typical (3-5 conditions)
- **Conditions**: 4
- **Codes**: 40 codes
- **UI Impact**: ⚠️ **Challenging** - Long scroll, hard to manage

#### Scenario 3: Complex (5-7 conditions)
- **Conditions**: 6
- **Codes**: 60 codes
- **UI Impact**: ❌ **Problematic** - Very long scroll, overwhelming

#### Scenario 4: Very Complex (7+ conditions)
- **Conditions**: 8
- **Codes**: 80 codes
- **UI Impact**: ❌ **Unusable** - Too many codes to manage manually

---

## Recommendations

### Immediate Needs (Based on Research)

1. **Handle 3-5 conditions gracefully** (most common case)
   - This means 30-50 codes need to be manageable
   - Current UI will struggle with this

2. **Support multi-line criteria input**
   - Users paste long criteria text
   - System should parse and extract conditions properly

3. **Optimize for most common case**
   - 3-5 conditions = 30-50 codes
   - Need better organization than flat checkbox list

### Proposed Solutions

#### Option 1: Collapsible Sections with Bulk Actions (Recommended)
```
📋 Diabetes (10 codes) [▼ Expand] [☑ Select All]
📋 Heart Failure (10 codes) [▼ Expand] [☑ Select All]
📋 CKD (10 codes) [▼ Expand] [☑ Select All]
📋 Hypertension (10 codes) [▼ Expand] [☑ Select All]

[Global: 🔍 Search codes...] [☑ Select All (40)]

Selected: 0 codes | [✅ Use Selected] [🟢 Use All]
```

**Benefits**:
- ✅ Handles 30-50 codes easily
- ✅ Organized by condition
- ✅ Bulk selection per condition
- ✅ Search/filter capability
- ✅ Scales to 70+ codes

#### Option 2: Smart Suggestions for Large Lists
```
I found 40 codes. I recommend these 8 most relevant:
  ✅ E11.9 - Type 2 diabetes (recommended)
  ✅ I50.9 - Heart failure (recommended)
  ...

[✅ Use Recommended (8)] [🟢 Use All (40)] [🔵 Select Different]
```

**Benefits**:
- ✅ Reduces decision fatigue
- ✅ Fast for common case
- ✅ Still allows full selection

#### Option 3: Pagination with Search
```
Showing 1-20 of 40 codes
[← Previous] [Next →]

[🔍 Search: ___________]

Filter: [All] [Diabetes] [Heart Failure] [CKD] [Hypertension]

[Checkboxes for current page]
```

**Benefits**:
- ✅ Handles any number of codes
- ✅ Searchable
- ✅ Filterable

---

## Multi-Line Criteria Input

### Current State
- Streamlit `st.chat_input()` supports multi-line
- LLM can parse multi-line text
- Intent service extracts conditions properly

### Considerations
- Long criteria text (500+ words) might need chunking
- Multiple conditions in one paragraph need proper extraction
- Timeframes and exclusions need clear parsing

**Status**: ✅ Should work with current implementation, but needs testing with real multi-line criteria

---

## Statistics Summary

| Metric | Average | Range | Impact on UI |
|--------|---------|-------|--------------|
| **Conditions per criteria** | 3-5 | 1-8 | Medium-High |
| **Codes per condition** | 10 | 5-15 | Fixed (limit=10) |
| **Total codes (typical)** | 30-50 | 10-80 | **Needs optimization** |
| **Criteria text length** | 200-500 words | 50-1000+ | Should work |
| **Number of lines** | 5-15 | 1-30+ | Should work |

---

## Action Items

1. **Test with real criteria** (3-5 conditions, 30-50 codes)
2. **Measure user experience** with current UI
3. **Implement collapsible sections** if current UI is overwhelming
4. **Add bulk selection** per condition
5. **Consider search/filter** for 50+ codes

---

## Conclusion

**Most users will have 3-5 conditions** = **30-50 codes**

Current inline checkbox UI will likely be:
- ✅ **OK for 1-2 conditions** (10-20 codes)
- ⚠️ **Challenging for 3-5 conditions** (30-50 codes)
- ❌ **Unusable for 6+ conditions** (60+ codes)

**Recommendation**: Implement collapsible sections with bulk actions to handle the typical 3-5 condition case gracefully.

