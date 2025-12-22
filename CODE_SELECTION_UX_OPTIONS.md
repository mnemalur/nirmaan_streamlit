# Code Selection UX Options - Ideation

## Current State
- Codes shown in expander (expanded by default)
- User must type response: "use all", "select codes", "I want X, Y, Z"
- Natural language parsing to understand user intent
- **Problem**: Requires typing, not very visual, can be unclear

## Option 1: Quick Action Buttons (Recommended ⭐)
**Approach**: Show prominent buttons below the codes table

**UI Layout**:
```
📋 View All 10 Codes Found [Expanded]
[Code table with all codes]

💬 How would you like to proceed?

[🟢 Use All Codes]  [🔵 Select Specific Codes]  [🔴 Exclude Some Codes]
```

**Pros**:
- ✅ Very clear and visual
- ✅ One click - no typing required
- ✅ Works well in Streamlit (buttons are native)
- ✅ Fast for "use all" case (most common)
- ✅ Still allows typing for advanced users

**Cons**:
- ⚠️ Takes up vertical space
- ⚠️ Need to handle "Select Specific" - opens another UI

**Implementation**:
- Use `st.button()` for quick actions
- "Select Specific" opens multi-select or checkboxes
- "Exclude Some" opens similar selection UI

---

## Option 2: Interactive Checkboxes in Table (Hybrid)
**Approach**: Add checkboxes directly in the codes dataframe

**UI Layout**:
```
📋 View All 10 Codes Found [Expanded]
[Code table with checkboxes in first column]
☑ Code | Description | Vocabulary
☑ E11.9 | Type 2 diabetes | ICD10CM
☐ E11.8 | Type 2 diabetes with complications | ICD10CM
☑ I50.9 | Heart failure | ICD10CM
...

[🟢 Use Selected (3)]  [🟢 Use All]  [🔴 Clear Selection]
```

**Pros**:
- ✅ Visual selection - see exactly what's selected
- ✅ Can select/deselect individual codes
- ✅ Shows count of selected codes
- ✅ Familiar UI pattern (like email inbox)

**Cons**:
- ⚠️ Streamlit doesn't natively support checkboxes in dataframes
- ⚠️ Would need custom component or workaround
- ⚠️ More complex to implement

**Implementation Options**:
- Use `st.data_editor()` with editable column (newer Streamlit)
- Use `st.checkbox()` for each row (works but verbose)
- Custom component using `streamlit-component-lib`

---

## Option 3: Multi-Select Dropdown (Simple)
**Approach**: Dropdown with all codes, user selects multiple

**UI Layout**:
```
📋 View All 10 Codes Found [Expanded]
[Code table for reference]

💬 Select codes to use:

[Multi-select dropdown: "Select codes..."]
  ☑ E11.9 - Type 2 diabetes
  ☐ E11.8 - Type 2 diabetes with complications
  ☑ I50.9 - Heart failure
  ...

[🟢 Use Selected]  [🟢 Use All]  [🔴 Clear]
```

**Pros**:
- ✅ Compact - doesn't take much space
- ✅ Native Streamlit `st.multiselect()` support
- ✅ Easy to implement
- ✅ Shows selected count

**Cons**:
- ⚠️ Dropdown can be long if many codes
- ⚠️ Can't see descriptions easily while selecting
- ⚠️ Less visual than checkboxes

**Implementation**:
- Use `st.multiselect()` with formatted labels: "E11.9 - Type 2 diabetes"
- Show selected codes summary below

---

## Option 4: Two-Step with Smart Defaults
**Approach**: Show codes grouped by condition, with smart suggestions

**UI Layout**:
```
📋 View All 10 Codes Found [Expanded]

**Diabetes Codes (5 codes)** - Suggested: Use all
☑ E11.9 - Type 2 diabetes (most common)
☑ E11.8 - Type 2 diabetes with complications
☐ E11.65 - Type 2 diabetes with hyperglycemia
☐ E11.22 - Type 2 diabetes with kidney complications
☐ E11.29 - Type 2 diabetes with other complications

[🟢 Use All Diabetes Codes]  [🔵 Select Specific]

**Heart Failure Codes (5 codes)** - Suggested: Use all
☑ I50.9 - Heart failure (most common)
☑ I50.1 - Left ventricular failure
☐ I50.2 - Systolic heart failure
☐ I50.3 - Diastolic heart failure
☐ I50.4 - Combined systolic and diastolic heart failure

[🟢 Use All Heart Failure Codes]  [🔵 Select Specific]

[🟢 Use All Selected]  [🔴 Review Selection]
```

**Pros**:
- ✅ Groups codes by condition (more intuitive)
- ✅ Highlights "most common" codes
- ✅ Can select by condition group
- ✅ Shows what's selected at a glance

**Cons**:
- ⚠️ More complex UI
- ⚠️ Requires grouping logic
- ⚠️ Takes more vertical space

**Implementation**:
- Group codes by `condition` field
- Use expanders for each condition group
- Add checkboxes or buttons per group

---

## Option 5: Natural Language + Visual Confirmation
**Approach**: Keep typing, but show visual confirmation

**UI Layout**:
```
📋 View All 10 Codes Found [Expanded]
[Code table]

💬 Type your selection (or use buttons below):
[Text input box]

Examples:
- "use all" - Use all 10 codes
- "E11.9, I50.9" - Use specific codes
- "exclude E11.8" - Use all except E11.8
- "diabetes codes only" - Use only diabetes-related codes

[🟢 Use All]  [🔵 Select Specific]  [🔴 Exclude Some]

**Selected Codes Preview:**
✅ E11.9 - Type 2 diabetes
✅ I50.9 - Heart failure
(2 of 10 codes selected)

[🟢 Confirm Selection]
```

**Pros**:
- ✅ Flexible - supports natural language
- ✅ Visual confirmation before proceeding
- ✅ Shows preview of what will be used
- ✅ Best of both worlds (typing + visual)

**Cons**:
- ⚠️ Still requires some typing for specific codes
- ⚠️ More complex to parse natural language

**Implementation**:
- Keep current natural language parsing
- Add visual preview of selected codes
- Add confirmation step before proceeding

---

## Option 6: Card-Based Selection (Modern UI)
**Approach**: Show codes as cards/tiles that can be clicked

**UI Layout**:
```
📋 View All 10 Codes Found

[Card Grid - 3 columns]

┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ ✅ E11.9     │  │ ☐ E11.8     │  │ ✅ I50.9     │
│ Type 2      │  │ Type 2 w/   │  │ Heart       │
│ diabetes    │  │ complications│  │ failure     │
│ ICD10CM     │  │ ICD10CM     │  │ ICD10CM     │
└─────────────┘  └─────────────┘  └─────────────┘

[More cards...]

Selected: 2 codes | [🟢 Use Selected] [🟢 Use All] [🔴 Clear]
```

**Pros**:
- ✅ Modern, visually appealing
- ✅ Easy to see and select
- ✅ Can show more info per code
- ✅ Good for touch interfaces

**Cons**:
- ⚠️ Requires custom Streamlit component
- ⚠️ More complex to implement
- ⚠️ Takes significant vertical space

**Implementation**:
- Would need custom component or use columns with styled containers
- Click to toggle selection
- Visual feedback on selection

---

## Option 7: Smart Suggestions with Quick Actions
**Approach**: AI suggests best codes, user confirms or adjusts

**UI Layout**:
```
📋 View All 10 Codes Found [Expanded]
[Code table]

💡 **Smart Suggestion**: Based on your criteria, I recommend using these 3 codes:
   ✅ E11.9 - Type 2 diabetes (most specific match)
   ✅ I50.9 - Heart failure (most specific match)
   ✅ E11.8 - Type 2 diabetes with complications (common variant)

[🟢 Use Suggested (3)]  [🔵 Use All (10)]  [🔵 Select Different]

Or type: "use all", "I want E11.9 and I50.9", "exclude complications"
```

**Pros**:
- ✅ Reduces decision fatigue
- ✅ Suggests most relevant codes
- ✅ Still allows full control
- ✅ Faster for common cases

**Cons**:
- ⚠️ Requires logic to determine "best" codes
- ⚠️ User might not trust suggestions
- ⚠️ Need to explain why codes were suggested

**Implementation**:
- Use relevance scores from vector search
- Suggest top N codes or codes above threshold
- Show reasoning for suggestions

---

## Comparison Matrix

| Option | Ease of Use | Visual Clarity | Implementation | Flexibility | Space Usage |
|--------|-------------|----------------|-----------------|-------------|-------------|
| **1. Quick Buttons** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ |
| **2. Checkboxes in Table** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| **3. Multi-Select Dropdown** | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **4. Two-Step Grouped** | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐ |
| **5. NL + Visual Confirm** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| **6. Card-Based** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐ |
| **7. Smart Suggestions** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ |

---

## Recommended Approach: **Option 1 (Quick Buttons) + Option 5 (Visual Confirmation)**

**Hybrid Solution**:
1. Show codes in expander (current)
2. Add prominent buttons: [Use All] [Select Specific] [Exclude Some]
3. If user clicks "Select Specific" → Show multi-select or checkboxes
4. Show preview of selected codes before confirming
5. Still allow natural language input for power users

**Why This Works**:
- ✅ Fast for common case ("use all")
- ✅ Clear visual options
- ✅ Flexible for advanced users
- ✅ Easy to implement in Streamlit
- ✅ Maintains conversational feel

**UI Flow**:
```
Step 1: Codes shown in expander
Step 2: Buttons appear + text input still works
Step 3: If "Select Specific" → Show selection UI
Step 4: Preview selected codes
Step 5: Confirm and proceed
```

---

## Implementation Considerations

### Streamlit Limitations:
- No native checkbox support in dataframes (need workaround)
- Buttons trigger rerun (need to handle state)
- Multi-select works well but can be long
- Custom components possible but add complexity

### Best Practices:
- Keep it simple - don't over-engineer
- Support both quick actions and detailed selection
- Show clear feedback on what's selected
- Allow easy correction/change of mind
- Maintain conversational flow

### User Experience Goals:
- **Speed**: "Use all" should be 1 click
- **Clarity**: User should know exactly what's selected
- **Flexibility**: Support both simple and complex selections
- **Forgiveness**: Easy to change selection before confirming

