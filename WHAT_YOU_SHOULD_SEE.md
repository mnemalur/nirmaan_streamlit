# What You Should See - Conversational Interface

## When You Run the App

### 1. **Main Page (Chat Page)**
```
🏥 Clinical Cohort Assistant          [Conversational cohort builder]

[Sidebar: Current Context, Quick Actions]

[Chat History Area - Empty if first time]

┌─────────────────────────────────────────────────────────┐
│ Describe your clinical criteria or ask a question...    │
│                                                          │
│                                    [Send Button]        │
└─────────────────────────────────────────────────────────┘
```

### 2. **After You Type a Query (e.g., "Find patients with diabetes")**

```
[User Message]
Find patients with diabetes

[Assistant Message]
🤔 Thinking...

I understood you're looking for: diabetes
I found 10 relevant clinical codes.

[📋 View 10 Codes Found] (expander)
[🧠 How I'm enriching your request for Genie] (expander)
[📝 View Generated SQL] (expander)
[🚀 Execute Query & Create Cohort] (button)

[🔍 What I'm doing (reasoning steps)] (expander)
  • Interpret Intent: Analyzing your query...
  • Vector Search: Found 10 codes...
  • Generate SQL: SQL generated...
```

### 3. **Follow-up Question**

```
[User Message]
What are the demographics?

[Assistant Message]
Here are the demographic characteristics of your cohort:
[Shows demographics charts/data]
```

## If You Don't See the Chat Interface

**Possible Issues:**

1. **Services Not Initialized**
   - Check sidebar - should show "Current Context"
   - If you see "Please configure Databricks connection" → Go to Configuration page

2. **Agent Not Initialized**
   - Check for error message: "Failed to initialize cohort agent"
   - This happens if services (vector_service, genie_service, etc.) aren't ready

3. **On Wrong Page**
   - Make sure you're on "Chat" page (not "Configuration")
   - Check sidebar navigation

4. **Chat Input Not Visible**
   - Scroll to bottom of page
   - Chat input should always be at the bottom
   - If services aren't initialized, chat input won't show

## How to Test

1. **Start App**: `streamlit run app.py`
2. **Go to Chat Page** (should be default)
3. **If services not initialized**: Go to Configuration, enter credentials, save
4. **Type in chat**: "Find patients with heart failure"
5. **See agent respond** with codes, SQL, reasoning

## Key Differences from M5

| M5 (Linear) | M6 (Conversational) |
|-------------|---------------------|
| Forms with submit buttons | Chat input at bottom |
| Step-by-step expanders | Natural language queries |
| Manual code selection | Agent finds codes automatically |
| Click "Next Step" buttons | Just type and get response |
| Fixed workflow | Flexible, conversational flow |

