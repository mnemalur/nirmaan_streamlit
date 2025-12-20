Cohort builder requirements  and assumptions

1.We are trying to serve real world evidence specialists who are trying to take a given clinical criteria and match with  patients in our database. 
2. They are in need of underdstanding the criteria and quickly see if there are significant potential  patients to further pursue this Clinical Criteria Opportunity. 
To this end we need an application where user can enter the criteria in natural language,  let the system understand and evaluate it for clarity and ambiguity. 
Use a Vector search to retriev precise diagnosis codes or drug codes etc. to resolve the ambiguity. 
Refine the clinical criteria into a structured manner by accouting for searched codes, date time frames and addditional demographic filters as applicable.
Send this refined criteria to a AB/BI  Text to SQL Genie that is trained on the patient encounter model to generate SQL and data results. 
Present these results in an intuitive manner to user to further assess if they want to dig into more insights or  adjus the criteria again to see different counts. 

The idea is to make this conversational like and let user discover the data, while  agent is doing the work and showing the reasoning to gain trust and enabling user to navigate the path for a successful patient set discovery. 

The tech stack we want to use are : Databricks Schema for Patient Model, Databricks Vector Search function for retrieving codes, Langgraph to create an agent and manage the conversations that user interacts,  LLM that can use available tools . A minimal UI to quickly iterate and show this and thus Streamlit while accounting the session problems by using forms as much as possible. 
Altair or ploty to show visuals based on patient result data set that we get.

Milestone 0 – Foundations & connectivity
M0‑1: Environment & Databricks config
Ensure requirements.txt has Streamlit + Databricks libraries, app runs locally, and secrets/config for Databricks are set.
M0‑2: Minimal Streamlit UI
One text area for “Clinical criteria” + submit button; echo the text back.
M0‑3: Databricks health‑check
Add a small function to run SELECT 1 (or simple row count) and show status in the UI.
M0‑4: First deploy & quick feedback
Deploy to your target environment and validate basic flow/connectivity with a couple of users.

Milestone 1 – Criteria understanding & ambiguity

M1‑1: LLM selection & config
Pick provider/model and wire config/env vars.
M1‑2: Interpretation service
Function that takes raw criteria and returns: summary, concepts, ambiguity list.
M1‑3: UI for interpretation
Section in Streamlit displaying summary, extracted concepts, and ambiguities clearly.
M1‑4: Example testing & prompt tuning
Try several real criteria, tune prompts/format.
M1‑5: Deploy & feedback
Get RWE users’ feedback on whether the interpretation matches their mental model.

Milestone 2 – Vector search for codes

M2‑1: Vector index readiness
Confirm/prepare Databricks Vector Search index for diagnosis/drug codes.
M2‑2: Vector search utility
Python function search_codes(concept_text) -> [code, description, score].
M2‑3: Code selection UI
For each concept, show top N codes and allow multi‑select for inclusion.
M2‑4: Logging & robustness
Handle errors, timeouts; log queries and selected codes.
M2‑5: Deploy & feedback
Validate that suggested codes look clinically reasonable.


Milestone 3 – Refined criteria (natural language + internal spec)
M3‑1: Internal criteria schema
Define a Python/JSON schema for conditions, drugs, dates, demographics, etc.
M3‑2: Criteria assembler
Build the internal criteria object from: original text, selected codes, and simple UI filters.
M3‑3: Natural‑language renderer
Function (or LLM prompt) that converts the internal object into a clear, human‑readable cohort definition.

M3‑4: Cohort definition card UI
Show the refined NL criteria prominently with simple controls (age range, date window, etc.).
M3‑5: Session persistence
Keep both internal spec and NL description in st.session_state for subsequent steps.
M3‑6: Deploy & feedback
Check that RWE users can read the refined criteria and feel it matches their intent.

Milestone 4 – Text‑to‑SQL Genie & Databricks execution
M4‑1: Genie wrapper
Implement a function that takes refined NL criteria (+ schema context) and returns SQL for the patient model.
M4‑2: SQL safety
Enforce allowlisted schemas/tables, no destructive commands, row/column limits, timeouts.
M4‑3: Execution & result retrieval
Run the SQL on Databricks, return aggregate metrics (e.g., patient count, encounter count).
M4‑4: Results UI & SQL preview
Show key numbers and (optionally) a truncated SQL snippet for transparency.
M4‑5: Deploy & feedback
Validate correctness of counts and trust in the generated SQL with domain experts.

Milestone 5 – Visualization of cohort results
M5‑1: Choose visualization library
Decide Altair vs Plotly and wire it into the app.
M5‑2: Aggregation queries for distributions
Extend queries (or add separate ones) to produce age, sex, and time distributions.
M5‑3: Build charts
Implement a small set of core charts (age histogram, sex bar chart, time trend).
M5‑4: Results page layout
Arrange KPIs at top, charts below with sensible defaults and tooltips.
M5‑5: Deploy & feedback
Check that visuals help users quickly decide if an opportunity is worth deeper exploration.


pwdMilestone 6 – Conversational agent with LangGraph
M6‑1: Add LangGraph and configure
Install dependency and set up a basic graph environment.
M6‑2: Graph design
Nodes for: interpret → vector search → refine criteria → text‑to‑SQL → execute → summarize.
M6‑3: State & memory
Ensure the graph maintains and updates the cohort criteria across turns.
M6‑4: Chat UI integration
Replace/augment the form with a chat interface that calls the LangGraph agent.
M6‑5: Reasoning trace display
Show high‑level “what I did” steps (no raw chain‑of‑thought) so users can follow the agent.
M6‑6: Deploy & feedback
Test multi‑turn refinement and gather usability/trust feedback.

Milestone 7 – Quality, safety, and evaluation
M7‑1: Logging & telemetry
Log prompts, refined criteria, generated SQL, and core metrics with privacy in mind.
M7‑2: Guardrails & validation
Strengthen input validation, schema allowlists, rate limiting, and error messages.
M7‑3: Evaluation scenarios
Define a small set of “known” cohort definitions with approximate expected results.
M7‑4: Automated tests
Add tests for interpretation, vector search, criteria rendering, and SQL generation.
M7‑5: Documentation & known limits
Write concise docs explaining how to use the app, assumptions, and current limitations.
