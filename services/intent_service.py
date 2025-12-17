"""
Intent Service - Uses Databricks-hosted LLM (e.g., Meta-Llama) to interpret
the user's natural language query and extract structured intent, starting
with diagnosis-related phrases for vector search.
"""

import json
import logging
import os
from typing import Dict, List

from databricks_langchain import ChatDatabricks
from langchain_core.prompts import ChatPromptTemplate

logger = logging.getLogger(__name__)


class IntentService:
    """LLM-based intent extractor for clinical queries."""

    def __init__(self, model: str | None = None, temperature: float = 0.0):
        self.llm = ChatDatabricks(
            model=model or os.getenv("INTENT_MODEL_NAME", "databricks-meta-llama-3-3-70b-instruct"),
            temperature=temperature,
        )
        # Prompt for extracting short diagnosis phrases used later for vector search
        self.intent_prompt = ChatPromptTemplate.from_template(
            """
You are a clinical intent extraction model.

Given a user query, extract the key diagnosis-related phrase(s) that should be
used to look up standard clinical codes (ICD, SNOMED, etc.).

Respond with STRICT JSON only, no prose, matching this schema:
{
  "diagnosis_phrases": ["..."]
}

User query: {query}
            """.strip()
        )

    def extract_diagnosis_phrases(self, query: str) -> List[str]:
        """Call the LLM to extract diagnosis phrases from the user query."""
        try:
            chain = self.intent_prompt | self.llm
            resp = chain.invoke({"query": query})
            text = resp.content if hasattr(resp, "content") else str(resp)
            data = json.loads(text)
            phrases = data.get("diagnosis_phrases") or []
            return [p for p in phrases if isinstance(p, str) and p.strip()]
        except Exception as e:
            logger.warning(f"Intent extraction failed, falling back to raw query: {e}")
            # Fallback: just return the full query as a single phrase
            return [query] if query else []

    def analyze_criteria(self, criteria: str) -> Dict:
        """Analyze raw clinical criteria text into summary, concepts, and ambiguities.

        Returns a dict shaped like:
        {
          "summary": "...",
          "conditions": ["..."],
          "drugs": ["..."],
          "procedures": ["..."],
          "demographics": ["..."],
          "timeframe": "...",
          "ambiguities": ["..."]
        }
        """
        analysis_prompt = ChatPromptTemplate.from_template(
            """
You are helping a real-world evidence specialist understand a draft clinical cohort definition.

Given the user's free-text criteria, do three things:
1) Summarize the intent in 1–2 plain-language sentences.
2) Extract key clinical concepts into SHORT phrases grouped by:
   - conditions
   - drugs
   - procedures
   - demographics (age, sex, setting, etc.)
   - timeframe (lookback windows, index periods, etc.)
3) List important ambiguities or missing specifics that would affect cohort construction
   (for example: unclear time window, vague disease severity, unspecified care setting).

Respond with STRICT JSON only, no prose, matching this schema exactly:
{{
  "summary": "string",
  "conditions": ["string"],
  "drugs": ["string"],
  "procedures": ["string"],
  "demographics": ["string"],
  "timeframe": "string",
  "ambiguities": ["string"]
}}

User criteria: {criteria}
            """.strip()
        )

        try:
            chain = analysis_prompt | self.llm
            resp = chain.invoke({"criteria": criteria})
            text = resp.content if hasattr(resp, "content") else str(resp)
            data = json.loads(text)
        except Exception as e:
            logger.warning(f"Criteria analysis failed, returning minimal structure: {e}")
            # Fallback: minimal but safe structure
            return {
                "summary": criteria or "",
                "conditions": [],
                "drugs": [],
                "procedures": [],
                "demographics": [],
                "timeframe": "",
                "ambiguities": [f"Automatic analysis failed: {e}"],
            }

        # Normalize keys and provide defaults so the UI never breaks
        return {
            "summary": data.get("summary") or (criteria or ""),
            "conditions": data.get("conditions") or [],
            "drugs": data.get("drugs") or [],
            "procedures": data.get("procedures") or [],
            "demographics": data.get("demographics") or [],
            "timeframe": data.get("timeframe") or "",
            "ambiguities": data.get("ambiguities") or [],
        }



