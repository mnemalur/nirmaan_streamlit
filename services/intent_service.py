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
        self.prompt = ChatPromptTemplate.from_template(
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
            chain = self.prompt | self.llm
            resp = chain.invoke({"query": query})
            text = resp.content if hasattr(resp, "content") else str(resp)
            data = json.loads(text)
            phrases = data.get("diagnosis_phrases") or []
            return [p for p in phrases if isinstance(p, str) and p.strip()]
        except Exception as e:
            logger.warning(f"Intent extraction failed, falling back to raw query: {e}")
            # Fallback: just return the full query as a single phrase
            return [query] if query else []



