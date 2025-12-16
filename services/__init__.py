"""
Services package for Clinical Cohort Assistant
Contains vector search, Genie, cohort management services, and LangGraph agent
"""

import os

# Only import real services if not in test mode
# This prevents import errors when Databricks packages aren't installed
if os.getenv('TEST_MODE', 'false').lower() != 'true':
    try:
        from .vector_search import VectorSearchService
        from .genie_service import GenieService
        from .cohort_manager import CohortManager
        from .cohort_agent import CohortAgent
        __all__ = ['VectorSearchService', 'GenieService', 'CohortManager', 'CohortAgent']
    except ImportError:
        # If imports fail, allow test mode to work
        __all__ = []
else:
    # In test mode, don't import real services
    __all__ = []

