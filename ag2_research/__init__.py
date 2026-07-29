"""AG2 Multi-Agent Strategy Research Framework.

Usage:
    from ag2_research import Orchestrator

    orch = Orchestrator()
    orch.run_workflow(
        "brainstorm",
        topic="How to improve B1 win rate above 60%?",
        research_context="V2 is production champion with NDCG 0.826...",
    )
"""

from .config import ResearchConfig
from .orchestrator import Orchestrator, ResearchSession

__all__ = ["Orchestrator", "ResearchConfig", "ResearchSession"]
