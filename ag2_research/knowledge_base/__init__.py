"""Knowledge base for AG2 — strategy research closures.

Public API:

    from ag2_research.knowledge_base import load, build_context, validate_proposal

    kb = load("b1_v3")                            # returns KnowledgeBase object
    ctx = build_context("b1_v3")                  # str: research_context to inject
    verdict = validate_proposal("b1_v3", proposal)# dict: allow / reject / needs_evidence
"""
from __future__ import annotations

from .loader import load, list_subjects, KnowledgeBase
from .context_builder import build_context
from .proposal_validator import validate_proposal

__all__ = [
    "load",
    "list_subjects",
    "KnowledgeBase",
    "build_context",
    "validate_proposal",
]
