"""
rag_runbooks.py — Retrieval-Augmented Runbook engine for WiziAgent
Drop this file next to main.py, then add:
    from rag_runbooks import router as runbook_router
    app.include_router(runbook_router)
"""

import os, glob, json, time
from pathlib import Path
from typing import List, Optional
import numpy as np

from fastapi import APIRouter
from pydantic import BaseModel

# ── Config ─────────────────────────────────────────────────────────────────────
RUNBOOKS_DIR = Path(__file__).parent / "runbooks"
CHUNK_SIZE   = 400   # characters per chunk
CHUNK_OVERLAP = 80
EMBED_MODEL  = "fast-bge-small-en"   # ~40MB ONNX, no PyTorch
TOP_K        = 4                      # chunks to retrieve

router = APIRouter(prefix="/api/runbook", tags=["runbook"])

# ── Embedder (lazy-loaded singleton) ──────────────────────────────────────────
_model  = None
_chunks : List[dict] = []   # [{text, source, heading}]
_embeds : Optional[np.ndarray] = None

def _get_model():
    global _model
    if _model is None:
        from fastembed import TextEmbedding
        _model = TextEmbedding(model_name=EMBED_MODEL)
    return _model

def _chunk_markdown(text: str, source: str) -> List[dict]:
    """Split markdown into overlapping chunks, preserving heading context."""
    lines   = text.split("\n")
    current_heading = ""
    chunks  = []
    buffer  = []
    buf_len = 0

    for line in lines:
        if line.startswith("## ") or line.startswith("# "):
            current_heading = line.lstrip("#").strip()
        buffer.append(line)
        buf_len += len(line) + 1
        if buf_len >= CHUNK_SIZE:
            chunk_text = "\n".join(buffer).strip()
            if chunk_text:
                chunks.append({"text": chunk_text, "source": source, "heading": current_heading})
            # overlap: keep last few lines
            overlap_lines = buffer[-max(1, CHUNK_OVERLAP // 40):]
            buffer  = overlap_lines
            buf_len = sum(len(l) + 1 for l in overlap_lines)

    if buffer:
        chunk_text = "\n".join(buffer).strip()
        if chunk_text:
            chunks.append({"text": chunk_text, "source": source, "heading": current_heading})

    return chunks

def _build_index():
    """Load all runbook markdown files, chunk them, embed them."""
    global _chunks, _embeds
    _chunks = []

    md_files = list(RUNBOOKS_DIR.glob("*.md"))
    if not md_files:
        return

    for f in md_files:
        text = f.read_text(encoding="utf-8")
        _chunks.extend(_chunk_markdown(text, f.stem))

    if not _chunks:
        return

    model  = _get_model()
    texts  = [c["text"] for c in _chunks]
    _embeds = np.array(list(model.embed(texts)))
    _embeds = _embeds / np.linalg.norm(_embeds, axis=1, keepdims=True)  # normalise

def _retrieve(query: str, top_k: int = TOP_K) -> List[dict]:
    """Return top_k most relevant chunks for a query."""
    if _embeds is None or len(_chunks) == 0:
        _build_index()
    if _embeds is None or len(_chunks) == 0:
        return []

    model   = _get_model()
    q_emb   = np.array(list(model.embed([query])))
    q_emb   = q_emb / np.linalg.norm(q_emb, axis=1, keepdims=True)
    scores  = (_embeds @ q_emb.T).flatten()
    top_idx = np.argsort(scores)[::-1][:top_k]

    return [
        {**_chunks[i], "score": float(scores[i])}
        for i in top_idx
        if scores[i] > 0.25   # minimum relevance threshold
    ]

# ── Build index at import time (non-blocking — happens once on startup) ────────
try:
    _build_index()
except Exception:
    pass  # will retry on first request

# ── API Models ─────────────────────────────────────────────────────────────────
class RunbookRequest(BaseModel):
    failed_checks: List[str]          # check names that failed
    workflow_name: Optional[str] = ""
    check_sql:     Optional[str] = ""  # SQL of the primary failing check

class RunbookResponse(BaseModel):
    steps:   str        # AI-composed remediation guidance
    sources: List[str]  # which runbooks were used
    raw_chunks: List[dict]  # for debug / frontend display

# ── Endpoint ───────────────────────────────────────────────────────────────────
@router.post("/suggest", response_model=RunbookResponse)
async def suggest_runbook(req: RunbookRequest):
    """
    Given a list of failed check names + optional SQL, retrieve relevant runbook
    chunks and compose step-by-step remediation via AI.
    """
    # Build retrieval query from context
    query_parts = req.failed_checks.copy()
    if req.workflow_name:
        query_parts.append(req.workflow_name)
    if req.check_sql:
        # Extract table names from SQL as hints
        import re
        tables = re.findall(r'FROM\s+(\w+\.\w+|\w+)', req.check_sql, re.IGNORECASE)
        query_parts.extend(tables[:3])
    query = " ".join(query_parts)

    chunks = _retrieve(query, top_k=TOP_K)
    sources = list({c["source"] for c in chunks})

    if not chunks:
        return RunbookResponse(
            steps="No matching runbook found for these checks. Review the check SQL directly in the Query tab and inspect the failing rows.",
            sources=[],
            raw_chunks=[]
        )

    # Compose context for AI
    context = "\n\n---\n\n".join(
        f"[{c['source']} / {c['heading']}]\n{c['text']}"
        for c in chunks
    )

    # Call AI to compose tailored steps
    import httpx, os
    api_key = os.getenv("OPENAI_API_KEY", "")

    prompt = f"""You are a data engineering assistant. Given the following runbook excerpts and a list of failed checks, compose a clear, numbered remediation plan tailored to the specific failures.

Failed checks: {', '.join(req.failed_checks)}
Workflow: {req.workflow_name or 'unknown'}

Runbook context:
{context}

Write 4-6 numbered steps. Be specific. Include any SQL from the runbook that's relevant. End with a verification step."""

    steps = ""
    if api_key:
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                r = await client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {api_key}"},
                    json={
                        "model": "gpt-4o-mini",
                        "messages": [{"role": "user", "content": prompt}],
                        "max_tokens": 500,
                        "temperature": 0.2
                    }
                )
                data = r.json()
                steps = data["choices"][0]["message"]["content"].strip()
        except Exception:
            pass

    # Fallback: return raw chunks formatted nicely (zero tokens)
    if not steps:
        steps = "\n\n".join(
            f"**{c['heading']}**\n{c['text']}"
            for c in chunks
        )

    return RunbookResponse(steps=steps, sources=sources, raw_chunks=chunks)


@router.post("/rebuild-index")
async def rebuild_index():
    """Force-rebuild the embedding index (call after adding new runbook files)."""
    _build_index()
    return {"status": "ok", "chunks": len(_chunks)}


@router.get("/list")
async def list_runbooks():
    """List available runbook files."""
    files = list(RUNBOOKS_DIR.glob("*.md"))
    return {"runbooks": [f.stem for f in files]}
