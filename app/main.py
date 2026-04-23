"""
Insurance Risk & Security Demo — Databricks App
FastAPI backend serving fraud scoring and compliance copilot.
"""

import os
import json
import logging
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from databricks.sdk import WorkspaceClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MODEL_SERVING_ENDPOINT = os.environ.get("MODEL_SERVING_ENDPOINT", "demo-fraud-scoring")
VS_ENDPOINT = os.environ.get("VS_ENDPOINT", "demo-vs-endpoint")
CATALOG = os.environ.get("CATALOG", "users")
SCHEMA = os.environ.get("SCHEMA", "jk_wong")
VS_INDEX_NAME = f"{CATALOG}.{SCHEMA}.demo_policy_chunks_index"
LLM_ENDPOINT = os.environ.get("LLM_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")

# ---------------------------------------------------------------------------
# Databricks SDK client (uses app service principal credentials automatically)
# ---------------------------------------------------------------------------
w = WorkspaceClient()

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class ClaimInput(BaseModel):
    claim_type: str = Field(..., description="Type of claim")
    claim_amount_hkd: float = Field(..., ge=0, description="Claim amount in HKD")
    region: str = Field(..., description="Region")
    provider_type: str = Field(..., description="Provider type")
    submission_channel: str = Field(..., description="Submission channel")
    processing_days: int = Field(..., ge=1, le=90, description="Days to process")
    provider_flagged_pct: float = Field(..., ge=0, le=80, description="Provider flagged claims %")


class RiskFactor(BaseModel):
    factor: str
    impact: str
    description: str


class ScoreResponse(BaseModel):
    fraud_score: float
    risk_category: str
    risk_factors: list[RiskFactor]


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    message: str
    history: list[ChatMessage] = Field(default_factory=list)


class SourceDocument(BaseModel):
    content: str
    source: str
    score: float = 0.0


class ChatResponse(BaseModel):
    answer: str
    sources: list[SourceDocument]


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="Insurance Risk & Security Demo")


@app.get("/api/health")
def health_check() -> dict[str, str]:
    return {"status": "healthy"}


# ---------------------------------------------------------------------------
# Tab 1: Fraud Scoring
# ---------------------------------------------------------------------------

@app.post("/api/score", response_model=ScoreResponse)
def score_claim(claim: ClaimInput) -> ScoreResponse:
    """Score a claim for fraud risk using the Model Serving endpoint."""
    try:
        payload = {
            "dataframe_split": {
                "columns": [
                    "claim_type",
                    "claim_amount_hkd",
                    "region",
                    "provider_type",
                    "submission_channel",
                    "processing_days",
                    "provider_flagged_pct",
                ],
                "data": [[
                    claim.claim_type,
                    claim.claim_amount_hkd,
                    claim.region,
                    claim.provider_type,
                    claim.submission_channel,
                    claim.processing_days,
                    claim.provider_flagged_pct,
                ]],
            }
        }

        response = w.serving_endpoints.query(
            name=MODEL_SERVING_ENDPOINT,
            dataframe_split=payload["dataframe_split"],
        )

        # Parse the model response — adapt to your model's output schema
        result = _parse_model_response(response, claim)
        return result

    except Exception as e:
        logger.error(f"Model serving call failed: {e}")
        # Fallback: return a heuristic score so the demo still works
        return _heuristic_score(claim)


def _parse_model_response(response: Any, claim: ClaimInput) -> ScoreResponse:
    """Parse model serving response into ScoreResponse."""
    try:
        # The response object has a predictions attribute
        predictions = response.predictions  # type: ignore[union-attr]
        if isinstance(predictions, list) and len(predictions) > 0:
            pred = predictions[0]
            if isinstance(pred, dict):
                score = float(pred.get("fraud_score", pred.get("score", 50)))
                factors_raw = pred.get("risk_factors", [])
                risk_factors = [
                    RiskFactor(
                        factor=f.get("factor", "Unknown"),
                        impact=f.get("impact", "medium"),
                        description=f.get("description", ""),
                    )
                    for f in factors_raw
                ]
            else:
                score = float(pred)
                risk_factors = _generate_risk_factors(claim, score)
        else:
            score = float(predictions) if predictions else 50.0
            risk_factors = _generate_risk_factors(claim, score)

        score = max(0.0, min(100.0, score))
        category = _score_to_category(score)
        return ScoreResponse(
            fraud_score=round(score, 1),
            risk_category=category,
            risk_factors=risk_factors,
        )
    except Exception:
        return _heuristic_score(claim)


def _heuristic_score(claim: ClaimInput) -> ScoreResponse:
    """Generate a heuristic-based fraud score when model endpoint is unavailable."""
    score = 10.0

    # Amount-based risk
    if claim.claim_amount_hkd > 500000:
        score += 30
    elif claim.claim_amount_hkd > 200000:
        score += 20
    elif claim.claim_amount_hkd > 100000:
        score += 10

    # Provider flagged percentage
    if claim.provider_flagged_pct > 50:
        score += 25
    elif claim.provider_flagged_pct > 30:
        score += 15
    elif claim.provider_flagged_pct > 15:
        score += 8

    # Fast processing can be suspicious
    if claim.processing_days <= 2:
        score += 10
    elif claim.processing_days <= 5:
        score += 5

    # Channel risk
    if claim.submission_channel == "email":
        score += 8
    elif claim.submission_channel == "agent":
        score += 5

    # Claim type risk
    if claim.claim_type in ("travel", "property"):
        score += 5

    score = max(0.0, min(100.0, score))
    risk_factors = _generate_risk_factors(claim, score)
    category = _score_to_category(score)

    return ScoreResponse(
        fraud_score=round(score, 1),
        risk_category=category,
        risk_factors=risk_factors,
    )


def _generate_risk_factors(claim: ClaimInput, score: float) -> list[RiskFactor]:
    """Generate explanatory risk factors based on claim attributes."""
    factors: list[RiskFactor] = []

    if claim.claim_amount_hkd > 200000:
        factors.append(RiskFactor(
            factor="High Claim Amount",
            impact="high",
            description=f"Claim amount of HKD {claim.claim_amount_hkd:,.0f} exceeds the 90th percentile threshold.",
        ))
    elif claim.claim_amount_hkd > 100000:
        factors.append(RiskFactor(
            factor="Elevated Claim Amount",
            impact="medium",
            description=f"Claim amount of HKD {claim.claim_amount_hkd:,.0f} is above average for {claim.claim_type} claims.",
        ))

    if claim.provider_flagged_pct > 30:
        factors.append(RiskFactor(
            factor="Provider History",
            impact="high",
            description=f"Provider has {claim.provider_flagged_pct}% flagged claims, significantly above the 10% baseline.",
        ))
    elif claim.provider_flagged_pct > 15:
        factors.append(RiskFactor(
            factor="Provider Watch List",
            impact="medium",
            description=f"Provider flagged claims rate ({claim.provider_flagged_pct}%) is above normal range.",
        ))

    if claim.processing_days <= 3:
        factors.append(RiskFactor(
            factor="Rapid Processing",
            impact="medium",
            description=f"Claim processed in {claim.processing_days} day(s) — unusually fast turnaround.",
        ))

    if claim.submission_channel in ("email", "agent"):
        factors.append(RiskFactor(
            factor="Submission Channel",
            impact="low",
            description=f"Claims via '{claim.submission_channel}' have a higher fraud incidence rate than digital channels.",
        ))

    if not factors:
        factors.append(RiskFactor(
            factor="Standard Profile",
            impact="low",
            description="No significant risk indicators detected for this claim.",
        ))

    return factors


def _score_to_category(score: float) -> str:
    if score < 25:
        return "low"
    elif score < 50:
        return "medium"
    elif score < 75:
        return "high"
    return "critical"


# ---------------------------------------------------------------------------
# Tab 2: Compliance Copilot (RAG)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a compliance and risk policy assistant for a major insurance company in Hong Kong.
You answer questions about fraud policies, security protocols, incident response procedures, regulatory requirements, and claims documentation standards.
Use ONLY the provided context to answer. If the context does not contain enough information, say so clearly.
Be concise, professional, and cite the relevant policy sections when possible."""


@app.post("/api/chat", response_model=ChatResponse)
def chat(req: ChatRequest) -> ChatResponse:
    """RAG-powered compliance copilot: vector search + LLM."""
    try:
        # Step 1: Retrieve relevant documents from Vector Search
        sources = _vector_search(req.message)

        # Step 2: Build context from retrieved documents
        context_text = "\n\n---\n\n".join(
            [f"[Source: {s.source}]\n{s.content}" for s in sources]
        )

        # Step 3: Build messages for LLM
        messages = [{"role": "system", "content": SYSTEM_PROMPT}]
        for msg in req.history[-10:]:  # Keep last 10 turns
            messages.append({"role": msg.role, "content": msg.content})

        user_prompt = f"""Context from policy documents:

{context_text}

---

User question: {req.message}

Provide a clear, well-structured answer based on the context above. Reference specific policy sections where applicable."""

        messages.append({"role": "user", "content": user_prompt})

        # Step 4: Call Foundation Model API
        answer = _call_llm(messages)

        return ChatResponse(answer=answer, sources=sources)

    except Exception as e:
        logger.error(f"Chat endpoint failed: {e}")
        raise HTTPException(status_code=500, detail=f"Chat processing failed: {str(e)}")


def _vector_search(query: str) -> list[SourceDocument]:
    """Query Databricks Vector Search for relevant policy chunks."""
    try:
        vs_client = w.vector_search_indexes

        results = vs_client.query_index(
            index_name=VS_INDEX_NAME,
            columns=["content", "source"],
            query_text=query,
            num_results=5,
        )

        sources: list[SourceDocument] = []
        if results.result and results.result.data_array:
            for row in results.result.data_array:
                sources.append(SourceDocument(
                    content=str(row[0]) if row[0] else "",
                    source=str(row[1]) if row[1] else "Unknown",
                    score=float(row[2]) if len(row) > 2 and row[2] else 0.0,
                ))
        return sources

    except Exception as e:
        logger.warning(f"Vector search failed: {e}")
        return [SourceDocument(
            content="Vector search is not available. Please ensure the index is configured.",
            source="System",
            score=0.0,
        )]


def _call_llm(messages: list[dict[str, str]]) -> str:
    """Call Foundation Model API via the Databricks SDK."""
    try:
        response = w.serving_endpoints.query(
            name=LLM_ENDPOINT,
            messages=messages,
            max_tokens=1024,
            temperature=0.1,
        )
        # Extract the assistant's reply
        if response.choices and len(response.choices) > 0:
            return response.choices[0].message.content  # type: ignore[union-attr]
        return "I was unable to generate a response. Please try again."
    except Exception as e:
        logger.error(f"LLM call failed: {e}")
        return f"LLM endpoint unavailable: {str(e)}. Please verify that '{LLM_ENDPOINT}' is deployed."


# ---------------------------------------------------------------------------
# Static files — serve the React frontend
# ---------------------------------------------------------------------------
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/{full_path:path}")
def serve_frontend(full_path: str) -> FileResponse:
    """Serve the single-page React app for all non-API routes."""
    return FileResponse("static/index.html")
