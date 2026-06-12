"""Pydantic schemas for HTTP request/response contracts.

Used by the NCT and Worker FastAPI health/status endpoints.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Generic
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    status: str = "ok"


class ErrorResponse(BaseModel):
    error: str


class BalanceResponse(BaseModel):
    student_id: str
    balance: float


# ---------------------------------------------------------------------------
# NCT
# ---------------------------------------------------------------------------


class TransactionRequest(BaseModel):
    sender: str = Field(..., min_length=1, description="User sending funds")
    receiver: str = Field(..., min_length=1, description="User receiving funds")
    amount: float = Field(..., gt=0, description="Amount to transfer")
    tx_type: str = Field(..., pattern=r"^(EARN|SPEND)$", description="Transaction type: EARN or SPEND")
    concept: str = Field(..., min_length=1, max_length=128, description="Free-text concept (e.g. TP1, FOTOCOPIADORA)")


class TransactionResponse(BaseModel):
    tx_id: str = Field(..., description="SHA-256 identifier of the transaction")


class NCTStatusResponse(BaseModel):
    chain_height: int
    pending_transactions: int
    current_block: int | None = Field(
        None, description="Index of the block currently being mined, if any"
    )


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------


class WorkerHealthResponse(BaseModel):
    status: str = "ok"
    worker_id: str
    uptime_seconds: float


class WorkerStatusResponse(BaseModel):
    worker_id: str
    current_task: str | None = Field(
        None, description="task_id currently being mined, or None if idle"
    )
    tasks_processed: int
    uptime_seconds: float
