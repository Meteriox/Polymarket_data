"""
Pydantic models for API request/response validation.
"""

from typing import Any, Optional
from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    sql: str = Field(..., description="SQL query (SELECT only)")
    limit: int = Field(default=1000, ge=1, le=100000, description="Max rows to return")


class QueryResponse(BaseModel):
    columns: list[str]
    data: list[dict[str, Any]]
    row_count: int
    truncated: bool = False


class StatusResponse(BaseModel):
    status: str
    latest_block: Optional[int] = None
    table_counts: dict[str, int]
    fetcher_running: bool


class TradeQuery(BaseModel):
    market_id: Optional[str] = None
    maker: Optional[str] = None
    taker: Optional[str] = None
    min_block: Optional[int] = None
    max_block: Optional[int] = None
    limit: int = Field(default=100, ge=1, le=10000)
    offset: int = Field(default=0, ge=0)


class PricePoint(BaseModel):
    datetime: str
    block_number: int
    price: float
    usd_amount: float


class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
