"""
API routes for querying Polymarket data.
"""

import re
import logging
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

from polymarket.db.engine import get_readonly_connection
from polymarket.api.models import (
    QueryRequest, QueryResponse, StatusResponse,
    TradeQuery, ErrorResponse
)
from polymarket.state import service_state

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api")

DANGEROUS_KEYWORDS = re.compile(
    r'\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|REPLACE|MERGE|'
    r'COPY|ATTACH|DETACH|LOAD|INSTALL|EXPORT|IMPORT|PRAGMA|SET|CALL)\b',
    re.IGNORECASE
)


def _execute_query(sql: str, limit: int = 1000) -> QueryResponse:
    """Execute a read-only SQL query and return results."""
    conn = get_readonly_connection()
    try:
        result = conn.execute(sql).fetchdf()
        truncated = len(result) > limit
        if truncated:
            result = result.head(limit)

        result = result.where(result.notna(), None)

        return QueryResponse(
            columns=list(result.columns),
            data=result.to_dict(orient='records'),
            row_count=len(result),
            truncated=truncated
        )
    finally:
        conn.close()


# ─── Status ─────────────────────────────────────────────────

@router.get("/status", response_model=StatusResponse)
async def get_status():
    """Service status: latest block, table row counts, fetcher status."""
    conn = get_readonly_connection()
    try:
        tables = ['orderfilled', 'trades', 'markets', 'quant', 'users']
        counts = {}
        for t in tables:
            try:
                row = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
                counts[t] = row[0] if row else 0
            except Exception:
                counts[t] = 0

        latest_block = None
        try:
            row = conn.execute("SELECT MAX(block_number) FROM orderfilled").fetchone()
            if row and row[0] is not None:
                latest_block = int(row[0])
        except Exception:
            pass

        fetcher_running = service_state.fetcher_running

        return StatusResponse(
            status="running",
            latest_block=latest_block,
            table_counts=counts,
            fetcher_running=fetcher_running
        )
    finally:
        conn.close()


# ─── Trades ─────────────────────────────────────────────────

@router.get("/trades")
async def get_trades(
    market_id: Optional[str] = Query(None, description="Filter by market ID"),
    maker: Optional[str] = Query(None, description="Filter by maker address"),
    taker: Optional[str] = Query(None, description="Filter by taker address"),
    min_block: Optional[int] = Query(None, description="Minimum block number"),
    max_block: Optional[int] = Query(None, description="Maximum block number"),
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    """Query trades with optional filters."""
    conditions = []
    params = []

    if market_id:
        conditions.append("market_id = $1")
        params.append(market_id)
    if maker:
        conditions.append(f"maker = ${len(params) + 1}")
        params.append(maker)
    if taker:
        conditions.append(f"taker = ${len(params) + 1}")
        params.append(taker)
    if min_block is not None:
        conditions.append(f"block_number >= ${len(params) + 1}")
        params.append(min_block)
    if max_block is not None:
        conditions.append(f"block_number <= ${len(params) + 1}")
        params.append(max_block)

    where = " AND ".join(conditions) if conditions else "1=1"
    sql = f"""
        SELECT * FROM trades
        WHERE {where}
        ORDER BY block_number DESC
        LIMIT {limit} OFFSET {offset}
    """

    conn = get_readonly_connection()
    try:
        result = conn.execute(sql, params).fetchdf()
        result = result.where(result.notna(), None)
        return {
            "data": result.to_dict(orient='records'),
            "count": len(result),
            "limit": limit,
            "offset": offset
        }
    finally:
        conn.close()


# ─── Markets ────────────────────────────────────────────────

@router.get("/markets")
async def get_markets(
    search: Optional[str] = Query(None, description="Search market question"),
    active: Optional[bool] = Query(None, description="Filter by active status"),
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    """Search and list markets."""
    conditions = []
    params = []

    if search:
        conditions.append(f"question ILIKE ${len(params) + 1}")
        params.append(f"%{search}%")
    if active is not None:
        conditions.append(f"active = ${len(params) + 1}")
        params.append(active)

    where = " AND ".join(conditions) if conditions else "1=1"
    sql = f"""
        SELECT * FROM markets
        WHERE {where}
        ORDER BY created_at DESC
        LIMIT {limit} OFFSET {offset}
    """

    conn = get_readonly_connection()
    try:
        result = conn.execute(sql, params).fetchdf()
        result = result.where(result.notna(), None)
        return {
            "data": result.to_dict(orient='records'),
            "count": len(result)
        }
    finally:
        conn.close()


# ─── Market Price ───────────────────────────────────────────

@router.get("/market/{market_id}/price")
async def get_market_price(
    market_id: str,
    limit: int = Query(1000, ge=1, le=100000),
):
    """Get price history for a specific market."""
    sql = """
        SELECT datetime, block_number, price, usd_amount
        FROM quant
        WHERE market_id = $1
        ORDER BY block_number ASC
        LIMIT $2
    """
    conn = get_readonly_connection()
    try:
        result = conn.execute(sql, [market_id, limit]).fetchdf()
        if len(result) == 0:
            raise HTTPException(404, f"No data found for market {market_id}")
        result = result.where(result.notna(), None)
        return {
            "market_id": market_id,
            "data": result.to_dict(orient='records'),
            "count": len(result)
        }
    finally:
        conn.close()


# ─── User Trades ────────────────────────────────────────────

@router.get("/user/{address}/trades")
async def get_user_trades(
    address: str,
    market_id: Optional[str] = Query(None, description="Filter by market"),
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    """Get trades for a specific user address."""
    conditions = ['"user" = $1']
    params = [address]

    if market_id:
        conditions.append(f"market_id = ${len(params) + 1}")
        params.append(market_id)

    where = " AND ".join(conditions)
    sql = f"""
        SELECT * FROM users
        WHERE {where}
        ORDER BY block_number DESC
        LIMIT {limit} OFFSET {offset}
    """

    conn = get_readonly_connection()
    try:
        result = conn.execute(sql, params).fetchdf()
        result = result.where(result.notna(), None)
        return {
            "address": address,
            "data": result.to_dict(orient='records'),
            "count": len(result)
        }
    finally:
        conn.close()


# ─── Custom SQL Query ───────────────────────────────────────

@router.post("/query", response_model=QueryResponse)
async def custom_query(request: QueryRequest):
    """Execute a custom SQL query (SELECT only).

    Supports any valid DuckDB SELECT statement. Write operations are blocked.
    Results are capped at the specified limit (default 1000, max 100000).
    """
    sql = request.sql.strip().rstrip(';')

    if not sql.upper().startswith("SELECT"):
        raise HTTPException(400, "Only SELECT queries are allowed")

    if DANGEROUS_KEYWORDS.search(sql):
        raise HTTPException(400, "Query contains forbidden keywords")

    if 'LIMIT' not in sql.upper():
        sql = f"{sql} LIMIT {request.limit}"

    try:
        return _execute_query(sql, request.limit)
    except Exception as e:
        logger.error(f"Query error: {e}")
        raise HTTPException(400, f"Query execution failed: {str(e)}")
