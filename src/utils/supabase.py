from __future__ import annotations

import requests
from dataclasses import dataclass, field
from typing import Any, Optional, Union

@dataclass
class APIResponse:
    data: list[Any]
    count: Optional[int] = None
    status: int = 200
    # Raw requests.Response for edge cases
    _raw: Any = field(default=None, repr=False)
    single: bool = False

    def __bool__(self) -> bool:
        return bool(self.data)
    
class QueryBuilder:
    """
    Chainable query builder.  Call .execute() to fire the request.
    """

    def __init__(self, client: "SupabaseClient", table: str) -> None:
        self._client = client
        self._table = table

        # Build state
        self._select_cols: Optional[str] = None
        self._filters: list[tuple[str, str]] = []   # [(col, "eq.value"), ...]
        self._order_col: Optional[str] = None
        self._order_desc: bool = False
        self._limit_val: Optional[int] = None
        self._offset_val: Optional[int] = None

        # Write state
        self._method: str = "GET"
        self._body: Optional[Union[dict, list]] = None
        self._upsert: bool = False
        self._returning: bool = True   # always ask Supabase to return the row(s)
        self._single: bool = False

    def select(self, *columns: str) -> "QueryBuilder":
        """
        Accepts one or more column strings, including join syntax.

        Examples::

            .select("*")
            .select("id", "name", "topics(label)")
            .select("*", "entities(text, label, confidence_score)")
        """
        self._select_cols = ", ".join(columns)
        self._method = "GET"
        return self
    
    def delete(self) -> "QueryBuilder":
        """
        DELETE row(s) based on filters.
        Note: PostgREST requires at least one filter (e.g., .eq()) 
        to perform a DELETE request on most configurations.
        """
        self._method = "DELETE"
        return self

    def eq(self, column: str, value: Any) -> "QueryBuilder":
        """column = value"""
        self._filters.append((column, f"eq.{value}"))
        return self

    def neq(self, column: str, value: Any) -> "QueryBuilder":
        """column != value"""
        self._filters.append((column, f"neq.{value}"))
        return self

    def gt(self, column: str, value: Any) -> "QueryBuilder":
        """column > value"""
        self._filters.append((column, f"gt.{value}"))
        return self

    def gte(self, column: str, value: Any) -> "QueryBuilder":
        """column >= value"""
        self._filters.append((column, f"gte.{value}"))
        return self

    def lt(self, column: str, value: Any) -> "QueryBuilder":
        """column < value"""
        self._filters.append((column, f"lt.{value}"))
        return self

    def lte(self, column: str, value: Any) -> "QueryBuilder":
        """column <= value"""
        self._filters.append((column, f"lte.{value}"))
        return self

    def like(self, column: str, pattern: str) -> "QueryBuilder":
        """column LIKE pattern  (case-sensitive)"""
        self._filters.append((column, f"like.{pattern}"))
        return self

    def ilike(self, column: str, pattern: str) -> "QueryBuilder":
        """column ILIKE pattern  (case-insensitive)"""
        self._filters.append((column, f"ilike.{pattern}"))
        return self

    def is_(self, column: str, value: Any) -> "QueryBuilder":
        """column IS value  (use for NULL checks: .is_('deleted_at', 'null'))"""
        self._filters.append((column, f"is.{value}"))
        return self

    def in_(self, column: str, values: list) -> "QueryBuilder":
        """column IN (values)"""
        joined = ",".join(str(v) for v in values)
        self._filters.append((column, f"in.({joined})"))
        return self

    def contains(self, column: str, value: Any) -> "QueryBuilder":
        """Array / JSON column @> value"""
        self._filters.append((column, f"cs.{value}"))
        return self

    def order(self, column: str, desc: bool = False) -> "QueryBuilder":
        self._order_col = column
        self._order_desc = desc
        return self

    def limit(self, n: int) -> "QueryBuilder":
        self._limit_val = n
        return self

    def offset(self, n: int) -> "QueryBuilder":
        self._offset_val = n
        return self

    def insert(self, data: Union[dict, list[dict]]) -> "QueryBuilder":
        """INSERT one row (dict) or many rows (list of dicts)."""
        self._method = "POST"
        self._body = data
        self._upsert = False
        return self
    
    def single(self) -> "QueryBuilder":
        """
        Expect exactly one row — returns a dict instead of a list.
        Sets the PostgREST header that raises an error if 0 or 2+ rows match.
        """
        self._single = True
        return self

    def upsert(
        self,
        data: Union[dict, list[dict]],
        on_conflict: Optional[str] = None,
    ) -> "QueryBuilder":
        """
        UPSERT — INSERT … ON CONFLICT DO UPDATE.

        Args:
            data: Row or list of rows to upsert.
            on_conflict: Comma-separated column name(s) for the conflict target.
                         If omitted, Supabase uses the table's primary key.
        """
        self._method = "POST"
        self._body = data
        self._upsert = True
        self._on_conflict = on_conflict
        return self

    def execute(self) -> APIResponse:
        url = f"{self._client.url}/rest/v1/{self._table}"
        headers = self._client.headers.copy()
        params: dict[str, Any] = {}

        if self._method in ("GET", "DELETE"):
            # --- columns / joins ---
            if self._select_cols and self._method == "GET":
                params["select"] = self._select_cols

            # --- filters ---
            for col, expression in self._filters:
                params[col] = expression

            # --- ordering ---
            if self._order_col and self._method == "GET":
                direction = "desc" if self._order_desc else "asc"
                params["order"] = f"{self._order_col}.{direction}"

            # --- pagination ---
            if self._limit_val is not None and self._method == "GET":
                params["limit"] = self._limit_val
            if self._offset_val is not None and self._method == "GET":
                params["offset"] = self._offset_val

            if self._single:
                headers["Accept"] = "application/vnd.pgrst.object+json"

            # Request an exact count alongside the data
            headers["Prefer"] = "count=exact"

            if self._method == "DELETE":
                headers["Prefer"] = "return=representation"
                raw = requests.delete(url, headers=headers, params=params)
            else:
                raw = requests.get(url, headers=headers, params=params)

        else:  # POST (insert / upsert)
            if self._upsert:
                conflict_clause = (
                    f"resolution=merge-duplicates,on_conflict={self._on_conflict}"
                    if getattr(self, "_on_conflict", None)
                    else "resolution=merge-duplicates"
                )
                headers["Prefer"] = f"{conflict_clause},return=representation"
            else:
                headers["Prefer"] = "return=representation"

            raw = requests.post(url, headers=headers, json=self._body)

        # --- parse ---
        if raw.status_code in (200, 201):
            body = raw.json()
            # single() → PostgREST returns a plain dict, not a list
            data = [body] if isinstance(body, dict) else body
            count = _parse_count(raw.headers.get("Content-Range"))
            return APIResponse(data=data, count=count, status=raw.status_code, _raw=raw)

        # Non-success: return empty response so callers can check truthiness
        return APIResponse(data=[], count=0, status=raw.status_code, _raw=raw)


class SupabaseClient:
    """
    Minimal Supabase REST client.

    Instantiate once and share across your codebase::

        supabase = SupabaseClient(url=SUPABASE_URL, key=SUPABASE_KEY)

    Then use the fluent builder::

        supabase.table("my_table").select("*").eq("active", True).execute()
    """

    def __init__(self, url: str, key: str) -> None:
        self.url = url.rstrip("/")
        self.key = key
        self.headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        }

    def table(self, name: str) -> QueryBuilder:
        """Entry point for all queries."""
        return QueryBuilder(client=self, table=name)

    def rpc(self, fn_name: str, params: Optional[dict] = None) -> APIResponse:
        """Call a Supabase RPC (Postgres function)."""
        url = f"{self.url}/rest/v1/rpc/{fn_name}"
        raw = requests.post(url, headers=self.headers, json=params or {})
        if raw.status_code in (200, 201):
            body = raw.json()
            data = body if isinstance(body, list) else [body]
            return APIResponse(data=data, status=raw.status_code, _raw=raw)
        return APIResponse(data=[], status=raw.status_code, _raw=raw)

def _parse_count(content_range: Optional[str]) -> Optional[int]:
    """
    Parse total row count from the Content-Range header.
    Header format:  0-24/300   →  300
    """
    if not content_range:
        return None
    try:
        return int(content_range.split("/")[-1])
    except (ValueError, IndexError):
        return None