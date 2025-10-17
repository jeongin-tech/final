# ==================== 환경/클라이언트 ====================
import os
from typing import Any, Dict, List, Optional, Tuple, Union
from supabase import create_client, Client
import pandas as pd
from datetime import date

def _read_supabase_credentials():
    """
    Streamlit Cloud Secrets 우선, 환경변수 fallback
    """
    url = None
    key = None

    # 1) Streamlit secrets 읽기 (최우선)
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and "supabase" in st.secrets:
            url = st.secrets["supabase"].get("url", "").strip().rstrip("/")
            key = st.secrets["supabase"].get("key", "").strip()
            print(f"✅ Streamlit secrets에서 Supabase 설정 로드: {url[:30]}...")
    except Exception as e:
        print(f"⚠️ Streamlit secrets 읽기 실패: {e}")

    # 2) 환경변수 fallback
    if not url or not key:
        url = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
        key = os.getenv("SUPABASE_KEY", "").strip()
        if url:
            print(f"✅ 환경변수에서 Supabase 설정 로드: {url[:30]}...")

    return (url or None), (key or None)

# Supabase 클라이언트 초기화
url, key = _read_supabase_credentials()

if not url or not key:
    raise ValueError(
        "❌ Supabase 설정을 찾을 수 없습니다!\n"
        "Streamlit Cloud: secrets.toml에 [supabase] 섹션 추가\n"
        "로컬: .env 파일에 SUPABASE_URL, SUPABASE_KEY 설정"
    )

supabase: Client = create_client(url, key)
print("✅ Supabase 클라이언트 초기화 완료")

# ==================== 내부 유틸 ====================

def _apply_op(q, col: str, op: str, val: Any):
    """연산자 적용"""
    op = op.lower()
    if op == "eq":
        return q.eq(col, val)
    if op == "neq":
        return q.neq(col, val)
    if op == "gt":
        return q.gt(col, val)
    if op == "gte":
        return q.gte(col, val)
    if op == "lt":
        return q.lt(col, val)
    if op == "lte":
        return q.lte(col, val)
    if op == "like":
        return q.like(col, val)
    if op == "ilike":
        return q.ilike(col, val)
    if op == "in":
        return q.in_(col, val)
    raise ValueError(f"지원하지 않는 연산자: {op}")

def _parse_filters(q, filters: Optional[Dict[str, Any]]):
    """
    filters 지원 형태:
      - {"col": value} -> eq
      - {"col__op": value} -> op in [eq,neq,gt,gte,lt,lte,like,ilike,in]
      - {"col": ("op", value)}
    """
    if not filters:
        return q

    for key, val in filters.items():
        if isinstance(val, tuple) and len(val) == 2 and isinstance(val[0], str):
            q = _apply_op(q, key, val[0], val[1])
            continue

        if "__" in key:
            col, op = key.split("__", 1)
            q = _apply_op(q, col, op, val)
        else:
            q = q.eq(key, val)
    return q

def _apply_order(q, order_by: Optional[Union[str, Tuple[str, str]]]):
    """정렬 적용"""
    if not order_by:
        return q
    if isinstance(order_by, tuple):
        col, direction = order_by
        return q.order(col, desc=(str(direction).lower() == "desc"))
    if isinstance(order_by, str):
        if "." in order_by:
            col, direction = order_by.split(".", 1)
            return q.order(col, desc=(direction.lower() == "desc"))
        return q.order(order_by)
    return q

def _apply_pagination(q, limit: Optional[int], offset: Optional[int]):
    """페이지네이션"""
    if limit is None and offset is None:
        return q
    if limit is not None and offset is None:
        return q.range(0, max(0, limit - 1))
    if limit is None and offset is not None:
        return q.range(offset, offset + 99)
    return q.range(offset, offset + max(0, limit - 1))

# ==================== CRUD 래퍼 ====================

def select_data(
    table: str,
    columns: Union[str, List[str]] = "*",
    filters: Optional[Dict[str, Any]] = None,
    order: Optional[Union[str, Tuple[str, str]]] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    to_df: bool = False,
):
    """데이터 조회"""
    col_expr = columns if isinstance(columns, str) else ",".join(columns)
    q = supabase.table(table).select(col_expr)
    q = _parse_filters(q, filters)
    q = _apply_order(q, order)
    q = _apply_pagination(q, limit, offset)

    resp = q.execute()
    data = resp.data or []
    return pd.DataFrame(data) if to_df else data

def insert_data(table: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """데이터 삽입"""
    resp = supabase.table(table).insert(data).execute()
    rows = resp.data or []
    return rows[0] if rows else None

def update_data(
    table: str,
    match: Dict[str, Any],
    data: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """데이터 업데이트"""
    q = supabase.table(table).update(data)
    for k, v in match.items():
        if isinstance(v, tuple) and len(v) == 2 and isinstance(v[0], str):
            q = _apply_op(q, k, v[0], v[1])
        else:
            q = q.eq(k, v)
    resp = q.execute()
    return resp.data or []

def delete_data(table: str, match: Dict[str, Any]) -> List[Dict[str, Any]]:
    """데이터 삭제"""
    q = supabase.table(table).delete()
    for k, v in match.items():
        if isinstance(v, tuple) and len(v) == 2 and isinstance(v[0], str):
            q = _apply_op(q, k, v[0], v[1])
        else:
            q = q.eq(k, v)
    resp = q.execute()
    return resp.data or []

# ==================== 특화 함수 ====================

def generate_reception_number() -> str:
    """접수번호 생성 (YYYYMMDD(순번))"""
    today = date.today().strftime("%Y%m%d")
    result = select_data(
        "as_reception",
        columns=["reception_number"],
        filters={"reception_number__like": f"{today}%"},
        limit=10000,
    )
    count = len(result)
    return f"{today}({count + 1})"

def get_user_by_credentials(username: str, password: str) -> Optional[Dict[str, Any]]:
    """로그인 인증"""
    rows = select_data(
        "users",
        filters={"username": username, "password": password, "is_active": 1},
        limit=1,
    )
    return rows[0] if rows else None

def get_receptions(
    branch_id: Optional[int] = None,
    status: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    keyword: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
    to_df: bool = True,
):
    """AS 접수 조회"""
    filters: Dict[str, Any] = {}
    if branch_id is not None:
        filters["branch_id"] = branch_id
    if status:
        filters["status"] = status
    if date_from:
        filters["request_date__gte"] = date_from
    if date_to:
        filters["request_date__lte"] = date_to

    q = supabase.table("as_reception").select("*")
    q = _parse_filters(q, filters)
    q = _apply_order(q, ("created_at", "desc"))

    if keyword:
        safe = keyword.replace(",", " ")
        q = q.or_(f"customer_name.ilike.%{safe}%,phone.ilike.%{safe}%")

    q = _apply_pagination(q, limit, offset)
    resp = q.execute()
    data = resp.data or []
    return pd.DataFrame(data) if to_df else data

def log_audit(
    user_id: int,
    action: str,
    table_name: str,
    record_id: Union[int, str],
    old_value: str = "",
    new_value: str = "",
) -> Optional[Dict[str, Any]]:
    """감사 로그 기록"""
    payload = {
        "user_id": user_id,
        "action": action,
        "table_name": table_name,
        "record_id": record_id,
        "old_value": old_value,
        "new_value": new_value,
    }
    return insert_data("audit_log", payload)

# ==================== 연결 테스트 ====================

def test_connection() -> bool:
    """Supabase 연결 테스트"""
    try:
        _ = select_data("users", limit=1)
        print("✅ Supabase 연결 성공!")
        return True
    except Exception as e:
        print(f"❌ 연결 실패: {e}")
        return False

if __name__ == "__main__":
    test_connection()
