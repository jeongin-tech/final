# ==================== 환경/클라이언트 ====================
import os
from typing import Any, Dict, List, Optional, Tuple, Union
from dotenv import load_dotenv
from supabase import create_client, Client
import pandas as pd
from datetime import date

# .env 로딩: 현재 파일 기준(.py와 같은 폴더) + CWD 둘 다 시도
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(dotenv_path=os.path.join(BASE_DIR, ".env"))
load_dotenv()  # fallback

def _clean(s: Optional[str]) -> Optional[str]:
    if not s:
        return s
    s = str(s).strip()
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1].strip()
    if s.startswith("'") and s.endswith("'"):
        s = s[1:-1].strip()
    return s

def _read_supabase_credentials():
    """
    1) Streamlit secrets: [supabase].url + (service_role_key | key | anon_key)
    2) Flat secrets      : SUPABASE_URL / SUPABASE_KEY
    3) 환경변수          : SUPABASE_URL / SUPABASE_(SERVICE_ROLE_KEY|KEY|ANON_KEY)
    """
    url = None
    key = None

    # 1) 섹션 방식(가장 확실) - 지금 스크린샷 구성과 동일
    try:
        import streamlit as st
        if "supabase" in st.secrets:
            bag = st.secrets["supabase"]
            url = (bag.get("url") or bag.get("URL") or "").strip().rstrip("/")
            key = (
                bag.get("service_role_key")
                or bag.get("key")
                or bag.get("anon_key")
                or bag.get("SERVICE_ROLE_KEY")
                or bag.get("KEY")
                or bag.get("ANON_KEY")
                or ""
            ).strip()
    except Exception:
        pass

    # 2) flat secrets (있으면 보강)
    if not url or not key:
        try:
            import streamlit as st
            url = (url or st.secrets.get("SUPABASE_URL", "")).strip().rstrip("/")
            key = (key or st.secrets.get("SUPABASE_KEY", "")).strip()
        except Exception:
            pass

    # 3) 환경변수 fallback
    if not url:
        url = (os.getenv("SUPABASE_URL", "") or os.getenv("supabase_url", "")).strip().rstrip("/")
    if not key:
        key = (
            os.getenv("SUPABASE_SERVICE_ROLE_KEY")
            or os.getenv("SUPABASE_KEY")
            or os.getenv("SUPABASE_ANON_KEY")
            or ""
        ).strip()

    return (url or None), (key or None)



# ==================== 내부 유틸 ====================

# op 별로 supabase-py 메서드 매핑
def _apply_op(q, col: str, op: str, val: Any):
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
        # supabase-py는 in_ 사용
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
        # tuple 스타일: {"col": ("op", value)}
        if isinstance(val, tuple) and len(val) == 2 and isinstance(val[0], str):
            q = _apply_op(q, key, val[0], val[1])
            continue

        # "col__op" 스타일
        if "__" in key:
            col, op = key.split("__", 1)
            q = _apply_op(q, col, op, val)
        else:
            # 기본 eq
            q = q.eq(key, val)
    return q

def _apply_order(q, order_by: Optional[Union[str, Tuple[str, str]]]):
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
    """
    Supabase는 .range(from, to)로 페이지네이션
    - limit만 있으면 0 ~ limit-1
    - limit+offset 있으면 offset ~ offset+limit-1
    """
    if limit is None and offset is None:
        return q
    if limit is not None and offset is None:
        return q.range(0, max(0, limit - 1))
    if limit is None and offset is not None:
        # offset만 있는 경우는 비권장. 큰 범위를 방지하기 위해 100개 고정
        return q.range(offset, offset + 99)
    # limit and offset
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
    """
    데이터 조회

    Args:
        table: 테이블명
        columns: '*' 또는 리스트/콤마문자열
        filters: {'col': val} / {'col__op': val} / {'col': ('op', val)}
        order: 'created_at.desc' 또는 ('created_at','desc')
        limit, offset: 페이지네이션
        to_df: DataFrame 반환 여부
    """
    # columns 정규화
    col_expr = columns if isinstance(columns, str) else ",".join(columns)
    q = supabase.table(table).select(col_expr)
    q = _parse_filters(q, filters)
    q = _apply_order(q, order)
    q = _apply_pagination(q, limit, offset)

    resp = q.execute()
    data = resp.data or []
    return pd.DataFrame(data) if to_df else data

def insert_data(table: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    데이터 삽입: 첫 행(dict) 반환 (없으면 None)
    """
    resp = supabase.table(table).insert(data).execute()
    rows = resp.data or []
    return rows[0] if rows else None

def update_data(
    table: str,
    match: Dict[str, Any],
    data: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    데이터 업데이트: match dict 로 WHERE 조건 구성
    반환: 업데이트된 행 리스트
    """
    q = supabase.table(table).update(data)
    for k, v in match.items():
        # match에서도 tuple 연산자를 허용
        if isinstance(v, tuple) and len(v) == 2 and isinstance(v[0], str):
            q = _apply_op(q, k, v[0], v[1])
        else:
            q = q.eq(k, v)
    resp = q.execute()
    return resp.data or []

def delete_data(table: str, match: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    데이터 삭제: match dict 로 WHERE 조건 구성
    반환: 삭제된 행 리스트
    """
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
    # 같은 날 prefix 카운트
    result = select_data(
        "as_reception",
        columns=["reception_number"],
        filters={"reception_number__like": f"{today}%"},
        limit=10000,  # 안전버퍼
    )
    count = len(result)
    return f"{today}({count + 1})"

def get_user_by_credentials(username: str, password: str) -> Optional[Dict[str, Any]]:
    """
    로그인 인증: 단일 사용자 dict 또는 None 반환
    """
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
    """
    AS 접수 조회 (서버 필터 우선 + 키워드 or 검색)
    keyword 는 customer_name / phone 두 컬럼에 대해 ilike OR 수행
    """
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
        safe = keyword.replace(",", " ")  # or 구문 안전 처리
        # Supabase or 구문: "col1.ilike.%xxx%,col2.ilike.%xxx%"
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
    try:
        _ = select_data("users", limit=1)
        print("✅ Supabase 연결 성공!")
        return True
    except Exception as e:
        print(f"❌ 연결 실패: {e}")
        return False

if __name__ == "__main__":
    test_connection()




