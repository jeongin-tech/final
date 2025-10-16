import os
from dotenv import load_dotenv
from supabase import create_client, Client
import pandas as pd
from datetime import date

# 환경 변수 로드
load_dotenv()

# Supabase 클라이언트 초기화
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("⚠️ .env 파일에 SUPABASE_URL과 SUPABASE_KEY를 설정하세요!")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==================== 기본 CRUD 함수 ====================

def select_data(table: str, columns: str = '*', filters: dict = None, order_by: str = None, limit: int = None, to_df: bool = False):
    """
    데이터 조회
    
    Args:
        table: 테이블명
        columns: 조회할 컬럼 ('*' 또는 'col1,col2')
        filters: 필터 조건 {'column': 'value'} 또는 {'column__op': 'value'}
        order_by: 정렬 ('created_at.desc' 또는 'name.asc')
        limit: 조회 건수 제한
        to_df: DataFrame으로 반환 여부
    """
    query = supabase.table(table).select(columns)
    
    # 필터 적용
    if filters:
        for key, val in filters.items():
            if '__' in key:
                col, op = key.split('__')
                if op == 'gte':
                    query = query.gte(col, val)
                elif op == 'lte':
                    query = query.lte(col, val)
                elif op == 'like':
                    query = query.like(col, val)
                elif op == 'ilike':
                    query = query.ilike(col, val)
                elif op == 'in':
                    query = query.in_(col, val)
            else:
                query = query.eq(key, val)
    
    # 정렬
    if order_by:
        if '.' in order_by:
            col, direction = order_by.split('.')
            query = query.order(col, desc=(direction == 'desc'))
        else:
            query = query.order(order_by)
    
    # 제한
    if limit:
        query = query.limit(limit)
    
    response = query.execute()
    
    if to_df:
        return pd.DataFrame(response.data)
    return response.data

def insert_data(table: str, data: dict):
    """데이터 삽입"""
    response = supabase.table(table).insert(data).execute()
    return response.data[0]['id'] if response.data else None

def update_data(table: str, data: dict, match_column: str, match_value):
    """데이터 업데이트"""
    response = supabase.table(table).update(data).eq(match_column, match_value).execute()
    return response.data

def delete_data(table: str, match_column: str, match_value):
    """데이터 삭제"""
    response = supabase.table(table).delete().eq(match_column, match_value).execute()
    return response.data

def run_query_custom(table: str, filters: dict = None, order_by: str = None, limit: int = None, to_df: bool = False):
    """복잡한 쿼리용 - select_data의 별칭"""
    return select_data(table, filters=filters, order_by=order_by, limit=limit, to_df=to_df)

# ==================== 특화 함수 ====================

def generate_reception_number():
    """접수번호 생성 (YYYYMMDD(순번))"""
    today = date.today().strftime("%Y%m%d")
    result = select_data('as_reception', 
                        columns='reception_number',
                        filters={'reception_number__like': f'{today}%'})
    count = len(result)
    return f"{today}({count + 1})"

def get_user_by_credentials(username: str, password: str):
    """로그인 인증"""
    result = select_data('users', 
                        filters={'username': username, 'password': password, 'is_active': 1},
                        to_df=True)
    return result

def get_receptions(branch_id: int = None, status: str = None, date_from: str = None, date_to: str = None, 
                   keyword: str = None, limit: int = 20, offset: int = 0):
    """AS 접수 조회"""
    filters = {}
    
    if branch_id:
        filters['branch_id'] = branch_id
    if status:
        filters['status'] = status
    if date_from:
        filters['request_date__gte'] = date_from
    if date_to:
        filters['request_date__lte'] = date_to
    
    # 키워드 검색은 Supabase에서 복잡하므로 일단 전체 조회 후 pandas 필터링
    result = select_data('as_reception', 
                        filters=filters, 
                        order_by='created_at.desc',
                        to_df=True)
    
    if keyword and not result.empty:
        mask = (result['customer_name'].str.contains(keyword, na=False) | 
                result['phone'].str.contains(keyword, na=False))
        result = result[mask]
    
    # 페이지네이션
    if not result.empty:
        return result.iloc[offset:offset+limit]
    return result

def log_audit(user_id: int, action: str, table_name: str, record_id: int, old_value: str = "", new_value: str = ""):
    """감사 로그 기록"""
    data = {
        'user_id': user_id,
        'action': action,
        'table_name': table_name,
        'record_id': record_id,
        'old_value': old_value,
        'new_value': new_value
    }
    return insert_data('audit_log', data)

# ==================== 테스트 함수 ====================

def test_connection():
    """연결 테스트"""
    try:
        result = select_data('users', limit=1)
        print("✅ Supabase 연결 성공!")
        print(f"📊 users 테이블에서 {len(result)}건 조회됨")
        return True
    except Exception as e:
        print(f"❌ 연결 실패: {e}")
        return False

if __name__ == "__main__":
    # 직접 실행 시 연결 테스트
    test_connection()
