"""
Supabase 연결 설정 (Streamlit Cloud 호환)
"""
import os
import streamlit as st
from supabase import create_client, Client

def get_supabase_client() -> Client:
    """
    Supabase 클라이언트 생성
    - Streamlit Cloud: st.secrets 사용
    - 로컬 개발: 환경변수 또는 하드코딩
    """
    try:
        # Streamlit Cloud: Secrets 사용
        if hasattr(st, 'secrets') and 'supabase' in st.secrets:
            url = st.secrets["supabase"]["url"]
            key = st.secrets["supabase"]["key"]
        # 환경변수 사용 (로컬)
        elif os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_KEY"):
            url = os.getenv("SUPABASE_URL")
            key = os.getenv("SUPABASE_KEY")
        else:
            raise ValueError(
                "⚠️ Supabase 설정이 필요합니다!\n"
                "Streamlit Cloud: Secrets에 supabase.url과 supabase.key 설정\n"
                "로컬: .env 파일에 SUPABASE_URL과 SUPABASE_KEY 설정"
            )
        
        return create_client(url, key)
    
    except Exception as e:
        st.error(f"❌ Supabase 연결 실패: {e}")
        st.stop()

# 전역 클라이언트 (캐시)
@st.cache_resource
def get_cached_supabase_client() -> Client:
    """캐시된 Supabase 클라이언트 반환"""
    return get_supabase_client()
