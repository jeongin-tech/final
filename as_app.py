import os
import streamlit as st
from supabase import create_client

st.set_page_config(page_title="Supabase 연결체크", page_icon="🧪", layout="centered")

# 1) Secrets -> ENV 강제 주입 (가장 먼저!)
if "supabase" in st.secrets:
    bag = st.secrets["supabase"]
    os.environ["SUPABASE_URL"] = (bag.get("url") or "").strip().rstrip("/")
    os.environ["SUPABASE_KEY"] = (bag.get("service_role_key") or bag.get("key") or bag.get("anon_key") or "").strip()
else:
    os.environ["SUPABASE_URL"] = (st.secrets.get("SUPABASE_URL","") or "").strip().rstrip("/")
    os.environ["SUPABASE_KEY"] = (st.secrets.get("SUPABASE_KEY","") or "").strip()

URL = os.getenv("SUPABASE_URL")
KEY = os.getenv("SUPABASE_KEY")

st.title("🔌 Supabase 연결 체크")
st.caption(f"URL: {URL or '(empty)'}")
st.caption(f"KEY: {'set' if KEY else '(empty)'}")

if not URL or not KEY:
    st.error("자격정보가 비었습니다. Secrets의 [supabase].url/key 를 확인하세요.")
    st.stop()

# 2) 클라이언트 생성
try:
    client = create_client(URL, KEY)
    st.success("✅ 클라이언트 생성 성공")
except Exception as e:
    st.error("❌ 클라이언트 생성 실패")
    st.code(str(e)[:1000])
    st.stop()

# 3) 테스트 쿼리 (실제 존재하는 테이블명으로 조회)
st.subheader("테이블 쿼리 테스트")
table = st.text_input("테이블명", value="branch")  # 실제 있는 테이블로 바꿔도 됨
if st.button("조회"):
    try:
        data = client.table(table).select("*").limit(1).execute()
        st.success("✅ 조회 성공")
        st.code(str(data.data)[:1000])
    except Exception as e:
        st.error("❌ 조회 실패 (테이블명/권한/RLS 확인)")
        st.code(str(e)[:1000])
