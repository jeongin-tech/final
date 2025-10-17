import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, date, timedelta
from doorlock_as_init import init_db, init_master_data
import io, os

DB_PATH = "doorlock_as.db"

# ==================== 대한민국 행정구역 데이터 (시/도 → 시·군·구) ====================
KOREA_REGIONS = {
    "서울특별시": [
        "강남구","강동구","강북구","강서구","관악구","광진구","구로구","금천구","노원구","도봉구",
        "동대문구","동작구","마포구","서대문구","서초구","성동구","성북구","송파구","양천구","영등포구",
        "용산구","은평구","종로구","중구","중랑구"
    ],
    "부산광역시": [
        "강서구","금정구","기장군","남구","동구","동래구","부산진구","북구","사상구","사하구",
        "서구","수영구","연제구","영도구","중구","해운대구"
    ],
    "대구광역시": ["남구","달서구","달성군","동구","북구","서구","수성구","중구"],
    "인천광역시": ["강화군","계양구","미추홀구","남동구","동구","부평구","서구","연수구","옹진군","중구"],
    "광주광역시": ["광산구","남구","동구","북구","서구"],
    "대전광역시": ["대덕구","동구","서구","유성구","중구"],
    "울산광역시": ["남구","동구","북구","울주군","중구"],
    "세종특별자치시": ["세종시"],
    "경기도": [
        "가평군","고양시 덕양구","고양시 일산동구","고양시 일산서구","과천시","광명시","광주시","구리시",
        "군포시","김포시","남양주시","동두천시","부천시","성남시 분당구","성남시 수정구","성남시 중원구",
        "수원시 권선구","수원시 영통구","수원시 장안구","수원시 팔달구","시흥시","안산시 단원구","안산시 상록구",
        "안성시","안양시 동안구","안양시 만안구","양주시","양평군","여주시","연천군","오산시",
        "용인시 기흥구","용인시 수지구","용인시 처인구","의왕시","의정부시","이천시","파주시","평택시",
        "포천시","하남시","화성시"
    ],
    "강원특별자치도": [
        "강릉시","고성군","동해시","삼척시","속초시","양구군","양양군","영월군","원주시","인제군",
        "정선군","철원군","춘천시","태백시","평창군","홍천군","화천군","횡성군"
    ],
    "충청북도": [
        "괴산군","단양군","보은군","영동군","옥천군","음성군","제천시","증평군","진천군",
        "청주시 상당구","청주시 서원구","청주시 청원구","청주시 흥덕구","충주시"
    ],
    "충청남도": [
        "계룡시","공주시","금산군","논산시","당진시","보령시","부여군","서산시","서천군",
        "아산시","예산군","천안시 동남구","천안시 서북구","청양군","태안군","홍성군"
    ],
    "전라북도": [
        "고창군","군산시","김제시","남원시","무주군","부안군","순창군","완주군",
        "익산시","임실군","장수군","전주시 덕진구","전주시 완산구","정읍시","진안군"
    ],
    "전라남도": [
        "강진군","고흥군","곡성군","광양시","구례군","나주시","담양군","목포시","무안군",
        "보성군","순천시","신안군","여수시","영광군","영암군","완도군","장성군","장흥군",
        "진도군","함평군","해남군","화순군"
    ],
    "경상북도": [
        "경산시","경주시","고령군","구미시","군위군","김천시","문경시","봉화군","상주시",
        "성주군","안동시","영덕군","영양군","영주시","영천시","예천군","울릉군","울진군",
        "의성군","청도군","청송군","칠곡군","포항시 남구","포항시 북구"
    ],
    "경상남도": [
        "거제시","거창군","고성군","김해시","남해군","밀양시","사천시","산청군","양산시",
        "의령군","진주시","창녕군","창원시 마산합포구","창원시 마산회원구","창원시 성산구",
        "창원시 의창구","창원시 진해구","통영시","하동군","함안군","함양군","합천군"
    ],
    "제주특별자치도": ["서귀포시","제주시"]
}

# ==================== 공통 유틸 ====================
@st.cache_resource
def get_connection():
    init_db(DB_PATH)
    init_master_data(DB_PATH)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def run_query(query, params=(), to_df=False, fetch_one=False):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query, params)
    if to_df:
        if cur.description:
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=cols)
        return pd.DataFrame()
    elif fetch_one:
        return cur.fetchone()
    else:
        conn.commit()
        return cur.lastrowid

def generate_reception_number():
    today = date.today().strftime("%Y%m%d")
    result = run_query(
        "SELECT COUNT(*) as cnt FROM as_reception WHERE reception_number LIKE ?",
        (f"{today}%",),
        fetch_one=True
    )
    count = result[0] if result else 0
    return f"{today}({count + 1})"

def log_audit(user_id, action, table_name, record_id, old_value="", new_value=""):
    run_query(
        "INSERT INTO audit_log (user_id, action, table_name, record_id, old_value, new_value) VALUES (?, ?, ?, ?, ?, ?)",
        (user_id, action, table_name, record_id, old_value, new_value)
    )

def download_excel(df, filename="export.xlsx"):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    output.seek(0)
    return output

def send_sms_notification(phone, message):
    print(f"📱 SMS 발송: {phone} - {message}")
    pass

# ==================== 로그인 ====================
def init_session_state():
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
    if 'user' not in st.session_state:
        st.session_state.user = None

def login_page():
    st.title("🔐 도어락 AS 관리 시스템")
    st.subheader("로그인")
    with st.form("login_form"):
        username = st.text_input("아이디", placeholder="아이디를 입력하세요")
        password = st.text_input("비밀번호", type="password", placeholder="비밀번호를 입력하세요")
        submitted = st.form_submit_button("로그인", use_container_width=True)
        if submitted:
            user = run_query(
                "SELECT * FROM users WHERE username=? AND password=? AND is_active=1",
                (username, password),
                to_df=True
            )
            if not user.empty:
                st.session_state.logged_in = True
                st.session_state.user = user.iloc[0].to_dict()
                st.success(f"✅ {user.iloc[0]['name']}님 환영합니다!")
                st.rerun()
            else:
                st.error("❌ 아이디 또는 비밀번호가 올바르지 않습니다.")

def logout():
    st.session_state.logged_in = False
    st.session_state.user = None
    st.rerun()

# ==================== 팝업: 접수 수정 ====================
@st.dialog("접수 내역 수정", width="large")
def edit_reception_dialog(reception_number, user):
    detail = run_query("SELECT * FROM as_reception WHERE reception_number=?", (reception_number,), to_df=True)
    if detail.empty:
        st.error("해당 접수 내역을 찾을 수 없습니다.")
        return
    row = detail.iloc[0]

    st.info(f"**접수번호:** {row['reception_number']} | **등록자:** {row['registrant_name']} | **등록일:** {row['created_at']}")

    # 첨부파일 표시
    apath = (row.get('attachment_path') or "").strip()
    if apath and os.path.exists(apath):
        st.markdown("**📎 첨부파일**")
        fname = os.path.basename(apath)
        with open(apath, "rb") as f:
            st.download_button("첨부파일 다운로드", f, file_name=fname)
        ext = os.path.splitext(apath)[1].lower()
        if ext in [".png", ".jpg", ".jpeg", ".webp"]:
            st.image(apath, use_column_width=True)

    st.markdown("### 📋 고객 정보")
    cols = st.columns(3)
    e_customer = cols[0].text_input("고객명*", row['customer_name'], key="edit_customer")
    e_phone    = cols[1].text_input("전화번호*", row['phone'] or "", key="edit_phone")
    e_order    = cols[2].text_input("주문번호", row['order_number'] or "", key="edit_order")

    e_address = st.text_input("주소", row['address'] or "", key="edit_address")
    e_address_detail = st.text_input("상세주소", row['address_detail'] or "", key="edit_address_detail")

    st.markdown("### 🔧 제품 및 증상")
    cols2 = st.columns(3)
    models = run_query("SELECT model_code, model_name FROM product_model", to_df=True)
    model_options = dict(zip(models['model_code'], models['model_name'])) if not models.empty else {}
    e_model = cols2[0].selectbox("제품 모델", list(model_options.keys()) if model_options else [],
                                 index=list(model_options.keys()).index(row['model_code']) if model_options and row['model_code'] in model_options else 0,
                                 format_func=lambda x: model_options.get(x, x), key="edit_model")

    symptoms = run_query("SELECT category, code, description FROM symptom_code", to_df=True)
    symptom_categories = sorted(symptoms['category'].unique(), key=lambda x: int(x.split('.')[0]) if isinstance(x,str) and '.' in x else 999) if not symptoms.empty else []
    e_symptom_cat = cols2[1].selectbox("증상 대분류", symptom_categories,
                                       index=symptom_categories.index(row['symptom_category']) if row['symptom_category'] in symptom_categories else 0,
                                       key="edit_symptom_cat")
    filtered_symptoms = symptoms[symptoms['category'] == e_symptom_cat] if not symptoms.empty else pd.DataFrame()
    symptom_options = dict(zip(filtered_symptoms['code'], filtered_symptoms['description'])) if not filtered_symptoms.empty else {}
    e_symptom_code = cols2[2].selectbox("증상 상세", list(symptom_options.keys()) if symptom_options else [],
                                        index=list(symptom_options.keys()).index(row['symptom_code']) if symptom_options and row['symptom_code'] in symptom_options else 0,
                                        format_func=lambda x: f"{x} - {symptom_options.get(x,'')}", key="edit_symptom_code")

    st.markdown("### 📅 기타")
    cols3 = st.columns(3)
    status_options = ["접수","완료","검수완료"] if user['role']=='관리자' else ["접수","완료"]
    current_status = row['status'] if row['status'] in status_options else "접수"
    e_status = cols3[0].selectbox("상태*", status_options, index=status_options.index(current_status), key="edit_status")
    payment_options = ["무상","유상","유무상현장확인","출장비유상/부품비무상"]
    e_payment = cols3[1].selectbox("유무상", payment_options,
                                   index=payment_options.index(row['payment_type']) if row['payment_type'] in payment_options else 0,
                                   key="edit_payment")
    e_install_date = cols3[2].date_input("설치일자",
                                         value=pd.to_datetime(row['install_date']).date() if row['install_date'] else None,
                                         key="edit_install_date")
    e_detail = st.text_area("상세 내용", row['detail_content'] or "", height=100, key="edit_detail")

    cols_btn = st.columns([1,1,2])
    if cols_btn[0].button("💾 저장", type="primary", use_container_width=True):
        old_status = row['status']; complete_date = None
        if e_status == '완료' and old_status != '완료':
            complete_date = str(date.today())
        run_query("""
            UPDATE as_reception
            SET customer_name=?, phone=?, order_number=?, address=?, address_detail=?,
                model_code=?, symptom_category=?, symptom_code=?, symptom_description=?,
                detail_content=?, status=?, payment_type=?, install_date=?, complete_date=?,
                updated_at=CURRENT_TIMESTAMP
            WHERE reception_number=?
        """, (e_customer, e_phone, e_order, e_address, e_address_detail, e_model, e_symptom_cat,
              e_symptom_code, symptom_options.get(e_symptom_code, ""), e_detail, e_status, e_payment,
              str(e_install_date) if e_install_date else None, complete_date, reception_number))
        log_audit(user['id'], 'UPDATE', 'as_reception', row['id'], old_status, e_status)
        st.success("✅ 수정 완료!"); st.rerun()
    if cols_btn[1].button("❌ 취소", use_container_width=True):
        st.rerun()

# ==================== 팝업: 처리 결과 등록 ====================
@st.dialog("처리 결과 등록", width="large")
def result_registration_dialog(reception_number, user):
    detail = run_query("SELECT * FROM as_reception WHERE reception_number=?", (reception_number,), to_df=True)
    if detail.empty:
        st.error("해당 접수 내역을 찾을 수 없습니다."); return
    row = detail.iloc[0]

    st.markdown("### 📋 접수 정보")
    c1,c2,c3 = st.columns(3)
    c1.info(f"**접수번호**  \n{row['reception_number']}  \n**고객명**  \n{row['customer_name']}  \n**전화**  \n{row['phone']}")
    c2.info(f"**모델**  \n{row['model_code']}  \n**증상**  \n{(row['symptom_description'] or '')[:24]}…")
    c3.success(f"**현재 상태**  \n{row['status']}")

    # 첨부파일 표시
    apath = (row.get('attachment_path') or "").strip()
    if apath and os.path.exists(apath):
        st.markdown("**📎 첨부파일**")
        fname = os.path.basename(apath)
        with open(apath, "rb") as f:
            st.download_button("첨부파일 다운로드", f, file_name=fname)
        ext = os.path.splitext(apath)[1].lower()
        if ext in [".png",".jpg",".jpeg",".webp"]:
            st.image(apath, use_column_width=True)

    payment_type = row['payment_type'] or ""
    st.markdown("### 🔧 처리 결과 입력")
    result_text = st.text_area("처리 내용", height=100, placeholder="작업 내용을 상세히 입력하세요", key="result_text")

    st.markdown("**사용 자재**")
    materials = run_query("SELECT material_code, material_name, unit_price FROM material_code ORDER BY material_code", to_df=True)
    selected_materials = []
    for i in range(5):
        cols = st.columns([3,1,2])
        if not materials.empty:
            opt = dict(zip(materials['material_code'], materials['material_name']))
            selected_mat = cols[0].selectbox(f"자재 {i+1}", ["선택안함"] + list(opt.keys()),
                                             format_func=lambda x: opt.get(x, x), key=f"result_mat_{i}")
            if selected_mat != "선택안함":
                qty = cols[1].number_input("수량", min_value=1, value=1, key=f"result_qty_{i}")
                wholesale_price = materials.loc[materials['material_code']==selected_mat, 'unit_price'].values[0]
                display_price = 0 if payment_type in ("무상","출장비유상/부품비무상") else wholesale_price
                cols[2].metric("단가", f"{display_price:,.0f}원")
                selected_materials.append({"code": selected_mat, "name": opt[selected_mat], "qty": qty, "price": display_price})

    if selected_materials:
        total_material_cost = sum(m["price"]*m["qty"] for m in selected_materials)
        st.info(f"자재비 합계: {total_material_cost:,.0f}원")

    st.markdown("### 💰 인건비")
    c = st.columns(2)
    labor_cost = c[0].number_input("인건비(원)", min_value=0, value=0, step=10_000, key="result_labor")
    labor_reason = c[1].selectbox("인건비 사유", ["선택안함","야간1","야간2","야간3","파손","장거리1","장거리2","장거리3","주말","기타"])

    st.divider(); st.warning("⚠️ 저장 시 상태가 **'검수완료'** 로 변경됩니다. (인건비 정산 반영)")
    b1,b2,_ = st.columns([1,1,2])
    if b1.button("✅ 저장하고 완료 처리", type="primary", use_container_width=True):
        try:
            result_id = run_query("""
                INSERT INTO as_result (reception_id, technician_id, technician_name, result, labor_cost, labor_reason, completed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (int(row['id']), user['id'], user['name'], result_text, labor_cost,
                  None if labor_reason=="선택안함" else labor_reason, str(date.today())))
            for m in selected_materials:
                run_query("""
                    INSERT INTO as_material_usage (reception_id, material_code, material_name, quantity, unit_price)
                    VALUES (?, ?, ?, ?, ?)
                """, (int(row['id']), m['code'], m['name'], m['qty'], m['price']))
            run_query("UPDATE as_reception SET status='검수완료', complete_date=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                      (str(date.today()), int(row['id'])))
            log_audit(user['id'], 'INSERT', 'as_result', result_id, '', reception_number)
            st.success("✅ 처리 결과 저장 완료!"); st.balloons(); st.rerun()
        except Exception as e:
            st.error(f"❌ 저장 실패: {e}")
    if b2.button("❌ 취소", use_container_width=True):
        st.rerun()

# ==================== 메인 앱 ====================
def main_app():
    user = st.session_state.user
    role = user['role']
    branch_id = user['branch_id']

    st.sidebar.title(f"👤 {user['name']} ({role})")
    st.sidebar.caption(f"소속: {user.get('branch_name', '본사')}")
    if role == '관리자':
        menu = st.sidebar.radio("메뉴", [
            "📊 대시보드","📝 AS 접수 등록","📋 접수 내역 조회","🔧 접수 결과 등록",
            "🏢 지점 관리","📦 재고/입출고 관리","🏷️ 자재 코드 관리","💰 인건비 관리",
            "📈 품질/VOC 통계","👤 사용자 관리"
        ])
    elif role == '지점':
        menu = st.sidebar.radio("메뉴", [
            "📊 대시보드","📝 AS 접수 등록","📋 접수 내역 조회","🔧 접수 결과 등록",
            "📦 재고 관리","📈 품질 통계"
        ])
    else:
        menu = st.sidebar.radio("메뉴", ["📋 내 작업 조회","🔧 작업 결과 입력"])

    # 로그아웃
    if st.sidebar.button("🚪 로그아웃", use_container_width=True): logout()

    # 라우팅
    if   menu == "📊 대시보드":            page_dashboard(user, role, branch_id)
    elif menu == "📝 AS 접수 등록":        page_reception_register(user)
    elif menu in ("📋 접수 내역 조회","📋 내 작업 조회"):
        page_reception_list(user, role, branch_id)
    elif menu in ("🔧 접수 결과 등록","🔧 작업 결과 입력"):
        page_result_register(user, role, branch_id)
    elif menu == "🏢 지점 관리":            page_branch_manage()
    elif menu in ("📦 재고/입출고 관리","📦 재고 관리"):
        page_inventory_manage(user, role, branch_id)
    elif menu == "🏷️ 자재 코드 관리":      page_material_code_manage()
    elif menu == "💰 인건비 관리":         page_labor_cost_manage()
    elif menu in ("📈 품질/VOC 통계","📈 품질 통계"):
        page_quality_stats(role, branch_id)
    elif menu == "👤 사용자 관리":
        # 관리자 아이디(admin)만 접근 허용
        if (user.get('username') or "").lower() == "admin":
            page_user_manage()
        else:
            st.error("접속 권한이 없습니다. (관리자 전용)")

# ==================== 페이지 1: 대시보드 ====================
def page_dashboard(user, role, branch_id):
    st.title("📊 대시보드")
    where_clause = "" if role == '관리자' else f"WHERE branch_id = {branch_id}"
    c1,c2,c3,c4 = st.columns(4)
    total     = run_query(f"SELECT COUNT(*) as cnt FROM as_reception {where_clause}", to_df=True)
    waiting   = run_query(f"SELECT COUNT(*) as cnt FROM as_reception {where_clause} {'AND' if where_clause else 'WHERE'} status='접수'", to_df=True)
    complete  = run_query(f"SELECT COUNT(*) as cnt FROM as_reception {where_clause} {'AND' if where_clause else 'WHERE'} status='완료'", to_df=True)
    inspected = run_query(f"SELECT COUNT(*) as cnt FROM as_reception {where_clause} {'AND' if where_clause else 'WHERE'} status='검수완료'", to_df=True)
    c1.metric("전체 접수", f"{total['cnt'][0]}건")
    c2.metric("접수 대기", f"{waiting['cnt'][0]}건")
    c3.metric("완료",      f"{complete['cnt'][0]}건")
    c4.metric("검수완료",  f"{inspected['cnt'][0]}건")
    st.divider()
    st.subheader("🕐 최근 접수 내역")
    recent_df = run_query(f"""
        SELECT reception_number, customer_name, phone, model_code, branch_name, status, request_date
        FROM as_reception
        {where_clause}
        ORDER BY created_at DESC
        LIMIT 10
    """, to_df=True)
    if not recent_df.empty:
        st.dataframe(recent_df, use_container_width=True, hide_index=True)
    else:
        st.info("접수 내역이 없습니다.")

# ==================== 페이지 2: AS 접수 등록 ====================
def page_reception_register(user):
    st.title("📝 AS 접수 등록")

    branches = run_query("SELECT id, branch_name FROM branch", to_df=True)
    models   = run_query("SELECT model_code, model_name FROM product_model", to_df=True)
    symptoms = run_query("SELECT category, code, description FROM symptom_code ORDER BY code", to_df=True)

    if not symptoms.empty:
        def extract_number(cat):
            try: return int(cat.split('.')[0])
            except: return 999
        sorted_categories = sorted(symptoms['category'].unique(), key=extract_number)
    else:
        sorted_categories = []

    st.markdown("### 📋 고객 정보")
    cols1 = st.columns([2, 2, 2])
    customer_name = cols1[0].text_input("고객명*", placeholder="홍길동")
    phone         = cols1[1].text_input("전화번호*", placeholder="010-1234-5678")
    order_number  = cols1[2].text_input("주문번호", placeholder="선택사항")

    # ===== 주소: 시/도, 시·군·구 드롭다운 + 상세주소 자유 입력 =====
    st.markdown("### 🏠 주소")
    caddr1, caddr2, caddr3 = st.columns([2,2,6])
    sido_list = list(KOREA_REGIONS.keys())
    sel_sido = caddr1.selectbox("시/도*", options=sido_list, index=0)
    sgg_list = KOREA_REGIONS.get(sel_sido, [])
    sel_sgg  = caddr2.selectbox("시·군·구*", options=sgg_list, index=0 if sgg_list else None)
    addr_free = caddr3.text_input("상세주소(자유 입력)*", placeholder="도로명, 건물명/동·층·호 등")
    st.caption("※ 주소는 저장 시 `시/도 시·군·구 상세주소`로 합쳐집니다.")

    st.markdown("### 🔧 제품 및 증상 정보")
    cols2 = st.columns([2, 2, 2])
    model_options = dict(zip(models['model_code'], models['model_name'])) if not models.empty else {}
    selected_model = cols2[0].selectbox("제품 모델*", options=list(model_options.keys()) if model_options else [],
                                        format_func=lambda x: model_options.get(x, x))
    symptom_category = cols2[1].selectbox("증상 대분류*", options=sorted_categories)
    filtered_symptoms = symptoms[symptoms['category'] == symptom_category] if not symptoms.empty else pd.DataFrame()
    symptom_options = dict(zip(filtered_symptoms['code'], filtered_symptoms['description'])) if not filtered_symptoms.empty else {}
    if symptom_options:
        selected_symptom_code = cols2[2].selectbox("증상 상세*", options=list(symptom_options.keys()),
                                                   format_func=lambda x: f"{x} - {symptom_options[x]}")
    else:
        selected_symptom_code = None
        cols2[2].warning("증상 데이터가 없습니다")

    with st.form("reception_form"):
        cols3 = st.columns([2, 2, 2])
        install_date = cols3[0].date_input("설치일자", value=None)
        request_date = cols3[1].date_input("요청일*", value=date.today())
        payment_options = ["무상", "유상", "유무상현장확인", "출장비유상/부품비무상"]
        payment_type = cols3[2].selectbox("유무상 구분*", payment_options)

        if user['role'] == '관리자':
            branch_options = dict(zip(branches['id'], branches['branch_name'])) if not branches.empty else {}
            selected_branch = st.selectbox("담당 지점*", options=list(branch_options.keys()) if branch_options else [],
                                          format_func=lambda x: branch_options.get(x, str(x)))
        else:
            selected_branch = user['branch_id']
            st.info(f"담당 지점: {user.get('branch_name', '미지정')}")

        detail_content = st.text_area("상세 내용", placeholder="증상에 대한 추가 설명을 입력하세요", height=100)
        uploaded_file = st.file_uploader("첨부파일", type=['jpg','jpeg','png','pdf','xlsx','docx'])

        submitted = st.form_submit_button("✅ 접수 등록", use_container_width=True)
        if submitted:
            if not (customer_name and phone and addr_free and sel_sido and sel_sgg):
                st.error("❌ 고객명/전화번호/주소(시·도/시·군·구/상세)를 모두 입력하세요.")
            elif not selected_symptom_code:
                st.error("❌ 증상을 선택해주세요.")
            else:
                os.makedirs("uploads", exist_ok=True)
                attachment_path = ""
                reception_number = generate_reception_number()
                if uploaded_file:
                    fname = f"{reception_number}_{uploaded_file.name}"
                    attachment_path = os.path.join("uploads", fname)
                    with open(attachment_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())

                address = f"{sel_sido} {sel_sgg} {addr_free}".strip()
                branch_name = run_query("SELECT branch_name FROM branch WHERE id=?", (selected_branch,), fetch_one=True)
                branch_name = branch_name[0] if branch_name else ""

                rid = run_query("""
                    INSERT INTO as_reception
                    (order_number, reception_number, customer_name, phone, address, address_detail,
                     model_code, symptom_category, symptom_code, symptom_description, detail_content,
                     branch_id, branch_name, registrant_id, registrant_name, request_date, install_date,
                     status, payment_type, attachment_path)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (order_number, reception_number, customer_name, phone, address, addr_free,
                      selected_model, symptom_category, selected_symptom_code,
                      symptom_options.get(selected_symptom_code, ""),
                      detail_content, selected_branch, branch_name, user['id'], user['name'],
                      str(request_date), str(install_date) if install_date else None,
                      '접수', payment_type, attachment_path))

                log_audit(user['id'], 'INSERT', 'as_reception', rid, '', reception_number)
                branch_phone = run_query("SELECT phone FROM branch WHERE id=?", (selected_branch,), fetch_one=True)
                if branch_phone and branch_phone[0]:
                    send_sms_notification(branch_phone[0], f"[AS접수] {reception_number} - {customer_name} ({phone})")
                st.success(f"✅ 접수 등록 완료! (접수번호: {reception_number})"); st.balloons()

# ==================== 페이지 3: 접수 내역 조회 ====================
def page_reception_list(user, role, branch_id):
    st.title("📋 접수 내역 조회")
    with st.expander("🔎 검색 조건", expanded=True):
        cols = st.columns([3, 2, 2, 2])
        search_keyword = cols[0].text_input("통합검색 (고객명/전화번호)", placeholder="이름 또는 전화번호 입력")
        date_from = cols[1].date_input("시작일", value=date.today() - timedelta(days=7))
        date_to   = cols[2].date_input("종료일", value=date.today())
        status_filter = cols[3].selectbox("상태", ["전체", "접수", "완료", "검수완료"])

    page = st.number_input("페이지", min_value=1, value=1, step=1)
    per_page = 20; offset = (page - 1) * per_page

    where, params = [], []
    if role != '관리자':
        where.append("branch_id = ?"); params.append(branch_id)
    where.extend(["request_date >= ?", "request_date <= ?"]); params.extend([str(date_from), str(date_to)])
    if status_filter != "전체":
        where.append("status = ?"); params.append(status_filter)
    if search_keyword:
        where.append("(customer_name LIKE ? OR phone LIKE ?)"); params.extend([f"%{search_keyword}%", f"%{search_keyword}%"])
    where_clause = "WHERE " + " AND ".join(where) if where else ""

    total_count = run_query(f"SELECT COUNT(*) as cnt FROM as_reception {where_clause}", tuple(params), fetch_one=True)[0]
    df = run_query(f"""
        SELECT reception_number, customer_name, phone, address, model_code, 
               symptom_code, symptom_description, branch_name, status, request_date, created_at
        FROM as_reception
        {where_clause}
        ORDER BY created_at DESC
        LIMIT ? OFFSET ?
    """, tuple(params + [per_page, offset]), to_df=True)

    st.caption(f"총 {total_count}건 (현재 페이지: {page} / {(total_count + per_page - 1) // per_page})")
    if not df.empty:
        excel_data = download_excel(df)
        st.download_button(
            "📥 엑셀 다운로드",
            data=excel_data,
            file_name=f"AS접수내역_{date.today()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        st.markdown("### 📋 접수 목록 (✏️ 버튼 클릭 시 수정 팝업)")
        header_cols = st.columns([1, 2, 1.5, 1.5, 1.5, 1.5, 1, 1, 0.8])
        for col, name in zip(header_cols,
            ["접수번호","고객명","전화번호","모델","증상코드","지점","상태","요청일","수정"]):
            col.markdown(f"**{name}**")
        st.divider()
        for _, row in df.iterrows():
            cols = st.columns([1, 2, 1.5, 1.5, 1.5, 1.5, 1, 1, 0.8])
            cols[0].write(row['reception_number'])
            cols[1].write(row['customer_name'])
            cols[2].write(row['phone'])
            cols[3].write(row['model_code'])
            cols[4].write(row['symptom_code'])
            cols[5].write(row['branch_name'])
            cols[6].write(row['status'])
            cols[7].write(row['request_date'])
            if cols[8].button("✏️", key=f"edit_{row['reception_number']}"):
                edit_reception_dialog(row['reception_number'], user)
    else:
        st.info("조회된 데이터가 없습니다.")

# ==================== 페이지 4: 접수 결과 등록 ====================
def page_result_register(user, role, branch_id):
    st.title("🔧 접수 결과 등록")
    cols_search = st.columns([3, 1])
    search_keyword = cols_search[0].text_input("🔎 검색 (고객명/전화번호)", placeholder="이름 또는 전화번호 입력")
    per_page = cols_search[1].number_input("표시 건수", min_value=10, max_value=100, value=20, step=10)

    where_branch = "" if role == '관리자' else f"AND branch_id = {branch_id}"
    search_clause, params = "", []
    if search_keyword:
        search_clause = "AND (customer_name LIKE ? OR phone LIKE ?)"
        params = [f"%{search_keyword}%", f"%{search_keyword}%"]

    pending_df = run_query(f"""
        SELECT id, reception_number, customer_name, phone, model_code, symptom_description, 
               branch_name, payment_type, request_date
        FROM as_reception
        WHERE status IN ('접수', '완료') {where_branch} {search_clause}
        ORDER BY request_date ASC
        LIMIT ?
    """, tuple(params + [per_page]), to_df=True)

    st.caption(f"총 {len(pending_df)}건 표시")
    if not pending_df.empty:
        excel_data = download_excel(pending_df)
        st.download_button(
            "📥 엑셀 다운로드",
            data=excel_data,
            file_name=f"미처리접수_{date.today()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        header_cols = st.columns([1, 1.5, 1.5, 1, 2, 1.5, 1, 1, 0.8])
        for col, name in zip(header_cols, ["접수번호","고객명","전화번호","모델","증상","지점","유무상","요청일","처리"]):
            col.markdown(f"**{name}**")
        st.divider()
        for _, row in pending_df.iterrows():
            cols = st.columns([1, 1.5, 1.5, 1, 2, 1.5, 1, 1, 0.8])
            cols[0].write(row['reception_number'])
            cols[1].write(row['customer_name'])
            cols[2].write(row['phone'])
            cols[3].write(row['model_code'])
            cols[4].write((row['symptom_description'] or "")[:20] + "…")
            cols[5].write(row['branch_name'])
            cols[6].write(row['payment_type'])
            cols[7].write(row['request_date'])
            if cols[8].button("🔧", key=f"result_{row['reception_number']}"):
                result_registration_dialog(row['reception_number'], user)
    else:
        st.info("처리 대기 중인 접수가 없습니다.")

# ==================== 페이지 5: 지점 관리 ====================
def page_branch_manage():
    st.title("🏢 지점 관리")
    tab1, tab2 = st.tabs(["📋 지점 목록", "➕ 지점 추가"])
    with tab1:
        df = run_query("SELECT * FROM branch ORDER BY branch_code", to_df=True)
        if not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.divider()
            st.subheader("✏️ 지점 정보 수정")
            branch_options = dict(zip(df['id'], df['branch_name']))
            selected_branch_id = st.selectbox("수정할 지점", list(branch_options.keys()),
                                              format_func=lambda x: branch_options[x])
            edit_row = df[df['id'] == selected_branch_id].iloc[0]
            with st.form("edit_branch_form"):
                cols = st.columns(3)
                e_code = cols[0].text_input("지점코드", edit_row.get('branch_code') or "")
                e_name = cols[1].text_input("지점명", edit_row.get('branch_name') or "")
                e_manager = cols[2].text_input("담당자", edit_row.get('manager') or "")
                cols2 = st.columns(3)
                e_phone = cols2[0].text_input("연락처", edit_row.get('phone') or "")
                e_region = cols2[1].text_input("지역", edit_row.get('region') or "")
                e_billing = cols2[2].selectbox("세금 유형", ["세금계산서", "현금영수증"],
                                               index=0 if (edit_row.get('billing_type')=="세금계산서") else 1)
                e_address = st.text_input("주소", edit_row.get('address') or "")
                if st.form_submit_button("💾 수정 저장"):
                    run_query("""
                        UPDATE branch 
                        SET branch_code=?, branch_name=?, manager=?, phone=?, region=?, address=?, billing_type=?
                        WHERE id=?
                    """, (e_code, e_name, e_manager, e_phone, e_region, e_address, e_billing, selected_branch_id))
                    st.success("✅ 지점 정보가 수정되었습니다.")
                    st.rerun()
        else:
            st.info("지점 데이터가 없습니다.")
    with tab2:
        with st.form("add_branch_form"):
            cols = st.columns(3)
            branch_code = cols[0].text_input("지점코드*", placeholder="B001")
            branch_name = cols[1].text_input("지점명*", placeholder="서울지점")
            manager = cols[2].text_input("담당자", placeholder="김담당")
            cols2 = st.columns(3)
            phone = cols2[0].text_input("연락처", placeholder="02-1234-5678")
            region = cols2[1].text_input("지역", placeholder="서울/경기")
            billing_type = cols2[2].selectbox("세금 유형*", ["세금계산서", "현금영수증"])
            address = st.text_input("주소")
            st.info("💡 세금계산서: 인건비에 부가세 10% 자동 가산 | 현금영수증: 인건비 그대로")
            if st.form_submit_button("✅ 지점 추가"):
                if branch_code and branch_name:
                    run_query("""
                        INSERT INTO branch (branch_code, branch_name, manager, phone, address, region, billing_type)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (branch_code, branch_name, manager, phone, address, region, billing_type))
                    st.success("✅ 지점이 추가되었습니다.")
                    st.rerun()
                else:
                    st.error("❌ 필수 항목을 입력하세요.")

# ==================== 페이지 6: 재고/입출고 관리 ====================
def page_inventory_manage(user, role, branch_id):
    st.title("📦 재고/입출고 관리")
    if role == '관리자':
        branches = run_query("SELECT id, branch_name FROM branch", to_df=True)
        branch_options = dict(zip(branches['id'], branches['branch_name'])) if not branches.empty else {}
        if not branch_options:
            st.warning("지점 데이터가 없습니다. 먼저 지점을 추가하세요.")
            return
        selected_branch = st.selectbox("지점 선택", list(branch_options.keys()),
                                       format_func=lambda x: branch_options.get(x, str(x)))
    else:
        selected_branch = branch_id
        st.info(f"현재 지점: {user.get('branch_name', '미지정')}")

    tab1, tab2, tab3 = st.tabs(["📋 재고 현황", "📥 입고", "📤 출고"])

    # 재고 현황
    with tab1:
        stock = run_query("SELECT material_code, material_name, quantity FROM inventory WHERE branch_id=? ORDER BY material_code",
                          (selected_branch,), to_df=True)
        if stock.empty:
            st.info("재고 데이터가 없습니다.")
        else:
            st.dataframe(stock, use_container_width=True, hide_index=True)
            low = stock[stock['quantity'].fillna(0) < 5]
            if not low.empty:
                st.warning(f"⚠️ 재고 부족 품목: {len(low)}개")

    # 입고
    with tab2:
        mats = run_query("SELECT material_code, material_name FROM material_code ORDER BY material_code", to_df=True)
        if mats.empty:
            st.info("자재 코드가 없습니다. '자재 코드 관리'에서 먼저 등록하세요.")
        else:
            opt = dict(zip(mats['material_code'], mats['material_name']))
            with st.form("inbound_form"):
                code = st.selectbox("자재 선택", list(opt.keys()), format_func=lambda x: opt.get(x, x), key="in_mat")
                qty  = st.number_input("입고 수량", min_value=1, value=10, step=1, key="in_qty")
                submit = st.form_submit_button("✅ 입고 처리")
            if submit:
                current = run_query("SELECT quantity FROM inventory WHERE branch_id=? AND material_code=?",
                                    (selected_branch, code), fetch_one=True)
                before = int(current[0]) if current else 0
                after  = before + int(qty)
                if current:
                    run_query("UPDATE inventory SET quantity=? WHERE branch_id=? AND material_code=?",
                              (after, selected_branch, code))
                else:
                    run_query("INSERT INTO inventory (branch_id, material_code, material_name, quantity) VALUES (?,?,?,?)",
                              (selected_branch, code, opt.get(code, ""), after))
                run_query("""INSERT INTO inventory_log (branch_id, material_code, material_name, type, quantity, before_qty, after_qty, user_id)
                             VALUES (?, ?, ?, '입고', ?, ?, ?, ?)""",
                          (selected_branch, code, opt.get(code, ""), int(qty), before, after, user['id']))
                st.success(f"✅ {qty}개 입고 완료"); st.rerun()

    # 출고
    with tab3:
        stock = run_query("SELECT material_code, material_name, quantity FROM inventory WHERE branch_id=?",
                          (selected_branch,), to_df=True)
        if stock.empty:
            st.info("출고할 재고가 없습니다.")
        else:
            opt = dict(zip(stock['material_code'], stock['material_name']))
            with st.form("outbound_form"):
                code = st.selectbox("자재 선택", list(opt.keys()), format_func=lambda x: opt.get(x, x), key="out_mat")
                current_qty = int(stock.set_index('material_code').loc[code]['quantity'])
                qty = st.number_input("출고 수량", min_value=1, max_value=max(1, current_qty), value=1, step=1, key="out_qty")
                submit = st.form_submit_button("✅ 출고 처리")
            if submit:
                if current_qty < qty:
                    st.error("❌ 재고 부족")
                else:
                    after = current_qty - int(qty)
                    run_query("UPDATE inventory SET quantity=? WHERE branch_id=? AND material_code=?",
                              (after, selected_branch, code))
                    run_query("""INSERT INTO inventory_log (branch_id, material_code, material_name, type, quantity, before_qty, after_qty, user_id)
                                 VALUES (?, ?, ?, '출고', ?, ?, ?, ?)""",
                              (selected_branch, code, opt.get(code, ""), int(qty), current_qty, after, user['id']))
                    st.success(f"✅ {qty}개 출고 완료"); st.rerun()

# ==================== 페이지 7: 자재 코드 관리 ====================
def page_material_code_manage():
    st.title("🏷️ 자재 코드 관리")

    tab1, tab2 = st.tabs(["📋 자재 목록", "➕ 자재 추가/수정"])

    # --- 자재 목록 ---
    with tab1:
        df = run_query("SELECT material_code, material_name, unit_price FROM material_code ORDER BY material_code", to_df=True)
        if df.empty:
            st.info("등록된 자재가 없습니다. 우측 탭에서 추가하세요.")
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)

    # --- 자재 추가/수정 ---
    with tab2:
        with st.form("material_form", clear_on_submit=False):
            c = st.columns(3)
            code  = (c[0].text_input("자재코드*", placeholder="예: 00027")).strip()
            name  = (c[1].text_input("자재명*",  placeholder="예: K100 인바디")).strip()
            price = c[2].number_input("단가(원)", min_value=0, value=0, step=1000)

            save = st.form_submit_button("✅ 저장", use_container_width=True)
            if save:
                if not code or not name:
                    st.error("자재코드/자재명은 필수입니다.")
                else:
                    exists = run_query("SELECT id FROM material_code WHERE material_code=?", (code,), fetch_one=True)
                    if exists:
                        run_query("UPDATE material_code SET material_name=?, unit_price=? WHERE material_code=?",
                                  (name, int(price), code))
                        st.success("✅ 자재 정보가 수정되었습니다.")
                    else:
                        run_query("INSERT INTO material_code (material_code, material_name, unit_price) VALUES (?,?,?)",
                                  (code, name, int(price)))
                        st.success("✅ 자재가 추가되었습니다.")
                    st.rerun()

# ==================== 페이지 8: 인건비 관리 ====================
def page_labor_cost_manage():
    st.title("💰 인건비 관리")
    cols = st.columns([1, 1, 3])
    current_year = date.today().year; current_month = date.today().month
    selected_year  = cols[0].selectbox("년도", range(current_year-2, current_year+2), index=2)
    selected_month = cols[1].selectbox("월", range(1,13), index=current_month-1)
    ym = f"{selected_year}-{selected_month:02d}"
    from calendar import monthrange
    last_day = monthrange(selected_year, selected_month)[1]
    start_date, end_date = f"{ym}-01", f"{ym}-{last_day}"
    st.info(f"📅 조회 기간: {start_date} ~ {end_date} (검수완료 기준)")

    df = run_query("""
        SELECT ar.branch_id, ar.branch_name, b.billing_type,
               COUNT(DISTINCT ar.id) AS job_count,
               SUM(COALESCE(asr.labor_cost,0)) AS total_labor_cost,
               CASE WHEN b.billing_type='세금계산서' THEN SUM(COALESCE(asr.labor_cost,0))*1.1
                    ELSE SUM(COALESCE(asr.labor_cost,0)) END AS final_amount
        FROM as_reception ar
        LEFT JOIN as_result asr ON ar.id = asr.reception_id
        LEFT JOIN branch b ON ar.branch_id = b.id
        WHERE ar.status='검수완료' AND ar.complete_date>=? AND ar.complete_date<=?
        GROUP BY ar.branch_id, ar.branch_name, b.billing_type
        ORDER BY ar.branch_name
    """, (start_date, end_date), to_df=True)

    if not df.empty:
        disp = df.copy()
        disp['total_labor_cost'] = disp['total_labor_cost'].apply(lambda x: f"{x:,.0f}원")
        disp['final_amount']     = disp['final_amount'].apply(lambda x: f"{x:,.0f}원")
        disp = disp[['branch_name','billing_type','job_count','total_labor_cost','final_amount']]
        disp.columns = ['지점명','세금유형','작업건수','인건비(공급가)','최종 정산금액']
        st.dataframe(disp, use_container_width=True, hide_index=True)

        st.divider()
        c1,c2,c3 = st.columns(3)
        total_labor = df['total_labor_cost'].sum()
        total_final = df['final_amount'].sum()
        total_vat   = total_final - total_labor
        c1.metric("총 인건비 (공급가)", f"{total_labor:,.0f}원")
        c2.metric("부가세 (10%)",       f"{total_vat:,.0f}원")
        c3.metric("최종 정산 금액",     f"{total_final:,.0f}원", help="세금계산서 지점은 부가세 10% 포함")

        with st.expander("📊 지점별 세부 내역"):
            branch_choices = df[['branch_id','branch_name']].drop_duplicates().sort_values('branch_name')
            sel_branch_name = st.selectbox("지점 선택", branch_choices['branch_name'].tolist())
            sel_branch_id = int(branch_choices.loc[branch_choices['branch_name']==sel_branch_name, 'branch_id'].iloc[0])

            detail_df = run_query("""
                SELECT 
                    ar.reception_number           AS 접수번호,
                    DATE(ar.created_at)           AS 접수일자,
                    ar.complete_date              AS 처리완료일자,
                    DATE(ar.updated_at)           AS 검수일자,
                    ar.customer_name              AS 고객명,
                    COALESCE(asr.labor_cost,0)    AS 인건비,
                    COALESCE(asr.labor_reason,'') AS 인건비사유,
                    ar.symptom_description        AS 증상명
                FROM as_reception ar
                LEFT JOIN as_result asr ON asr.reception_id = ar.id
                WHERE ar.status='검수완료'
                  AND ar.complete_date >= ?
                  AND ar.complete_date <= ?
                  AND ar.branch_id = ?
                ORDER BY ar.complete_date, ar.reception_number
            """, (start_date, end_date, sel_branch_id), to_df=True)

            st.caption(f"총 {len(detail_df)}건")
            if not detail_df.empty:
                st.dataframe(detail_df, use_container_width=True, hide_index=True)
                subtotal = int(detail_df['인건비'].sum())
                st.markdown("**합계**")
                st.write(f"- 인건비 합계(공급가): {subtotal:,}원")
                _bt = df.loc[df['branch_id']==sel_branch_id, 'billing_type'].iloc[0]
                if _bt == '세금계산서':
                    st.write(f"- 부가세 포함: {int(subtotal * 1.1):,}원")
                xlsx = download_excel(detail_df, f"인건비_세부_{sel_branch_name}_{ym}.xlsx")
                st.download_button("📥 선택 지점 세부 내역 다운로드", xlsx,
                                   file_name=f"인건비_세부_{sel_branch_name}_{ym}.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            else:
                st.info("해당 기간에 검수완료된 세부 내역이 없습니다.")
    else:
        st.warning(f"⚠️ {ym}에 검수완료된 건이 없습니다.")
        st.info("💡 '검수완료' 상태로 변경된 건만 인건비에 집계됩니다.")

# ==================== 페이지 9: 품질/VOC 통계 ====================
def page_quality_stats(role, branch_id):
    st.title("📈 품질/VOC 통계")

    with st.expander("🔎 필터", expanded=True):
        # 기간 필터
        c1, c2, c3 = st.columns([2, 2, 3])
        date_from = c1.date_input("시작일", value=date.today() - timedelta(days=30))
        date_to   = c2.date_input("종료일", value=date.today())

        # 모델/증상 필터
        model_df = run_query("SELECT DISTINCT model_code FROM as_reception", to_df=True)
        models = ["전체"] + (model_df["model_code"].dropna().tolist() if not model_df.empty else [])
        model_sel = c3.selectbox("모델", models, index=0)

        sym_df = run_query("SELECT DISTINCT symptom_code, symptom_description FROM as_reception", to_df=True)
        sym_options = ["전체"]
        if not sym_df.empty:
            for _, r in sym_df.iterrows():
                code = r["symptom_code"] or ""
                desc = r["symptom_description"] or ""
                sym_options.append(f"{code} - {desc}" if code else desc)
        sym_sel = st.selectbox("증상", sym_options, index=0)

    where, params = ["request_date >= ?", "request_date <= ?"], [str(date_from), str(date_to)]
    if model_sel != "전체":
        where.append("model_code = ?"); params.append(model_sel)
    if sym_sel != "전체":
        # 앞의 코드 부분만 추출(있다면)
        sym_code = sym_sel.split(" - ")[0]
        where.append("symptom_code = ?"); params.append(sym_code)

    where_sql = "WHERE " + " AND ".join(where)
    df = run_query(f"""
        SELECT model_code, symptom_code, symptom_description, DATE(request_date) AS req_date
        FROM as_reception
        {where_sql}
    """, tuple(params), to_df=True)

    if df.empty:
        st.info("데이터가 없습니다. 기간/필터를 조정해 보세요.")
        return

    st.subheader("모델별 건수")
    model_count = df.groupby("model_code").size().reset_index(name="count").sort_values("count", ascending=False)
    st.dataframe(model_count, use_container_width=True, hide_index=True)
    try:
        st.bar_chart(model_count.set_index("model_code")["count"])
    except Exception:
        pass

    st.subheader("증상별 건수")
    sym_count = df.groupby(["symptom_code","symptom_description"]).size().reset_index(name="count")
    sym_count["증상"] = sym_count.apply(lambda r: f"{r['symptom_code']} - {r['symptom_description']}", axis=1)
    sym_count = sym_count[["증상","count"]].sort_values("count", ascending=False)
    st.dataframe(sym_count, use_container_width=True, hide_index=True)
    try:
        st.bar_chart(sym_count.set_index("증상")["count"])
    except Exception:
        pass

    st.subheader("기간별 추이(일자)")
    daily = df.groupby("req_date").size().reset_index(name="count").sort_values("req_date")
    st.dataframe(daily, use_container_width=True, hide_index=True)
    try:
        st.line_chart(daily.set_index("req_date")["count"])
    except Exception:
        pass

# ==================== 페이지 10: 사용자 관리 ====================
def page_user_manage():
    st.title("👤 사용자 관리")
    tab1, tab2 = st.tabs(["📋 사용자 목록", "➕ 사용자 추가"])

    with tab1:
        df = run_query("""
            SELECT u.id, u.username, u.name, u.role, b.branch_name, u.phone, u.is_active, u.created_at
            FROM users u
            LEFT JOIN branch b ON u.branch_id = b.id
            ORDER BY u.created_at DESC
        """, to_df=True)
        if not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.divider()
            user_id = st.number_input("비밀번호 초기화할 사용자 ID", min_value=1, step=1)
            if st.button("🔑 비밀번호 초기화 (1234로 변경)"):
                run_query("UPDATE users SET password='1234' WHERE id=?", (user_id,))
                st.success("✅ 비밀번호가 '1234'로 초기화되었습니다.")

    with tab2:
        with st.form("add_user_form"):
            username = st.text_input("아이디*", placeholder="user01")
            password = st.text_input("비밀번호*", type="password", placeholder="초기 비밀번호")
            name     = st.text_input("이름*", placeholder="홍길동")
            cols = st.columns(2)
            role = cols[0].selectbox("권한*", ["관리자", "지점", "기사"])
            branches = run_query("SELECT id, branch_name FROM branch", to_df=True)
            if not branches.empty:
                opt = {0: "선택안함"}
                opt.update(dict(zip(branches['id'], branches['branch_name'])))
                branch_id = cols[1].selectbox("소속 지점", list(opt.keys()), format_func=lambda x: opt[x])
            else:
                branch_id = 0
            phone = st.text_input("연락처", placeholder="010-1234-5678")

            if st.form_submit_button("✅ 사용자 추가"):
                if username and password and name:
                    run_query("""
                        INSERT INTO users (username, password, name, role, branch_id, phone)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (username, password, name, role, branch_id if branch_id > 0 else None, phone))
                    st.success("✅ 사용자가 추가되었습니다.")
                    st.rerun()
                else:
                    st.error("❌ 필수 항목을 입력하세요.")

# ==================== 메인 ====================
def main():
    st.set_page_config(page_title="도어락 AS 관리", page_icon="🔧", layout="wide")
    init_session_state()
    if not st.session_state.logged_in:
        login_page()
    else:
        main_app()

if __name__ == "__main__":
    main()
