# === MAIN (1/4) – Supabase 전환 호환 패치 ===
import streamlit as st
import os
import pandas as pd
from datetime import datetime, date, timedelta
import io
from dotenv import load_dotenv

# 환경 변수 먼저 로드
load_dotenv()

# Streamlit Cloud용 임시 디렉토리 설정
if not os.path.exists("uploads"):
    os.makedirs("uploads", exist_ok=True)

# ==================== Supabase 연동 ====================
from doorlock_as_supabase import (
    supabase, 
    select_data,
    insert_data,
    update_data,
    delete_data,
    generate_reception_number,
    get_user_by_credentials,
    log_audit,
    test_connection
)

# 앱 시작 시 연결 테스트
if not test_connection():
    st.error("❌ Supabase 연결 실패! .env 파일을 확인하세요.")
    st.stop()

# ==================== 대한민국 행정구역 데이터 ====================
KOREA_REGIONS = {
    "서울특별시": ["강남구","강동구","강북구","강서구","관악구","광진구","구로구","금천구","노원구","도봉구","동대문구","동작구","마포구","서대문구","서초구","성동구","성북구","송파구","양천구","영등포구","용산구","은평구","종로구","중구","중랑구"],
    "부산광역시": ["강서구","금정구","기장군","남구","동구","동래구","부산진구","북구","사상구","사하구","서구","수영구","연제구","영도구","중구","해운대구"],
    "대구광역시": ["남구","달서구","달성군","동구","북구","서구","수성구","중구"],
    "인천광역시": ["강화군","계양구","미추홀구","남동구","동구","부평구","서구","연수구","옹진군","중구"],
    "광주광역시": ["광산구","남구","동구","북구","서구"],
    "대전광역시": ["대덕구","동구","서구","유성구","중구"],
    "울산광역시": ["남구","동구","북구","울주군","중구"],
    "세종특별자치시": ["세종시"],
    "경기도": ["가평군","고양시","과천시","광명시","광주시","구리시","군포시","김포시","남양주시","동두천시","부천시","성남시","수원시","시흥시","안산시","안성시","안양시","양주시","양평군","여주시","연천군","오산시","용인시","의왕시","의정부시","이천시","파주시","평택시","포천시","하남시","화성시"],
    "강원특별자치도": ["강릉시","고성군","동해시","삼척시","속초시","양구군","양양군","영월군","원주시","인제군","정선군","철원군","춘천시","태백시","평창군","홍천군","화천군","횡성군"],
    "충청북도": ["괴산군","단양군","보은군","영동군","옥천군","음성군","제천시","증평군","진천군","청주시","충주시"],
    "충청남도": ["계룡시","공주시","금산군","논산시","당진시","보령시","부여군","서산시","서천군","아산시","예산군","천안시","청양군","태안군","홍성군"],
    "전라북도": ["고창군","군산시","김제시","남원시","무주군","부안군","순창군","완주군","익산시","임실군","장수군","전주시","정읍시","진안군"],
    "전라남도": ["강진군","고흥군","곡성군","광양시","구례군","나주시","담양군","목포시","무안군","보성군","순천시","신안군","여수시","영광군","영암군","완도군","장성군","장흥군","진도군","함평군","해남군","화순군"],
    "경상북도": ["경산시","경주시","고령군","구미시","군위군","김천시","문경시","봉화군","상주시","성주군","안동시","영덕군","영양군","영주시","영천시","예천군","울릉군","울진군","의성군","청도군","청송군","칠곡군","포항시"],
    "경상남도": ["거제시","거창군","고성군","김해시","남해군","밀양시","사천시","산청군","양산시","의령군","진주시","창녕군","창원시","통영시","하동군","함안군","함양군","합천군"],
    "제주특별자치도": ["서귀포시","제주시"]
}

# ==================== 공통 유틸 ====================
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
            user = get_user_by_credentials(username, password)  # dict | None
            if user:
                st.session_state.logged_in = True
                st.session_state.user = user  # 이미 dict
                st.success(f"✅ {user.get('name','사용자')}님 환영합니다!")
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
    detail = select_data('as_reception', filters={'reception_number': reception_number}, to_df=True)
    if detail.empty:
        st.error("해당 접수 내역을 찾을 수 없습니다.")
        return
    row = detail.iloc[0]

    st.info(f"**접수번호:** {row['reception_number']} | **등록자:** {row.get('registrant_name','-')} | **등록일:** {row.get('created_at','')}")

    # 첨부파일 표시
    apath = (row.get('attachment_path') or "").strip()
    if apath and os.path.exists(apath):
        st.markdown("### 📎 첨부파일")
        fname = os.path.basename(apath)
        with open(apath, "rb") as f:
            st.download_button("📥 다운로드", f, file_name=fname, use_container_width=True)
        ext = os.path.splitext(apath)[1].lower()
        if ext in [".png", ".jpg", ".jpeg", ".webp", ".gif"]:
            st.image(apath, caption=fname)

    st.markdown("### 📋 고객 정보")
    cols = st.columns(3)
    e_customer = cols[0].text_input("고객명*", row['customer_name'], key="edit_customer")
    e_phone = cols[1].text_input("전화번호*", row['phone'] or "", key="edit_phone")
    e_order = cols[2].text_input("주문번호", row['order_number'] or "", key="edit_order")
    e_address = st.text_input("주소", row['address'] or "", key="edit_address")
    e_address_detail = st.text_input("상세주소", row['address_detail'] or "", key="edit_address_detail")

    st.markdown("### 🔧 제품 및 증상")
    cols2 = st.columns(3)
    models = select_data('product_model', columns='model_code,model_name', to_df=True)
    model_options = dict(zip(models['model_code'], models['model_name'])) if not models.empty else {}
    e_model = cols2[0].selectbox("제품 모델", list(model_options.keys()) if model_options else [],
                                 index=list(model_options.keys()).index(row['model_code']) if model_options and row['model_code'] in model_options else 0,
                                 format_func=lambda x: model_options.get(x, x), key="edit_model")

    symptoms = select_data('symptom_code', columns='category,code,description', order='code.asc', to_df=True)
    symptom_categories = sorted(symptoms['category'].unique()) if not symptoms.empty else []
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
        old_status = row['status']
        complete_date = None
        if e_status == '완료' and old_status != '완료':
            complete_date = str(date.today())
        
        update_dict = {
            'customer_name': e_customer,
            'phone': e_phone,
            'order_number': e_order if e_order else None,
            'address': e_address if e_address else None,
            'address_detail': e_address_detail if e_address_detail else None,
            'model_code': e_model,
            'symptom_category': e_symptom_cat,
            'symptom_code': e_symptom_code,
            'symptom_description': symptom_options.get(e_symptom_code, ""),
            'detail_content': e_detail if e_detail else None,
            'status': e_status,
            'payment_type': e_payment,
            'install_date': str(e_install_date) if e_install_date else None,
            'complete_date': complete_date
        }
        
        # ✅ 래퍼 시그니처: update_data(table, match=dict, data=dict)
        update_data('as_reception', match={'id': int(row['id'])}, data=update_dict)
        log_audit(user['id'], 'UPDATE', 'as_reception', row['id'], old_status, e_status)
        st.success("✅ 수정 완료!")
        st.rerun()
    if cols_btn[1].button("❌ 취소", use_container_width=True):
        st.rerun()

# ==================== 팝업: 처리 결과 등록 ====================
@st.dialog("처리 결과 등록", width="large")
def result_registration_dialog(reception_number, user):
    detail = select_data('as_reception', filters={'reception_number': reception_number}, to_df=True)
    if detail.empty:
        st.error("해당 접수 내역을 찾을 수 없습니다.")
        return
    row = detail.iloc[0]

    st.markdown("### 📋 접수 정보")
    c1,c2,c3 = st.columns(3)
    c1.info(f"**접수번호**\n{row['reception_number']}\n**고객명**\n{row['customer_name']}")
    c2.info(f"**모델**\n{row['model_code']}\n**증상**\n{str(row['symptom_description'])[:20]}...")
    c3.success(f"**상태**\n{row['status']}")

    payment_type = row['payment_type'] or ""
    
    # 실제 증상 수정 기능
    st.markdown("### 🩺 실제 증상 (현장 확인 후)")
    symptoms = select_data('symptom_code', columns='category,code,description', order='code.asc', to_df=True)
    symptom_categories = sorted(symptoms['category'].unique()) if not symptoms.empty else []
    
    cols_symptom = st.columns(2)
    actual_symptom_cat = cols_symptom[0].selectbox(
        "실제 증상 대분류*",
        symptom_categories,
        index=symptom_categories.index(row['symptom_category']) if row['symptom_category'] in symptom_categories else 0,
        key="actual_symptom_cat"
    )
    
    filtered_symptoms = symptoms[symptoms['category'] == actual_symptom_cat] if not symptoms.empty else pd.DataFrame()
    symptom_options = dict(zip(filtered_symptoms['code'], filtered_symptoms['description'])) if not filtered_symptoms.empty else {}
    
    if symptom_options:
        default_code = row['symptom_code'] if row['symptom_code'] in symptom_options else (list(symptom_options.keys())[0] if symptom_options else None)
        actual_symptom_code = cols_symptom[1].selectbox(
            "실제 증상 상세*",
            list(symptom_options.keys()),
            index=list(symptom_options.keys()).index(default_code) if default_code in symptom_options else 0,
            format_func=lambda x: f"{x} - {symptom_options[x]}",
            key="actual_symptom_code"
        )
        actual_symptom_desc = symptom_options.get(actual_symptom_code, "")
    else:
        actual_symptom_code = row['symptom_code']
        actual_symptom_desc = row['symptom_description']
    
    st.markdown("### 🔧 처리 결과 입력")
    result_text = st.text_area("처리 내용", height=100, placeholder="작업 내용 입력", key="result_text")

    st.markdown("**사용 자재**")
    materials = select_data('material_code', columns='material_code,material_name,unit_price', order='material_code.asc', to_df=True)
    selected_materials = []
    for i in range(5):
        cols = st.columns([3,1,2])
        if not materials.empty:
            opt = dict(zip(materials['material_code'], materials['material_name']))
            selected_mat = cols[0].selectbox(
                f"자재 {i+1}",
                ["선택안함"] + list(opt.keys()),
                format_func=lambda x: opt.get(x, x),
                key=f"result_mat_{i}"
            )
            if selected_mat != "선택안함":
                qty = cols[1].number_input("수량", min_value=1, value=1, key=f"result_qty_{i}")
                wholesale_price = materials.loc[materials['material_code']==selected_mat, 'unit_price'].values[0]
                display_price = 0 if payment_type in ("무상","출장비유상/부품비무상") else wholesale_price
                cols[2].metric("단가", f"{display_price:,.0f}원")
                selected_materials.append({"code": selected_mat, "name": opt[selected_mat], "qty": int(qty), "price": int(display_price)})

    if selected_materials:
        total_material_cost = sum(m["price"]*m["qty"] for m in selected_materials)
        st.info(f"자재비 합계: {total_material_cost:,.0f}원")

    st.markdown("### 💰 인건비")
    c = st.columns(2)
    labor_cost = c[0].number_input("인건비(원)", min_value=0, value=0, step=10_000, key="result_labor")
    labor_reason = c[1].selectbox("인건비 사유", ["선택안함","야간1","야간2","야간3","파손","장거리1","장거리2","장거리3","주말","기타"])

    st.warning("⚠️ 저장 시 상태가 '검수완료'로 변경됩니다.")
    b1,b2,_ = st.columns([1,1,2])
    if b1.button("✅ 저장", type="primary", use_container_width=True):
        try:
            result_data = {
                'reception_id': int(row['id']),
                'technician_id': user['id'],
                'technician_name': user['name'],
                'result': result_text,
                'labor_cost': int(labor_cost),
                'labor_reason': None if labor_reason=="선택안함" else labor_reason,
                'actual_symptom_category': actual_symptom_cat,
                'actual_symptom_code': actual_symptom_code,
                'actual_symptom_description': actual_symptom_desc,
                'completed_at': str(date.today())
            }
            res = insert_data('as_result', result_data)
            result_id = (res or {}).get('id')

            for m in selected_materials:
                material_data = {
                    'reception_id': int(row['id']),
                    'material_code': m['code'],
                    'material_name': m['name'],
                    'quantity': m['qty'],
                    'unit_price': m['price']
                }
                insert_data('as_material_usage', material_data)
            
            update_data('as_reception', match={'id': int(row['id'])}, data={'status': '검수완료', 'complete_date': str(date.today())})
            
            log_audit(user['id'], 'INSERT', 'as_result', result_id or 0, '', reception_number)
            st.success("✅ 저장 완료!")
            st.balloons()
            st.rerun()
        except Exception as e:
            st.error(f"❌ 저장 실패: {e}")
    if b2.button("❌ 취소", use_container_width=True):
        st.rerun()

# ==================== 메인 앱 ====================
def main_app():
    user = st.session_state.user
    role = user['role']
    branch_id = user.get('branch_id')

    st.sidebar.title(f"👤 {user['name']} ({role})")
    st.sidebar.caption(f"소속: {user.get('branch_name', '본사')}")
    st.sidebar.markdown("---")
    
    if role == '관리자':
        menu = st.sidebar.radio("메뉴", [
            "📊 대시보드",
            "📝 AS 접수 등록",
            "📋 접수 내역 조회",
            "🔧 접수 결과 등록",
            "📦 재고/입출고 관리",
            "📚 백데이터 조회",
            "─────────────",
            "📈 품질/VOC 통계",
            "🔍 검수 결과",
            "🏢 지점 관리",
            "🏷️ 자재 코드 관리",
            "💰 인건비 관리",
            "👤 사용자 관리"
        ])
        if menu == "─────────────":
            menu = "📊 대시보드"
    else:
        menu = st.sidebar.radio("메뉴", [
            "📊 대시보드",
            "📝 AS 접수 등록",
            "📋 접수 내역 조회",
            "🔧 접수 결과 등록",
            "📦 재고 관리",
            "📚 백데이터 조회"
        ])

    st.sidebar.markdown("---")
    if st.sidebar.button("🚪 로그아웃", use_container_width=True): 
        logout()

    if   menu == "📊 대시보드":            page_dashboard(user, role, branch_id)
    elif menu == "📝 AS 접수 등록":        page_reception_register(user)
    elif menu == "📋 접수 내역 조회":      page_reception_list(user, role, branch_id)
    elif menu == "🔧 접수 결과 등록":      page_result_register(user, role, branch_id)
    elif menu == "📚 백데이터 조회":       page_legacy_data(user)
    elif menu == "🔍 검수 결과":
        if role == '관리자':
            page_quality_inspection(user, role, branch_id)
        else:
            st.error("⛔ 관리자 권한이 필요합니다.")
    elif menu == "🏢 지점 관리":
        if role == '관리자':
            page_branch_manage()
        else:
            st.error("⛔ 관리자 권한이 필요합니다.")
    elif menu in ("📦 재고/입출고 관리", "📦 재고 관리"):
        page_inventory_manage(user, role, branch_id)
    elif menu == "🏷️ 자재 코드 관리":
        if role == '관리자':
            page_material_code_manage()
        else:
            st.error("⛔ 관리자 권한이 필요합니다.")
    elif menu == "💰 인건비 관리":
        if role == '관리자':
            page_labor_cost_manage()
        else:
            st.error("⛔ 관리자 권한이 필요합니다.")
    elif menu == "📈 품질/VOC 통계":
        if role == '관리자':
            page_quality_stats(role, branch_id)
        else:
            st.error("⛔ 관리자 권한이 필요합니다.")
    elif menu == "👤 사용자 관리":
        if role == '관리자':
            page_user_manage()
        else:
            st.error("⛔ 관리자 권한이 필요합니다.")

# ==================== 페이지 1: 대시보드 ====================
def page_dashboard(user, role, branch_id):
    st.title("📊 대시보드")
    
    filters = {} if role == '관리자' else {'branch_id': branch_id}
    
    c1, c2, c3, c4 = st.columns(4)
    
    total = select_data('as_reception', columns='id', filters=filters, to_df=True)
    total_count = len(total)
    
    filters_waiting = filters.copy()
    filters_waiting['status'] = '접수'
    waiting = select_data('as_reception', columns='id', filters=filters_waiting, to_df=True)
    waiting_count = len(waiting)
    
    filters_complete = filters.copy()
    filters_complete['status'] = '완료'
    complete = select_data('as_reception', columns='id', filters=filters_complete, to_df=True)
    complete_count = len(complete)
    
    filters_inspected = filters.copy()
    filters_inspected['status'] = '검수완료'
    inspected = select_data('as_reception', columns='id', filters=filters_inspected, to_df=True)
    inspected_count = len(inspected)
    
    c1.metric("전체 접수", f"{total_count}건")
    c2.metric("접수 대기", f"{waiting_count}건")
    c3.metric("완료", f"{complete_count}건")
    c4.metric("검수완료", f"{inspected_count}건")
    st.divider()
    
    st.subheader("🕐 최근 접수 내역")
    recent_df = select_data(
        'as_reception',
        columns='reception_number,customer_name,phone,model_code,symptom_code,symptom_description,branch_name,registrant_name,status,request_date',
        filters=filters,
        order='created_at.desc',
        limit=10,
        to_df=True
    )
    
    if not recent_df.empty:
        for _, row in recent_df.iterrows():
            cols = st.columns([1, 1, 1.2, 1, 2, 1, 0.8, 1])
            cols[0].write(row['reception_number'])
            cols[1].write(row['customer_name'])
            cols[2].write(row['phone'])
            cols[3].write(row['model_code'])
            desc = str(row['symptom_description']) if row['symptom_description'] is not None else ""
            cols[4].write(desc[:30] + "..." if len(desc) > 30 else desc)
            cols[5].write(row['branch_name'])
            status = row['status']
            if status == '접수':
                cols[6].info(status)
            elif status == '완료':
                cols[6].success(status)
            else:
                cols[6].write(status)
            cols[7].write(str(row['request_date']))
    else:
        st.info("접수 내역이 없습니다.")

# ==================== 페이지 2: AS 접수 등록 ====================
def page_reception_register(user):
    st.title("📝 AS 접수 등록")

    branches = select_data('branch', columns='id,branch_name', to_df=True)
    models   = select_data('product_model', columns='model_code,model_name', to_df=True)
    symptoms = select_data('symptom_code', columns='category,code,description', order='code.asc', to_df=True)

    sorted_categories = sorted(symptoms['category'].unique()) if not symptoms.empty else []

    st.markdown("### 📋 고객 정보")
    cols1 = st.columns([2, 2, 2])
    customer_name = cols1[0].text_input("고객명*", placeholder="홍길동")
    phone         = cols1[1].text_input("전화번호*", placeholder="010-1234-5678")
    order_number  = cols1[2].text_input("주문번호", placeholder="선택사항")

    st.markdown("### 🏠 주소")
    caddr1, caddr2, caddr3 = st.columns([2,2,6])
    sido_list = list(KOREA_REGIONS.keys())
    sel_sido = caddr1.selectbox("시/도*", options=sido_list, index=0)
    sgg_list = KOREA_REGIONS.get(sel_sido, [])
    sel_sgg  = caddr2.selectbox("시·군·구*", options=sgg_list, index=0 if sgg_list else None)
    addr_free = caddr3.text_input("상세주소*", placeholder="도로명, 건물명 등")

    st.markdown("### 🔧 제품 및 증상")
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
        cols2[2].warning("증상 데이터 없음")

    with st.form("reception_form"):
        cols3 = st.columns([2, 2, 2])
        install_date = cols3[0].date_input("설치일자", value=None)
        request_date = cols3[1].date_input("요청일*", value=date.today())
        payment_options = ["무상", "유상", "유무상현장확인", "출장비유상/부품비무상"]
        payment_type = cols3[2].selectbox("유무상*", payment_options)

        if user['role'] == '관리자':
            if not branches.empty:
                branch_options = dict(zip(branches['id'], branches['branch_name']))
                selected_branch = st.selectbox("담당 지점*", options=list(branch_options.keys()),
                                              format_func=lambda x: branch_options.get(x, str(x)))
            else:
                st.error("⚠️ 지점 데이터가 없습니다.")
                selected_branch = None
        else:
            selected_branch = user.get('branch_id')
            if selected_branch:
                st.info(f"담당 지점: {user.get('branch_name', '미지정')}")
            else:
                st.warning("⚠️ 소속 지점이 없습니다.")

        detail_content = st.text_area("상세 내용", placeholder="증상 추가 설명", height=100)
        uploaded_file = st.file_uploader("첨부파일", type=['jpg','jpeg','png','pdf'])

        submitted = st.form_submit_button("✅ 접수 등록", use_container_width=True)
        if submitted:
            if not (customer_name and phone and addr_free and sel_sido and sel_sgg):
                st.error("❌ 필수 항목을 입력하세요.")
            elif not selected_symptom_code:
                st.error("❌ 증상을 선택하세요.")
            elif not selected_branch:
                st.error("❌ 지점을 선택하세요.")
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
                
                if selected_branch and selected_branch > 0:
                    branch_data = select_data('branch', columns='branch_name,phone', filters={'id': int(selected_branch)})
                    branch_name = branch_data[0]['branch_name'] if branch_data else ""
                    branch_phone_value = branch_data[0]['phone'] if branch_data else None
                else:
                    branch_name = ""
                    branch_phone_value = None

                reception_data = {
                    'order_number': order_number if order_number else None,
                    'reception_number': reception_number,
                    'customer_name': customer_name,
                    'phone': phone,
                    'address': address,
                    'address_detail': addr_free,
                    'model_code': selected_model,
                    'symptom_category': symptom_category,
                    'symptom_code': selected_symptom_code,
                    'symptom_description': symptom_options.get(selected_symptom_code, ""),
                    'detail_content': detail_content if detail_content else None,
                    'branch_id': selected_branch,
                    'branch_name': branch_name,
                    'registrant_id': user['id'],
                    'registrant_name': user['name'],
                    'request_date': str(request_date),
                    'install_date': str(install_date) if install_date else None,
                    'status': '접수',
                    'payment_type': payment_type,
                    'attachment_path': attachment_path if attachment_path else None
                }
                
                res = insert_data('as_reception', reception_data)
                rid = (res or {}).get('id')
                log_audit(user['id'], 'INSERT', 'as_reception', rid or 0, '', reception_number)
                
                if branch_phone_value:
                    send_sms_notification(branch_phone_value, f"[AS접수] {reception_number}")
                
                st.success(f"✅ 접수 완료! (번호: {reception_number})")
                st.balloons()

# ==================== 페이지 3: 접수 내역 조회 ====================
def page_reception_list(user, role, branch_id):
    st.title("📋 접수 내역 조회")
    
    with st.expander("🔎 검색", expanded=True):
        cols = st.columns([3, 2, 2, 2])
        search_keyword = cols[0].text_input("통합검색", placeholder="고객명/전화번호")
        date_from = cols[1].date_input("시작일", value=date.today() - timedelta(days=7))
        date_to   = cols[2].date_input("종료일", value=date.today())
        status_filter = cols[3].selectbox("상태", ["전체", "접수", "완료", "검수완료"])

    filters = {}
    if role != '관리자':
        filters['branch_id'] = branch_id
    filters['request_date__gte'] = str(date_from)
    filters['request_date__lte'] = str(date_to)
    if status_filter != "전체":
        filters['status'] = status_filter
    
    all_data = select_data(
        'as_reception', 
        columns='reception_number,customer_name,phone,model_code,symptom_code,symptom_description,branch_name,status,request_date',
        filters=filters,
        order=('created_at','desc'),
        to_df=True
    )
    
    if search_keyword and not all_data.empty:
        mask = (all_data['customer_name'].str.contains(search_keyword, na=False) | 
                all_data['phone'].str.contains(search_keyword, na=False))
        all_data = all_data[mask]
    
    st.caption(f"총 {len(all_data)}건")
    
    if not all_data.empty:
        excel_data = download_excel(all_data)
        st.download_button("📥 엑셀", excel_data, file_name=f"접수내역_{date.today()}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        
        for _, row in all_data.head(50).iterrows():
            cols = st.columns([1, 1, 1.2, 1, 2, 1, 0.8, 1, 0.5])
            cols[0].write(row['reception_number'])
            cols[1].write(row['customer_name'])
            cols[2].write(row['phone'])
            cols[3].write(row['model_code'])
            cols[4].write(str(row['symptom_description'])[:30] + "...")
            cols[5].write(row['branch_name'])
            status = row['status']
            if status == '접수':
                cols[6].info(status)
            elif status == '완료':
                cols[6].success(status)
            else:
                cols[6].write(status)
            cols[7].write(str(row['request_date']))
            if cols[8].button("✏️", key=f"edit_{row['reception_number']}"):
                edit_reception_dialog(row['reception_number'], user)
    else:
        st.info("조회된 데이터가 없습니다.")

# ==================== 페이지 4: 접수 결과 등록 ====================
def page_result_register(user, role, branch_id):
    st.title("🔧 접수 결과 등록")
    
    filters = {} if role == '관리자' else {'branch_id': branch_id}
    
    pending_waiting = select_data(
        'as_reception', 
        columns='id,reception_number,customer_name,phone,model_code,symptom_description,payment_type',
        filters={**filters, 'status': '접수'},
        order=('request_date','asc'),
        limit=20,
        to_df=True
    )
    
    pending_complete = select_data(
        'as_reception', 
        columns='id,reception_number,customer_name,phone,model_code,symptom_description,payment_type',
        filters={**filters, 'status': '완료'},
        order=('request_date','asc'),
        limit=20,
        to_df=True
    )
    
    pending_df = pd.concat([pending_waiting, pending_complete], ignore_index=True) if not pending_waiting.empty or not pending_complete.empty else pd.DataFrame()
    
    if not pending_df.empty:
        for _, row in pending_df.iterrows():
            cols = st.columns([1, 1, 1.2, 1, 2, 1, 0.5])
            cols[0].write(row['reception_number'])
            cols[1].write(row['customer_name'])
            cols[2].write(row['phone'])
            cols[3].write(row['model_code'])
            cols[4].write(str(row['symptom_description'])[:30])
            cols[5].write(row['payment_type'])
            if cols[6].button("🔧", key=f"result_{row['reception_number']}"):
                result_registration_dialog(row['reception_number'], user)
    else:
        st.info("처리 대기 중인 접수가 없습니다.")

# ==================== 페이지 5: 지점 관리 ====================
def page_branch_manage():
    st.title("🏢 지점 관리")
    tab1, tab2 = st.tabs(["📋 지점 목록", "➕ 지점 추가"])
    
    with tab1:
        df = select_data('branch', order=('branch_code','asc'), to_df=True)
        if not df.empty:
            st.dataframe(df, use_container_width=True, hide_index=True)
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
            region = cols2[1].text_input("지역", placeholder="서울")
            billing_type = cols2[2].selectbox("세금 유형*", ["세금계산서", "현금영수증"])
            address = st.text_input("주소")
            if st.form_submit_button("✅ 추가"):
                if branch_code and branch_name:
                    branch_data = {
                        'branch_code': branch_code,
                        'branch_name': branch_name,
                        'manager': manager if manager else None,
                        'phone': phone if phone else None,
                        'address': address if address else None,
                        'region': region if region else None,
                        'billing_type': billing_type
                    }
                    insert_data('branch', branch_data)
                    st.success("✅ 지점이 추가되었습니다.")
                    st.rerun()
                else:
                    st.error("❌ 필수 항목을 입력하세요.")

# ==================== 페이지 6: 재고/입출고 관리 ====================
def page_inventory_manage(user, role, branch_id):
    st.title("📦 재고/입출고 관리")
    
    tab1, tab2 = st.tabs(["📊 현재 재고", "📝 입출고 등록"])
    
    with tab1:
        st.subheader("현재 재고 현황")
        filters = {} if role == '관리자' else {'branch_id': branch_id}
        inventory = select_data(
            'inventory', 
            columns='id,material_code,material_name,branch_id,branch_name,quantity',
            filters=filters,
            order=('material_code','asc'),
            to_df=True
        )
        
        if not inventory.empty:
            st.dataframe(inventory, use_container_width=True, hide_index=True)
        else:
            st.info("재고 데이터가 없습니다.")
    
    with tab2:
        st.subheader("입출고 등록")
        with st.form("inventory_form"):
            cols = st.columns(3)
            
            materials = select_data('material_code', columns='material_code,material_name', to_df=True)
            material_options = dict(zip(materials['material_code'], materials['material_name'])) if not materials.empty else {}
            
            selected_material = cols[0].selectbox(
                "자재*", 
                list(material_options.keys()) if material_options else [],
                format_func=lambda x: f"{x} - {material_options.get(x, x)}"
            )
            
            transaction_type = cols[1].selectbox("거래 유형*", ["입고", "출고"])
            quantity = cols[2].number_input("수량*", min_value=1, value=1)
            
            if role == '관리자':
                branches = select_data('branch', columns='id,branch_name', to_df=True)
                branch_options = dict(zip(branches['id'], branches['branch_name'])) if not branches.empty else {}
                selected_branch = st.selectbox(
                    "지점*", 
                    list(branch_options.keys()) if branch_options else [],
                    format_func=lambda x: branch_options.get(x, str(x))
                )
            else:
                selected_branch = branch_id
                st.info(f"지점: {user.get('branch_name', '미지정')}")
            
            memo = st.text_area("메모")
            
            if st.form_submit_button("✅ 등록"):
                if selected_material and selected_branch:
                    try:
                        # 재고 조회
                        current_inventory = select_data(
                            'inventory',
                            columns='id,quantity',
                            filters={'material_code': selected_material, 'branch_id': selected_branch}
                        )
                        
                        if transaction_type == "입고":
                            new_quantity = (current_inventory[0]['quantity'] if current_inventory else 0) + quantity
                        else:  # 출고
                            current_qty = current_inventory[0]['quantity'] if current_inventory else 0
                            if current_qty < quantity:
                                st.error(f"❌ 재고가 부족합니다. (현재: {current_qty})")
                                st.stop()
                            new_quantity = current_qty - quantity
                        
                        # 재고 업데이트
                        if current_inventory:
                            update_data('inventory', match={'id': current_inventory[0]['id']}, data={'quantity': new_quantity})
                        else:
                            branch_data = select_data('branch', columns='branch_name', filters={'id': selected_branch})
                            branch_name = branch_data[0]['branch_name'] if branch_data else ""
                            insert_data('inventory', {
                                'material_code': selected_material,
                                'material_name': material_options.get(selected_material, ""),
                                'branch_id': selected_branch,
                                'branch_name': branch_name,
                                'quantity': new_quantity
                            })
                        
                        # 입출고 로그
                        insert_data('inventory_log', {
                            'material_code': selected_material,
                            'material_name': material_options.get(selected_material, ""),
                            'branch_id': selected_branch,
                            'transaction_type': transaction_type,
                            'quantity': quantity,
                            'memo': memo if memo else None,
                            'user_id': user['id'],
                            'user_name': user['name']
                        })
                        
                        st.success(f"✅ {transaction_type} 완료!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ 등록 실패: {e}")
                else:
                    st.error("❌ 필수 항목을 입력하세요.")

# ==================== 페이지 7: 자재 코드 관리 ====================
def page_material_code_manage():
    st.title("🏷️ 자재 코드 관리")
    
    tab1, tab2 = st.tabs(["📋 자재 목록", "➕ 자재 추가/수정"])
    
    with tab1:
        materials = select_data(
            'material_code', 
            columns='material_code,material_name,unit_price',
            order=('material_code','asc'),
            to_df=True
        )
        
        if materials.empty:
            st.info("등록된 자재가 없습니다.")
        else:
            materials.columns = ['자재코드', '자재명', '단가(원)']
            st.dataframe(materials, use_container_width=True, hide_index=True)
    
    with tab2:
        with st.form("material_form"):
            c = st.columns(3)
            code  = c[0].text_input("자재코드*", placeholder="00027")
            name  = c[1].text_input("자재명*",  placeholder="K100 인바디")
            price = c[2].number_input("단가(원)", min_value=0, value=0, step=1000)

            if st.form_submit_button("✅ 저장", use_container_width=True):
                if not code or not name:
                    st.error("필수 항목을 입력하세요.")
                else:
                    exists = select_data('material_code', columns='id', filters={'material_code': code})
                    if exists:
                        # ✅ 래퍼 시그니처: update_data(table, match=dict, data=dict)
                        update_data('material_code', match={'material_code': code}, data={'material_name': name, 'unit_price': int(price)})
                        st.success("✅ 수정되었습니다.")
                    else:
                        insert_data('material_code', {'material_code': code, 'material_name': name, 'unit_price': int(price)})
                        st.success("✅ 추가되었습니다.")
                    st.rerun()

# ==================== 페이지 8: 인건비 관리 ====================
def page_labor_cost_manage():
    st.title("💰 인건비 관리")
    
    tab1, tab2 = st.tabs(["📊 인건비 통계", "📋 인건비 내역"])
    
    with tab1:
        st.subheader("기간별 인건비 통계")
        cols = st.columns(2)
        date_from = cols[0].date_input("시작일", value=date.today() - timedelta(days=30))
        date_to = cols[1].date_input("종료일", value=date.today())
        
        # as_result에서 인건비 데이터 조회
        results = select_data(
            'as_result',
            columns='technician_name,labor_cost,labor_reason,completed_at',
            filters={
                'completed_at__gte': str(date_from),
                'completed_at__lte': str(date_to)
            },
            to_df=True
        )
        
        if not results.empty:
            # 숫자형 안정화
            if 'labor_cost' in results.columns:
                results['labor_cost'] = pd.to_numeric(results['labor_cost'], errors='coerce').fillna(0).astype(int)

            total_labor = results['labor_cost'].sum()
            st.metric("총 인건비", f"{total_labor:,.0f}원")
            
            # 기사별 집계
            tech_stats = results.groupby('technician_name', dropna=False)['labor_cost'].agg(['sum', 'count']).reset_index()
            tech_stats.columns = ['기사명', '총 인건비', '건수']
            st.dataframe(tech_stats, use_container_width=True, hide_index=True)
            
            # 사유별 집계
            if 'labor_reason' in results.columns:
                reason_stats = results[results['labor_reason'].notna()].groupby('labor_reason')['labor_cost'].agg(['sum', 'count']).reset_index()
                reason_stats.columns = ['사유', '총 인건비', '건수']
                st.subheader("사유별 인건비")
                st.dataframe(reason_stats, use_container_width=True, hide_index=True)
        else:
            st.info("해당 기간의 인건비 데이터가 없습니다.")
    
    with tab2:
        st.subheader("인건비 상세 내역")
        cols = st.columns(2)
        date_from2 = cols[0].date_input("시작일", value=date.today() - timedelta(days=7), key="labor_detail_from")
        date_to2 = cols[1].date_input("종료일", value=date.today(), key="labor_detail_to")
        
        # 상세 내역 조회 (reception_id로 접수정보 조인 → 배치 조회로 N+1 제거)
        results_detail = select_data(
            'as_result',
            columns='reception_id,technician_name,labor_cost,labor_reason,completed_at',
            filters={
                'completed_at__gte': str(date_from2),
                'completed_at__lte': str(date_to2)
            },
            order=('completed_at','desc'),
            to_df=True
        )
        
        if not results_detail.empty:
            # 숫자형 안정화
            results_detail['labor_cost'] = pd.to_numeric(results_detail['labor_cost'], errors='coerce').fillna(0).astype(int)

            # ✅ reception_id 배치 조회
            rec_ids = sorted(set(int(r) for r in results_detail['reception_id'].dropna().tolist()))
            rec_map = {}
            if rec_ids:
                # Supabase in 연산자 사용
                rec_rows = select_data(
                    'as_reception',
                    columns='id,reception_number,customer_name',
                    filters={'id': ('in', rec_ids)}
                )
                rec_map = {r['id']: r for r in (rec_rows or [])}

            # 표 출력
            header = st.columns([1, 1, 1, 1, 1])
            header[0].markdown("**접수번호**")
            header[1].markdown("**고객명**")
            header[2].markdown("**기사명**")
            header[3].markdown("**인건비**")
            header[4].markdown("**사유**")

            for _, row in results_detail.iterrows():
                rec = rec_map.get(int(row['reception_id'])) if pd.notna(row['reception_id']) else None
                cols = st.columns([1, 1, 1, 1, 1])
                cols[0].write(rec['reception_number'] if rec else "-")
                cols[1].write(rec['customer_name'] if rec else "-")
                cols[2].write(row.get('technician_name', '-'))
                cols[3].write(f"{int(row['labor_cost']):,}원")
                cols[4].write(row.get('labor_reason') or "-")
        else:
            st.info("해당 기간의 내역이 없습니다.")

# ==================== 페이지 9: 품질/VOC 통계 ====================
def page_quality_stats(role, branch_id):
    st.title("📈 품질/VOC 통계")
    
    tab1, tab2, tab3 = st.tabs(["📊 증상별 통계", "📉 기간별 추이", "🔍 상세 분석"])
    
    with tab1:
        st.subheader("증상별 접수 통계")
        cols = st.columns(2)
        date_from = cols[0].date_input("시작일", value=date.today() - timedelta(days=30))
        date_to = cols[1].date_input("종료일", value=date.today())
        
        filters = {
            'request_date__gte': str(date_from),
            'request_date__lte': str(date_to)
        }
        if role != '관리자':
            filters['branch_id'] = branch_id
        
        receptions = select_data(
            'as_reception',
            columns='symptom_category,symptom_code,symptom_description',
            filters=filters,
            to_df=True
        )
        
        if not receptions.empty:
            cat_stats = receptions.groupby('symptom_category', dropna=False).size().reset_index(name='건수')
            cat_stats = cat_stats.sort_values('건수', ascending=False)
            st.dataframe(cat_stats, use_container_width=True, hide_index=True)
            
            st.divider()
            
            code_stats = receptions.groupby(['symptom_code', 'symptom_description'], dropna=False).size().reset_index(name='건수')
            code_stats = code_stats.sort_values('건수', ascending=False).head(20)
            code_stats.columns = ['증상코드', '증상설명', '건수']
            st.subheader("Top 20 증상")
            st.dataframe(code_stats, use_container_width=True, hide_index=True)
        else:
            st.info("해당 기간의 데이터가 없습니다.")
    
    with tab2:
        st.subheader("기간별 접수 추이")
        cols = st.columns(2)
        date_from2 = cols[0].date_input("시작일", value=date.today() - timedelta(days=90), key="trend_from")
        date_to2 = cols[1].date_input("종료일", value=date.today(), key="trend_to")
        
        filters2 = {
            'request_date__gte': str(date_from2),
            'request_date__lte': str(date_to2)
        }
        if role != '관리자':
            filters2['branch_id'] = branch_id
        
        trend_data = select_data(
            'as_reception',
            columns='request_date,status',
            filters=filters2,
            to_df=True
        )
        
        if not trend_data.empty:
            daily_stats = trend_data.groupby('request_date').size().reset_index(name='건수')
            st.line_chart(daily_stats.set_index('request_date'))
            
            status_stats = trend_data.groupby('status').size().reset_index(name='건수')
            st.dataframe(status_stats, use_container_width=True, hide_index=True)
        else:
            st.info("해당 기간의 데이터가 없습니다.")
    
    with tab3:
        st.subheader("상세 분석")
        
        # 실제 증상 vs 접수 증상 비교
        st.markdown("### 실제 증상 vs 접수 증상 비교")
        cols = st.columns(2)
        date_from3 = cols[0].date_input("시작일", value=date.today() - timedelta(days=30), key="detail_from")
        date_to3 = cols[1].date_input("종료일", value=date.today(), key="detail_to")
        
        # as_result에서 실제 증상 조회
        results_with_symptom = select_data(
            'as_result',
            columns='reception_id,actual_symptom_category,actual_symptom_code,actual_symptom_description',
            filters={
                'completed_at__gte': str(date_from3),
                'completed_at__lte': str(date_to3)
            },
            to_df=True
        )
        
        if not results_with_symptom.empty and 'actual_symptom_code' in results_with_symptom.columns:
            actual_symptom_stats = results_with_symptom.groupby(
                ['actual_symptom_code', 'actual_symptom_description'], dropna=False
            ).size().reset_index(name='건수')
            actual_symptom_stats = actual_symptom_stats.sort_values('건수', ascending=False).head(10)
            actual_symptom_stats.columns = ['실제 증상 코드', '설명', '건수']
            st.subheader("실제 현장 증상 Top 10")
            st.dataframe(actual_symptom_stats, use_container_width=True, hide_index=True)
            
            # ✅ 접수증상 vs 실제증상 변경 사례 배치 비교 (N+1 제거)
            rec_ids = sorted(set(int(r) for r in results_with_symptom['reception_id'].dropna().tolist()))
            mismatch_count = 0
            if rec_ids:
                rec_rows = select_data(
                    'as_reception',
                    columns='id,symptom_code',
                    filters={'id': ('in', rec_ids)}
                ) or []
                rec_map = {r['id']: r.get('symptom_code') for r in rec_rows}
                for _, r in results_with_symptom.iterrows():
                    rid = int(r['reception_id']) if pd.notna(r['reception_id']) else None
                    if rid and rec_map.get(rid) != r.get('actual_symptom_code'):
                        mismatch_count += 1
            
            st.metric("증상 변경 건수", f"{mismatch_count}건")
            st.caption(f"전체 {len(results_with_symptom)}건 중 접수 시 증상과 실제 증상이 다른 경우")
        else:
            st.info("실제 증상 데이터가 없습니다.")

# ==================== 페이지 10: 사용자 관리 ====================
def page_user_manage():
    st.title("👤 사용자 관리")
    
    tab1, tab2 = st.tabs(["📋 사용자 목록", "➕ 사용자 추가"])
    
    with tab1:
        users = select_data(
            'users', 
            columns='id,username,name,role,branch_name,is_active,created_at',
            order=('id','asc'),
            to_df=True
        )
        
        if not users.empty:
            st.dataframe(users, use_container_width=True, hide_index=True)
        else:
            st.info("사용자 데이터가 없습니다.")
    
    with tab2:
        with st.form("add_user_form"):
            cols = st.columns(3)
            username = cols[0].text_input("아이디*", placeholder="user01")
            password = cols[1].text_input("비밀번호*", type="password", placeholder="****")
            name = cols[2].text_input("이름*", placeholder="홍길동")
            
            cols2 = st.columns(2)
            role = cols2[0].selectbox("권한*", ["사용자", "관리자"])
            
            branches = select_data('branch', columns='id,branch_name', to_df=True)
            branch_options = dict(zip(branches['id'], branches['branch_name'])) if not branches.empty else {}
            branch_id = cols2[1].selectbox(
                "소속 지점", 
                [0] + list(branch_options.keys()) if branch_options else [0],
                format_func=lambda x: "미지정" if x == 0 else branch_options.get(x, str(x))
            )
            
            is_active = st.checkbox("활성화", value=True)
            
            if st.form_submit_button("✅ 추가"):
                if username and password and name:
                    branch_name = branch_options.get(branch_id, None) if branch_id > 0 else None
                    user_data = {
                        'username': username,
                        'password': password,
                        'name': name,
                        'role': role,
                        'branch_id': branch_id if branch_id > 0 else None,
                        'branch_name': branch_name,
                        'is_active': 1 if is_active else 0
                    }
                    insert_data('users', user_data)
                    st.success("✅ 사용자가 추가되었습니다.")
                    st.rerun()
                else:
                    st.error("❌ 필수 항목을 입력하세요.")

# ==================== 페이지 11: 검수 결과 ====================
def page_quality_inspection(user, role, branch_id):
    st.title("🔍 검수 결과")
    
    with st.expander("🔎 검색", expanded=True):
        cols = st.columns([2, 2, 2, 2])
        search_keyword = cols[0].text_input("통합검색", placeholder="고객명/접수번호")
        date_from = cols[1].date_input("시작일", value=date.today() - timedelta(days=7))
        date_to = cols[2].date_input("종료일", value=date.today())
        tech_filter = cols[3].text_input("기사명", placeholder="기사명 검색")
    
    filters = {'status': '검수완료'}
    if role != '관리자':
        filters['branch_id'] = branch_id
    filters['complete_date__gte'] = str(date_from)
    filters['complete_date__lte'] = str(date_to)
    
    inspected_data = select_data(
        'as_reception',
        columns='id,reception_number,customer_name,phone,model_code,symptom_description,complete_date',
        filters=filters,
        order=('complete_date','desc'),
        to_df=True
    )
    
    if search_keyword and not inspected_data.empty:
        mask = (inspected_data['customer_name'].str.contains(search_keyword, na=False) | 
                inspected_data['reception_number'].str.contains(search_keyword, na=False))
        inspected_data = inspected_data[mask]
    
    st.caption(f"총 {len(inspected_data)}건")
    
    if not inspected_data.empty:
        for _, row in inspected_data.iterrows():
            # 결과 정보 조회
            result = select_data(
                'as_result',
                columns='technician_name,result,labor_cost,completed_at',
                filters={'reception_id': int(row['id'])},
                limit=1
            )
            
            if tech_filter and result:
                tech_name = result[0].get('technician_name') or ""
                if tech_filter not in tech_name:
                    continue
            
            with st.container():
                cols = st.columns([1, 1, 1, 1, 2, 1, 1])
                cols[0].write(row['reception_number'])
                cols[1].write(row['customer_name'])
                cols[2].write(row['phone'])
                cols[3].write(row['model_code'])
                cols[4].write(str(row['symptom_description'])[:30])
                
                if result:
                    tech_name = result[0].get('technician_name') or "-"
                    labor = result[0].get('labor_cost') or 0
                    try:
                        labor = int(labor)
                    except:
                        labor = 0
                    cols[5].write(tech_name)
                    cols[6].write(f"{labor:,.0f}원")
                else:
                    cols[5].write("-")
                    cols[6].write("-")
                
                st.markdown("---")
    else:
        st.info("검수 완료된 내역이 없습니다.")

# ==================== 페이지 12: 백데이터 조회 ====================
def page_legacy_data(user):
    st.title("📚 백데이터 조회")
    st.caption("이카운트 ERP에서 이관된 과거 AS 접수 내역을 조회합니다.")
    
    # 업로드된 파일 경로
    legacy_file_path = "uploads/legacy_data.xlsx"
    
    # 관리자만 업로드 가능
    if user['role'] == '관리자':
        with st.expander("📤 엑셀 파일 업로드 (관리자 전용)", expanded=False):
            uploaded_file = st.file_uploader("이카운트 ERP 엑셀 파일 업로드", type=['xlsx'], key="legacy_upload")
            if uploaded_file:
                if st.button("✅ 업로드 및 저장"):
                    os.makedirs("uploads", exist_ok=True)
                    with open(legacy_file_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())
                    st.success("✅ 백데이터 파일이 업로드되었습니다!")
                    st.rerun()
    
    # 파일 존재 여부 확인
    if not os.path.exists(legacy_file_path):
        st.warning("⚠️ 업로드된 백데이터 파일이 없습니다.")
        if user['role'] == '관리자':
            st.info("👆 위 업로드 섹션에서 엑셀 파일을 업로드하세요.")
        else:
            st.info("관리자가 파일을 업로드할 때까지 기다려주세요.")
        return
    
    # 엑셀 파일 읽기
    try:
        df = pd.read_excel(legacy_file_path)
        
        # 첫 행이 헤더인 경우 처리
        if '일자-No.' in df.iloc[0].values:
            df.columns = df.iloc[0]
            df = df[1:].reset_index(drop=True)
        
        # 컬럼명 정리
        df.columns = df.columns.str.strip()
        
        st.success(f"✅ 백데이터 로드 완료 (총 {len(df)}건)")
        
    except Exception as e:
        st.error(f"❌ 파일 읽기 오류: {e}")
        return
    
    # 검색 기능
    with st.expander("🔎 검색", expanded=True):
        cols = st.columns([3, 2, 2])
        search_keyword = cols[0].text_input("통합검색", placeholder="고객명/연락처/일자-No.")
        status_filter = cols[1].selectbox("진행상태", ["전체"] + df['진행상태'].unique().tolist() if '진행상태' in df.columns else ["전체"])
        branch_filter = cols[2].selectbox("지점", ["전체"] + df['지점명'].unique().tolist() if '지점명' in df.columns else ["전체"])
    
    # 필터링
    filtered_df = df.copy()
    
    if search_keyword:
        mask = False
        if '고객명' in filtered_df.columns:
            mask |= filtered_df['고객명'].astype(str).str.contains(search_keyword, na=False, case=False)
        if '연락처' in filtered_df.columns:
            mask |= filtered_df['연락처'].astype(str).str.contains(search_keyword, na=False, case=False)
        if '일자-No.' in filtered_df.columns:
            mask |= filtered_df['일자-No.'].astype(str).str.contains(search_keyword, na=False, case=False)
        filtered_df = filtered_df[mask]
    
    if status_filter != "전체" and '진행상태' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['진행상태'] == status_filter]
    
    if branch_filter != "전체" and '지점명' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['지점명'] == branch_filter]
    
    st.caption(f"검색 결과: {len(filtered_df)}건")
    
    # 통계
    if not filtered_df.empty:
        st.divider()
        st.subheader("📊 통계")
        cols_stat = st.columns(4)
        
        if '진행상태' in filtered_df.columns:
            status_counts = filtered_df['진행상태'].value_counts()
            for i, (status, count) in enumerate(status_counts.items()):
                if i < 4:
                    cols_stat[i].metric(status, f"{count}건")
        
        st.divider()
    
    # 데이터 표시
    if not filtered_df.empty:
        # 엑셀 다운로드
        excel_data = download_excel(filtered_df)
        st.download_button(
            "📥 검색 결과 엑셀 다운로드",
            excel_data,
            file_name=f"백데이터_검색결과_{date.today()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        
        # 데이터 테이블
        st.dataframe(filtered_df, use_container_width=True, hide_index=True)
    else:
        st.info("조회된 데이터가 없습니다.")

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
