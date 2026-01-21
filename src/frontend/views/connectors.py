import streamlit as st
import os
import base64
import time

def get_img_as_base64(file_path):
    try:
        with open(file_path, "rb") as f:
            data = f.read()
        return base64.b64encode(data).decode()
    except Exception:
        return ""

def render():
    assets_dir = os.path.join(os.path.dirname(__file__), "../assets")

    CONNECTORS = {
        "snowflake": { "name": "Snowflake", "logo": "snowflake_logo.png", "status": "Active", "desc": "Data Warehouse", "fields": ["Account", "User", "Password", "Warehouse", "Database", "Schema"] },
        "databricks": { "name": "Databricks", "logo": "databricks_logo.png", "status": "Inactive", "desc": "Lakehouse Platform", "fields": ["Host", "HTTP Path", "Token"] },
        "sqlserver": { "name": "SQL Server", "logo": "sqlserver_logo.png", "status": "Inactive", "desc": "RDBMS", "fields": ["Server", "Database", "User", "Password", "Port"] },
        "oracle": { "name": "Oracle DB", "logo": "oracle_logo.png", "status": "Inactive", "desc": "RDBMS", "fields": ["Host", "Port", "Service", "User", "Password"] },
        "sap": { "name": "SAP HANA", "logo": "sap_logo.png", "status": "Inactive", "desc": "In-Memory DB", "fields": ["Host", "Instance", "User", "Password"] },
        "deltalake": { "name": "Delta Lake", "logo": "deltalake_logo.png", "status": "Inactive", "desc": "Storage Layer", "fields": ["Account", "Container", "Key"] }
    }

    if 'conn_selection' not in st.session_state:
        st.session_state['conn_selection'] = "Snowflake"

    # ==========================================================================
    # BULLETPROOF CSS - MAXIMUM SPECIFICITY
    # ==========================================================================
    st.markdown("""
    <style>
        /* =====================================================================
           CONNECTOR PAGE - CLEAN DROPDOWN STYLING
           ===================================================================== */
        
        /* DROPDOWN TRIGGER - CLEAN WHITE */
        .stSelectbox [data-baseweb="select"] > div:first-child,
        div[data-baseweb="select"] > div:first-child {
            background: #ffffff !important;
            background-color: #ffffff !important;
            border: 1px solid #d1d5db !important;
            border-radius: 8px !important;
            color: #1f2937 !important;
            box-shadow: none !important;
        }
        
        .stSelectbox [data-baseweb="select"] > div:first-child:hover,
        div[data-baseweb="select"] > div:first-child:hover {
            border-color: #9ca3af !important;
        }
        
        /* DROPDOWN TEXT */
        .stSelectbox span, div[data-baseweb="select"] span {
            color: #1f2937 !important;
            font-weight: 500 !important;
            font-size: 14px !important;
        }
        
        /* DROPDOWN ARROW */
        .stSelectbox svg, div[data-baseweb="select"] svg {
            fill: #6b7280 !important;
        }
        
        /* DROPDOWN MENU CONTAINER - CLEAN WHITE */
        [data-baseweb="popover"],
        [data-baseweb="popover"] > div {
            background: #ffffff !important;
            background-color: #ffffff !important;
            border: none !important;
            box-shadow: 0 4px 16px rgba(0,0,0,0.12) !important;
            border-radius: 10px !important;
            overflow: hidden !important;
        }
        
        /* DROPDOWN LIST */
        ul[data-testid="stSelectboxVirtualDropdown"],
        [data-baseweb="menu"],
        ul[role="listbox"] {
            background: #ffffff !important;
            background-color: #ffffff !important;
            padding: 6px !important;
            border: none !important;
            border-radius: 10px !important;
        }
        
        /* DROPDOWN ITEMS - TARGET EMOTION CACHE CLASSES */
        li[role="option"],
        li.st-emotion-cache-xcuh4j,
        li.st-emotion-cache-5djvkm,
        .eg1z3xh0 {
            background: #ffffff !important;
            background-color: #ffffff !important;
            color: #374151 !important;
            border: none !important;
            border-radius: 6px !important;
            margin: 2px 6px !important;
            padding: 10px 14px !important;
            font-size: 14px !important;
            font-weight: 500 !important;
        }
        
        /* HOVER STATE */
        li[role="option"]:hover,
        li.st-emotion-cache-xcuh4j:hover,
        li.st-emotion-cache-5djvkm:hover {
            background: #f3f4f6 !important;
            background-color: #f3f4f6 !important;
            color: #111827 !important;
        }
        
        /* SELECTED STATE */
        li[role="option"][aria-selected="true"],
        li.st-emotion-cache-5djvkm {
            background: #fef2f2 !important;
            background-color: #fef2f2 !important;
            color: #dc2626 !important;
            font-weight: 600 !important;
        }
        
        /* TEXT INSIDE DROPDOWN ITEMS */
        .st-emotion-cache-qiev7j,
        .st-emotion-cache-11loom0,
        li[role="option"] div {
            color: inherit !important;
            font-weight: inherit !important;
        }
        
        /* REMOVE ANY DARK BORDERS/OUTLINES */
        li[role="option"]::before,
        li[role="option"]::after,
        .stSelectbox *::before,
        .stSelectbox *::after {
            display: none !important;
        }
        
        /* SCROLLBAR - HIDE HORIZONTAL, CLEAN VERTICAL */
        ul[data-testid="stSelectboxVirtualDropdown"],
        [data-baseweb="menu"],
        ul[role="listbox"] {
            overflow-x: hidden !important;
            overflow-y: auto !important;
            scrollbar-width: thin !important;
            scrollbar-color: #e5e7eb transparent !important;
        }
        
        ul[data-testid="stSelectboxVirtualDropdown"]::-webkit-scrollbar {
            width: 4px !important;
            height: 0px !important;
        }
        ul[data-testid="stSelectboxVirtualDropdown"]::-webkit-scrollbar-thumb {
            background: #d1d5db !important;
            border-radius: 4px !important;
        }
        ul[data-testid="stSelectboxVirtualDropdown"]::-webkit-scrollbar-track {
            background: transparent !important;
        }
        
        /* Hide horizontal scrollbar everywhere in dropdown */
        [data-baseweb="popover"] *,
        .stSelectbox * {
            overflow-x: hidden !important;
        }
        /* =================================================================
           PREMIUM LIGHT BUTTONS - DARK TEXT, RED ACCENTS
           ================================================================= */
        
        /* === SHARED BASE STYLES === */
        .st-key-test_snowflake button,
        .st-key-test_databricks button,
        .st-key-test_sqlserver button,
        .st-key-test_oracle button,
        .st-key-test_sap button,
        .st-key-test_deltalake button,
        .st-key-toggle_snowflake button,
        .st-key-toggle_databricks button,
        .st-key-toggle_sqlserver button,
        .st-key-toggle_oracle button,
        .st-key-toggle_sap button,
        .st-key-toggle_deltalake button,
        .st-key-save_snowflake button,
        .st-key-save_databricks button,
        .st-key-save_sqlserver button,
        .st-key-save_oracle button,
        .st-key-save_sap button,
        .st-key-save_deltalake button,
        div[data-testid="stVerticalBlockBorderWrapper"] .st-emotion-cache-1anq8dj,
        div[data-testid="stVerticalBlockBorderWrapper"] .st-emotion-cache-1krtkoa {
            height: 44px !important;
            min-height: 44px !important;
            padding: 0 28px !important;
            border-radius: 10px !important;
            font-size: 14px !important;
            font-weight: 600 !important;
            letter-spacing: 0.2px !important;
            cursor: pointer !important;
            transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1) !important;
        }
        
        /* === DEFAULT - Light & Premium with Depth === */
        .st-key-test_snowflake button,
        .st-key-test_databricks button,
        .st-key-test_sqlserver button,
        .st-key-test_oracle button,
        .st-key-test_sap button,
        .st-key-test_deltalake button,
        .st-key-toggle_snowflake button,
        .st-key-toggle_databricks button,
        .st-key-toggle_sqlserver button,
        .st-key-toggle_oracle button,
        .st-key-toggle_sap button,
        .st-key-toggle_deltalake button,
        .st-key-save_snowflake button,
        .st-key-save_databricks button,
        .st-key-save_sqlserver button,
        .st-key-save_oracle button,
        .st-key-save_sap button,
        .st-key-save_deltalake button,
        div[data-testid="stVerticalBlockBorderWrapper"] .st-emotion-cache-1anq8dj,
        div[data-testid="stVerticalBlockBorderWrapper"] .st-emotion-cache-1krtkoa {
            background: #fafafa !important;
            background-color: #fafafa !important;
            color: #1f2937 !important;
            border: 1px solid #d1d5db !important;
            box-shadow: 0 2px 6px rgba(0, 0, 0, 0.08), 0 1px 2px rgba(0, 0, 0, 0.04) !important;
        }
        
        /* === HOVER - Mild Red Accent === */
        .st-key-test_snowflake button:hover,
        .st-key-test_databricks button:hover,
        .st-key-test_sqlserver button:hover,
        .st-key-test_oracle button:hover,
        .st-key-test_sap button:hover,
        .st-key-test_deltalake button:hover,
        .st-key-toggle_snowflake button:hover,
        .st-key-toggle_databricks button:hover,
        .st-key-toggle_sqlserver button:hover,
        .st-key-toggle_oracle button:hover,
        .st-key-toggle_sap button:hover,
        .st-key-toggle_deltalake button:hover,
        .st-key-save_snowflake button:hover,
        .st-key-save_databricks button:hover,
        .st-key-save_sqlserver button:hover,
        .st-key-save_oracle button:hover,
        .st-key-save_sap button:hover,
        .st-key-save_deltalake button:hover,
        div[data-testid="stVerticalBlockBorderWrapper"] .st-emotion-cache-1anq8dj:hover,
        div[data-testid="stVerticalBlockBorderWrapper"] .st-emotion-cache-1krtkoa:hover {
            background: #fef2f2 !important;
            background-color: #fef2f2 !important;
            color: #991b1b !important;
            border-color: #fecaca !important;
            box-shadow: 0 4px 12px rgba(153, 27, 27, 0.08) !important;
            transform: translateY(-1px) !important;
        }
        
        /* === ACTIVE/CLICK - Red Tint === */
        .st-key-test_snowflake button:active,
        .st-key-test_databricks button:active,
        .st-key-test_sqlserver button:active,
        .st-key-test_oracle button:active,
        .st-key-test_sap button:active,
        .st-key-test_deltalake button:active,
        .st-key-toggle_snowflake button:active,
        .st-key-toggle_databricks button:active,
        .st-key-toggle_sqlserver button:active,
        .st-key-toggle_oracle button:active,
        .st-key-toggle_sap button:active,
        .st-key-toggle_deltalake button:active,
        .st-key-save_snowflake button:active,
        .st-key-save_databricks button:active,
        .st-key-save_sqlserver button:active,
        .st-key-save_oracle button:active,
        .st-key-save_sap button:active,
        .st-key-save_deltalake button:active,
        div[data-testid="stVerticalBlockBorderWrapper"] .st-emotion-cache-1anq8dj:active,
        div[data-testid="stVerticalBlockBorderWrapper"] .st-emotion-cache-1krtkoa:active {
            background: #fee2e2 !important;
            color: #7f1d1d !important;
            border-color: #fca5a5 !important;
            transform: translateY(0) !important;
            box-shadow: 0 1px 2px rgba(0, 0, 0, 0.05) !important;
        }
        
        /* === TEXT COLORS === */
        .st-key-test_snowflake button p,
        .st-key-test_snowflake button span,
        .st-key-toggle_snowflake button p,
        .st-key-toggle_snowflake button span,
        .st-key-save_snowflake button p,
        .st-key-save_snowflake button span,
        div[data-testid="stVerticalBlockBorderWrapper"] .st-emotion-cache-1anq8dj p,
        div[data-testid="stVerticalBlockBorderWrapper"] .st-emotion-cache-1anq8dj span,
        div[data-testid="stVerticalBlockBorderWrapper"] .st-emotion-cache-1krtkoa p,
        div[data-testid="stVerticalBlockBorderWrapper"] .st-emotion-cache-1krtkoa span {
            color: inherit !important;
            font-weight: 600 !important;
        }
        
        /* Inner containers transparent */
        .e1q4kxr41, .e1q4kxr42,
        .e1q4kxr422, .e1q4kxr423 {
            background: transparent !important;
        }
        
        /* Card container */
        div[data-testid="stVerticalBlockBorderWrapper"] > div {
            background: #ffffff !important;
            border: 1px solid #e5e7eb !important;
            border-radius: 12px !important;
            padding: 24px !important;
        }
        
    </style>
    """, unsafe_allow_html=True)

    # ==========================================================================
    # PAGE HEADER
    # ==========================================================================
    st.markdown("### Data Connectors")
    st.markdown("<p style='color:#6b7280; font-size:14px; margin-top:-10px;'>Manage and configure your enterprise data sources.</p>", unsafe_allow_html=True)

    # Helpers
    conn_list = list(CONNECTORS.values())
    option_names = [c["name"] for c in conn_list]
    
    def get_key(name):
        for k, v in CONNECTORS.items():
            if v["name"] == name:
                return k
        return list(CONNECTORS.keys())[0]

    current_idx = option_names.index(st.session_state.get('conn_selection', 'Snowflake')) if st.session_state.get('conn_selection') in option_names else 0

    # ==========================================================================
    # DROPDOWN
    # ==========================================================================
    col1, col2 = st.columns([1, 3])
    with col1:
        selected_name = st.selectbox(
            "Select",
            options=option_names,
            index=current_idx,
            label_visibility="collapsed",
            key="connector_select"
        )
    st.session_state['conn_selection'] = selected_name

    # Get active connector
    active_data = next((item for item in conn_list if item["name"] == selected_name), conn_list[0])
    conn_key = get_key(selected_name)
    logo_path = os.path.join(assets_dir, active_data["logo"])
    logo_b64 = get_img_as_base64(logo_path)
    is_active = active_data["status"] == "Active"

    st.write("")

    # ==========================================================================
    # CONNECTOR CARD
    # ==========================================================================
    with st.container(border=True):
        
        # Header row
        hc1, hc2 = st.columns([6, 1])
        with hc1:
            st.markdown(f"""
            <div style="display:flex; align-items:center; gap:14px;">
                <img src="data:image/png;base64,{logo_b64}" style="width:44px; height:44px; object-fit:contain;">
                <div>
                    <div style="font-size:17px; font-weight:600; color:#111827; margin-bottom:2px;">{active_data['name']}</div>
                    <div style="font-size:13px; color:#6b7280;">{active_data['desc']}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with hc2:
            if is_active:
                st.markdown("""<div style="background:#dcfce7; color:#166534; padding:6px 12px; border-radius:20px; font-size:12px; font-weight:600; display:inline-flex; align-items:center; gap:6px; float:right;">
                    <span style="width:6px; height:6px; background:#22c55e; border-radius:50%;"></span>Active</div>""", unsafe_allow_html=True)
            else:
                st.markdown("""<div style="background:#f3f4f6; color:#6b7280; padding:6px 12px; border-radius:20px; font-size:12px; font-weight:600; display:inline-flex; align-items:center; gap:6px; float:right;">
                    <span style="width:6px; height:6px; background:#9ca3af; border-radius:50%;"></span>Inactive</div>""", unsafe_allow_html=True)

        st.markdown("<hr style='border:none; border-top:1px solid #e5e7eb; margin:16px 0;'>", unsafe_allow_html=True)
        
        # Section header
        st.markdown("<div style='font-size:12px; font-weight:600; color:#6b7280; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px;'>Configuration</div>", unsafe_allow_html=True)
        
        # Input fields
        c1, c2 = st.columns(2)
        for i, f in enumerate(active_data["fields"]):
            with (c1 if i % 2 == 0 else c2):
                is_secret = any(s in f.lower() for s in ["password", "token", "key"])
                st.text_input(f.upper(), key=f"{conn_key}_{f}", type="password" if is_secret else "default")

        # Spacer
        st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)
        
        # ==========================================================================
        # ACTION BUTTONS
        # ==========================================================================
        _, b1, b2, b3 = st.columns([4, 1.5, 1.3, 0.8])
        
        with b1:
            test_clicked = st.button("Test Connection", type="secondary", use_container_width=True, key=f"test_{conn_key}")
            if test_clicked:
                with st.spinner("Testing..."):
                    time.sleep(1)
                st.toast("Connection verified")
        
        with b2:
            label = "Disconnect" if is_active else "Connect"
            toggle_clicked = st.button(label, type="secondary", use_container_width=True, key=f"toggle_{conn_key}")
            if toggle_clicked:
                time.sleep(0.5)
                st.toast("Done")
        
        with b3:
            save_clicked = st.button("Save", type="primary", use_container_width=True, key=f"save_{conn_key}")
            if save_clicked:
                time.sleep(0.5)
                st.toast("Saved")
