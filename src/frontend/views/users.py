import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def render():
    # --- BRAND COLORS & SYSTEM ---
    COLORS = {
        'red': "#D11F41",
        'slate': "#0F172A",
        'slate_light': "#64748B",
        'bg_main': "#F8FAFC",
        'white': "#FFFFFF",
        'border': "#E2E8F0",
        'green': "#059669",
        'green_light': "#ECFDF5",
        'red_light': "#FFF1F2",
        'blue': "#3B82F6",
        'blue_light': "#EFF6FF"
    }

    # --- SYNC WITH PLATFORM REGISTRY ---
    if 'PLATFORM_USERS' in st.session_state:
        # Convert dict to list for table display
        registry = st.session_state['PLATFORM_USERS']
        users_list = []
        for username, info in registry.items():
            users_list.append({
                "username": username,
                "name": info.get("name", ""),
                "email": info.get("email", ""),
                "role": info.get("role", ""),
                "status": info.get("status", "Active"),
                "last_login": info.get("last_login", "Never")
            })
        st.session_state['users_db'] = users_list
    else:
        # Fallback if app.py hasn't initialized it (unlikely)
        if 'users_db' not in st.session_state:
            st.session_state['users_db'] = []

    # --- CUSTOM UI/UX STYLING ---
    st.markdown(f"""
    <style>
        .stApp {{ background-color: {COLORS['bg_main']} !important; }}
        
        /* Glassmorphism Header */
        .page-header {{
            background: rgba(255, 255, 255, 0.7);
            backdrop-filter: blur(10px);
            border: 1px solid {COLORS['border']};
            border-radius: 16px;
            padding: 24px 32px;
            margin-bottom: 32px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        
        /* KPI Cards */
        .user-kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 32px;
        }}
        
        .user-stat-card {{
            background: {COLORS['white']};
            border: 1px solid {COLORS['border']};
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.04);
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        }}
        
        .user-stat-card:hover {{
            transform: translateY(-4px);
            box-shadow: 0 12px 20px -8px rgba(0,0,0,0.1);
            border-color: {COLORS['red']};
        }}
        
        /* Table / List View */
        .user-table-container {{
            background: {COLORS['white']};
            border: 1px solid {COLORS['border']};
            border-radius: 16px;
            overflow: hidden;
            box-shadow: 0 4px 6px -1px rgba(0,0,0,0.02);
        }}
        
        /* Premium Table Styles override */
        .stDataFrame {{
            border: none !important;
        }}
        
        /* Status Badges */
        .badge {{
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
        }}
        .badge-active {{ background: {COLORS['green_light']}; color: {COLORS['green']}; }}
        .badge-inactive {{ background: #F1F5F9; color: {COLORS['slate_light']}; }}
        
        /* Add User Button (Specifically this one, standard st.button) */
        .main-content div.stButton > button[kind="primary"],
        button[data-testid="stBaseButton-primary"] {{
            background: {COLORS['red']} !important; /* Brand Red */
            color: white !important;
            border: none !important;
            border-radius: 24px !important; /* Pill Shape */
            padding: 0.6rem 2.0rem !important; /* Wider padding */
            font-weight: 600 !important;
            box-shadow: 0 4px 6px -1px rgba(209, 31, 65, 0.3) !important;
            transition: all 0.3s ease !important;
        }}
        
        .main-content div.stButton > button[kind="primary"]:hover,
        button[data-testid="stBaseButton-primary"]:hover {{
            background: #9f1239 !important;
            transform: translateY(-2px);
            box-shadow: 0 10px 15px -3px rgba(159, 18, 57, 0.4) !important;
        }}
        
        /* Create Identity Button (Form Submit - Keep rectangular/rounded) */
        .main-content div.stButton > button[kind="primaryFormSubmit"],
        button[kind="primaryFormSubmit"],
        button[data-testid="stBaseButton-primaryFormSubmit"] {{
            background: {COLORS['red']} !important;
            color: white !important;
            border: none !important;
            border-radius: 10px !important; /* Original Radius */
            padding: 0.6rem 1.5rem !important;
            font-weight: 600 !important;
            box-shadow: 0 4px 6px -1px rgba(209, 31, 65, 0.3) !important;
            transition: all 0.3s ease !important;
        }}
        
        button[kind="primaryFormSubmit"]:hover,
        button[data-testid="stBaseButton-primaryFormSubmit"]:hover {{
             background: #9f1239 !important;
             transform: translateY(-2px);
             box-shadow: 0 10px 15px -3px rgba(159, 18, 57, 0.4) !important;
        }}
        
        /* Secondary Button - Explicit "Back/Cancel" styling */
        .main-content div.stButton > button[kind="secondary"],
        .main-content div.stButton > button[kind="secondaryFormSubmit"],
        button[kind="secondaryFormSubmit"],
        button[data-testid="stBaseButton-secondaryFormSubmit"] {{
            background-color: {COLORS['white']} !important;
            color: #475569 !important;
            border: 1px solid {COLORS['border']} !important;
            font-weight: 600 !important;
            border-radius: 10px !important;
            padding: 0.6rem 1.5rem !important; /* Match primary padding */
            box-shadow: 0 1px 2px rgba(0,0,0,0.05) !important;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
        }}
        
        .main-content div.stButton > button[kind="secondary"]:hover,
        .main-content div.stButton > button[kind="secondaryFormSubmit"]:hover,
        button[kind="secondaryFormSubmit"]:hover,
        button[data-testid="stBaseButton-secondaryFormSubmit"]:hover {{
            border-color: #CBD5E1 !important;
            background-color: #F8FAFC !important;
            color: #0F172A !important;
            box-shadow: 0 10px 15px -3px rgba(0,0,0,0.1), 0 4px 6px -2px rgba(0,0,0,0.05) !important;
            transform: translateY(-2px);
        }}
        
        /* Search Box */
        div[data-baseweb="base-input"] {{
            border-radius: 10px !important;
            border: 1px solid {COLORS['border']} !important;
            padding: 2px 8px !important;
        }}
        
        .section-title {{
            font-size: 20px;
            font-weight: 700;
            color: {COLORS['slate']};
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        
        .section-title::before {{
            content: "";
            width: 4px;
            height: 20px;
            background: {COLORS['red']};
            border-radius: 2px;
        }}

        /* --- FORCE LIGHT THEME OVERRIDES --- */
        
        /* Dropdowns & Inputs */
        .stSelectbox div[data-baseweb="select"] > div,
        div[data-baseweb="base-input"] {{
            background-color: #ffffff !important;
            color: #0f172a !important;
            border: 1px solid #e2e8f0 !important;
        }}
        
        /* Labels targeting */
        .stTextInput label, .stSelectbox label, [data-testid="stForm"] label {{
            color: {COLORS['slate_light']} !important;
            font-size: 11px !important;
            font-weight: 600 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.5px !important;
        }}
        
        /* =====================================================================
           USER MANAGEMENT DROPDOWN - CLEAN LIGHT THEME
           ===================================================================== */
        
        /* DROPDOWN TRIGGER - CLEAN WHITE */
        .stSelectbox [data-baseweb="select"] > div:first-child,
        div[data-baseweb="select"] > div:first-child {{
            background: #ffffff !important;
            background-color: #ffffff !important;
            border: 1px solid #d1d5db !important;
            border-radius: 8px !important;
            color: #1f2937 !important;
            box-shadow: none !important;
        }}
        
        .stSelectbox [data-baseweb="select"] > div:first-child:hover,
        div[data-baseweb="select"] > div:first-child:hover {{
            border-color: #9ca3af !important;
        }}
        
        /* DROPDOWN TEXT */
        .stSelectbox span, div[data-baseweb="select"] span {{
            color: #1f2937 !important;
            font-weight: 500 !important;
            font-size: 14px !important;
        }}
        
        /* DROPDOWN ARROW */
        .stSelectbox svg, div[data-baseweb="select"] svg {{
            fill: #6b7280 !important;
        }}
        
        /* DROPDOWN MENU CONTAINER - CLEAN WHITE */
        [data-baseweb="popover"],
        [data-baseweb="popover"] > div {{
            background: #ffffff !important;
            background-color: #ffffff !important;
            border: none !important;
            box-shadow: 0 4px 16px rgba(0,0,0,0.12) !important;
            border-radius: 10px !important;
            overflow: hidden !important;
        }}
        
        /* DROPDOWN LIST */
        ul[data-testid="stSelectboxVirtualDropdown"],
        [data-baseweb="menu"],
        ul[role="listbox"] {{
            background: #ffffff !important;
            background-color: #ffffff !important;
            padding: 6px !important;
            border: none !important;
            border-radius: 10px !important;
        }}
        
        /* DROPDOWN ITEMS */
        li[role="option"],
        ul[data-baseweb="menu"] li,
        [data-baseweb="menu"] li {{
            background: #ffffff !important;
            background-color: #ffffff !important;
            color: #374151 !important;
            border: none !important;
            border-radius: 6px !important;
            margin: 2px 6px !important;
            padding: 10px 14px !important;
            font-size: 14px !important;
            font-weight: 500 !important;
            transition: all 0.2s ease !important;
        }}
        
        /* HOVER STATE */
        li[role="option"]:hover,
        ul[data-baseweb="menu"] li:hover,
        [data-baseweb="menu"] li:hover {{
            background: #fef2f2 !important;
            background-color: #fef2f2 !important;
            color: #dc2626 !important;
        }}
        
        /* SELECTED STATE */
        li[role="option"][aria-selected="true"],
        ul[data-baseweb="menu"] li[aria-selected="true"],
        [data-baseweb="menu"] li[aria-selected="true"] {{
            background: #fef2f2 !important;
            background-color: #fef2f2 !important;
            color: #dc2626 !important;
            font-weight: 600 !important;
        }}
        
        /* TEXT INSIDE DROPDOWN ITEMS */
        li[role="option"] div,
        ul[data-baseweb="menu"] li *,
        [data-baseweb="menu"] li * {{
            color: inherit !important;
            font-weight: inherit !important;
        }}
        
        /* SCROLLBAR - CLEAN */
        ul[data-testid="stSelectboxVirtualDropdown"],
        [data-baseweb="menu"],
        ul[role="listbox"] {{
            overflow-x: hidden !important;
            overflow-y: auto !important;
            scrollbar-width: thin !important;
            scrollbar-color: #e5e7eb transparent !important;
        }}
        
        ul[data-testid="stSelectboxVirtualDropdown"]::-webkit-scrollbar {{
            width: 4px !important;
            height: 0px !important;
        }}
        ul[data-testid="stSelectboxVirtualDropdown"]::-webkit-scrollbar-thumb {{
            background: #d1d5db !important;
            border-radius: 4px !important;
        }}
        ul[data-testid="stSelectboxVirtualDropdown"]::-webkit-scrollbar-track {{
            background: transparent !important;
        }}
        
        /* Input Text Color */
        .stTextInput input, .stSelectbox div {{
            color: #0f172a !important;
        }}

        /* DataFrame/Table - Force Light Background & Dark Text */
        .stDataFrame {{
            background-color: #ffffff !important;
        }}
        .stDataFrame div[data-testid="stDataFrameResizable"] {{
            background-color: #ffffff !important;
        }}
        .stDataFrame div[role="grid"] {{
            color: #0f172a !important; /* Dark Text for rows */
            background-color: #ffffff !important;
        }}
        /* Header Text */
        .stDataFrame div[role="columnheader"] {{
            color: #475569 !important; 
            background-color: #f8fafc !important;
            font-weight: 600 !important;
        }}
        
        /* Override Streamlit Dark Mode Button Defaults if persistent */
        .main-content div.stButton > button {{
             color: #0f172a !important;
        }}
        .main-content div.stButton > button[kind="primary"] {{
             color: #ffffff !important; /* Keep Primary White Text */
        }}
    </style>
    """, unsafe_allow_html=True)

    # --- HEADER SECTION ---
    st.markdown(f"""
    <div class="main-content">
    <div style="margin-bottom: 32px;">
        <h1 style="color: {COLORS['slate']}; font-weight: 800; font-size: 32px; margin: 0; letter-spacing: -0.5px;">Identity Control Center</h1>
        <p style="color: {COLORS['slate_light']}; font-size: 15px; margin-top: 8px; margin-bottom: 0;">Manage platform users, roles, and access permissions with precision.</p>
    </div>
    """, unsafe_allow_html=True)

    # --- KPI STRIP ---
    users_db = st.session_state.get('users_db', [])
    total_users = len(users_db)
    active_users = sum(1 for u in users_db if u['status'] == "Active")
    admins = sum(1 for u in users_db if u['role'] == "Admin")
    
    st.markdown('<div class="user-kpi-grid">', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"""<div class="user-stat-card"><div style="color:{COLORS['slate_light']}; font-size:11px; font-weight:600; text-transform:uppercase; letter-spacing:1px; margin-bottom:8px;">Total Users</div><div style="font-size:32px; font-weight:800; color:{COLORS['slate']}">{total_users}</div><div style="font-size:12px; color:{COLORS['slate_light']}; font-weight:500; margin-top:6px;">Registered accounts</div></div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""<div class="user-stat-card"><div style="color:{COLORS['slate_light']}; font-size:11px; font-weight:600; text-transform:uppercase; letter-spacing:1px; margin-bottom:8px;">Active Users</div><div style="font-size:32px; font-weight:800; color:{COLORS['green']}">{active_users}</div><div style="font-size:12px; color:{COLORS['slate_light']}; font-weight:500; margin-top:6px;">{int(active_users/total_users*100) if total_users > 0 else 0}% of total</div></div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""<div class="user-stat-card"><div style="color:{COLORS['slate_light']}; font-size:11px; font-weight:600; text-transform:uppercase; letter-spacing:1px; margin-bottom:8px;">Administrators</div><div style="font-size:32px; font-weight:800; color:{COLORS['red']}">{admins}</div><div style="font-size:12px; color:{COLORS['slate_light']}; font-weight:500; margin-top:6px;">Admin access</div></div>""", unsafe_allow_html=True)
    with c4:
        st.markdown(f"""<div class="user-stat-card"><div style="color:{COLORS['slate_light']}; font-size:11px; font-weight:600; text-transform:uppercase; letter-spacing:1px; margin-bottom:8px;">License Capacity</div><div style="font-size:32px; font-weight:800; color:{COLORS['slate']}">{50 - total_users}</div><div style="font-size:12px; color:{COLORS['blue']}; font-weight:500; margin-top:6px;">Available seats</div></div>""", unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    # --- ACTION BAR (SEARCH & FILTERS) ---
    st.markdown('<div class="section-title">Directory Management</div>', unsafe_allow_html=True)
    
    # Action bar container with proper alignment
    st.markdown(f"""
    <style>
        /* Custom action bar label styling */
        .action-bar-label {{
            font-size: 11px;
            font-weight: 600;
            color: {COLORS['slate_light']};
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 8px;
            height: 16px;
            display: block;
        }}
        
        /* HIDE all default Streamlit labels in the action bar */
        .st-key-user_search label[data-testid="stWidgetLabel"],
        .st-key-user_search .stTextInput label {{
            display: none !important;
            height: 0 !important;
            margin: 0 !important;
            padding: 0 !important;
        }}
        
        /* Ensure all inputs and selects have same height */
        .st-key-user_search [data-baseweb="base-input"],
        .stSelectbox [data-baseweb="select"] > div:first-child {{
            min-height: 42px !important;
            height: 42px !important;
        }}
        
        /* Match search input border with dropdown borders */
        .st-key-user_search [data-baseweb="input"],
        .st-key-user_search [data-baseweb="base-input"],
        .st-key-user_search div[data-testid="stTextInputRootElement"] {{
            border: 1px solid #d1d5db !important;
            border-radius: 8px !important;
            background: #ffffff !important;
            box-shadow: none !important;
        }}
        
        .st-key-user_search [data-baseweb="input"]:hover,
        .st-key-user_search [data-baseweb="base-input"]:hover,
        .st-key-user_search div[data-testid="stTextInputRootElement"]:hover {{
            border-color: #9ca3af !important;
        }}
        
        .st-key-user_search [data-baseweb="input"]:focus-within,
        .st-key-user_search [data-baseweb="base-input"]:focus-within,
        .st-key-user_search div[data-testid="stTextInputRootElement"]:focus-within {{
            border-color: #9ca3af !important;
            box-shadow: none !important;
        }}
        
        /* Remove inner border from base-input when parent has border */
        .st-key-user_search [data-baseweb="input"] [data-baseweb="base-input"] {{
            border: none !important;
        }}
        
        /* Align button with inputs */
        .stButton > button {{
            min-height: 42px !important;
            height: 42px !important;
        }}
    </style>
    """, unsafe_allow_html=True)
    
    with st.container():
        s1, s2, s3, s4 = st.columns([2.8, 1.4, 1.4, 1.2])
        with s1:
            st.markdown(f'<div class="action-bar-label">Search Users</div>', unsafe_allow_html=True)
            search_query = st.text_input("Search Users", placeholder="Search by name, email, or team...", key="user_search", label_visibility="collapsed")
        with s2:
            st.markdown(f'<div class="action-bar-label">Filter by Role</div>', unsafe_allow_html=True)
            role_filter = st.selectbox("Role Filter", ["All Roles", "Admin", "Executive", "Steward", "Developer"], label_visibility="collapsed")
        with s3:
            st.markdown(f'<div class="action-bar-label">Filter by Status</div>', unsafe_allow_html=True)
            status_filter = st.selectbox("Status Filter", ["All Status", "Active", "Inactive"], label_visibility="collapsed")
        with s4:
            st.markdown('<div class="action-bar-label">&nbsp;</div>', unsafe_allow_html=True)
            if st.button("Add New User", type="primary", use_container_width=True):
                st.session_state['show_add_user'] = True
                st.rerun()

    # --- USER ADDITION FORM (MAIN AREA) ---
    if st.session_state.get('show_add_user', False):
        # Inject Custom Form Styling to make the form itself the "Card"
        st.markdown(f"""
        <style>
            div[data-testid="stForm"] {{
                background-color: {COLORS['white']};
                border: 1px solid {COLORS['border']};
                border-radius: 12px;
                padding: 32px;
                box-shadow: 0 4px 12px rgba(0,0,0,0.05);
            }}
        </style>
        """, unsafe_allow_html=True)

        with st.form("new_user_form", border=False):
            # Header moved INSIDE the form
            st.markdown(f"""
            <div style="display:flex; justify-content:space-between; align-items:flex-start; margin-bottom: 24px;">
                <div>
                    <h3 style="margin:0 0 8px 0; color:{COLORS['slate']}; font-size:18px;">Provision New Identity</h3>
                    <p style="font-size:14px; color:{COLORS['slate_light']}; margin:0;">Configure access credentials and security profile for the new user.</p>
                </div>
                <div style="background:{COLORS['red_light']}; color:{COLORS['red']}; padding:4px 12px; border-radius:12px; font-size:11px; font-weight:700;">SECURE FORM</div>
            </div>
            """, unsafe_allow_html=True)

            f1, f2 = st.columns(2)
            with f1:
                new_username = st.text_input("Username (Login ID)", placeholder="e.g. jdoe")
                new_name = st.text_input("Full Name", placeholder="e.g. John Doe")
                new_role = st.selectbox("Role Assignment", ["Steward", "Admin", "Executive", "Developer"])
            with f2:
                new_email = st.text_input("Email Address", placeholder="e.g. j.doe@icore.io")
                new_pass = st.text_input("Temporary Password", type="password", value="welcome123")
                new_status = st.selectbox("Initial Account Status", ["Active", "Inactive"])
            
            st.markdown("<div style='margin-top:24px;'></div>", unsafe_allow_html=True)
            
            b1, b2, b3 = st.columns([1, 1, 4])
            with b1:
                if st.form_submit_button("Create Identity", type="primary", use_container_width=True):
                    if new_name and new_email and new_username:
                        user_data = {
                            "name": new_name,
                            "email": new_email,
                            "pass": new_pass,
                            "role": new_role,
                            "status": new_status,
                            "last_login": "Never"
                        }
                        st.session_state['user_manager'].add_user(new_username, user_data)
                        st.session_state['PLATFORM_USERS'] = st.session_state['user_manager'].get_users()
                        st.success(f"Identity {new_username} provisioned successfully.")
                        st.session_state['show_add_user'] = False
                        st.rerun()
                    else:
                        st.error("Missing required fields.")
            with b2:
                if st.form_submit_button("← Back", type="secondary", use_container_width=True):
                    st.session_state['show_add_user'] = False
                    st.rerun()

    # --- DATA FILTERING ---
    df_users = pd.DataFrame(st.session_state['users_db'])
    
    # Custom display logic for roles (adding icons/colors)
    # Note: Streamlit Dataframe doesn't support HTML in cells well, 
    # but we can use emojis or just leverage the SelectboxColumn colors.
    
    if search_query:
        df_users = df_users[df_users.apply(lambda r: search_query.lower() in str(r).lower(), axis=1)]
    if role_filter != "All Roles":
        df_users = df_users[df_users['role'] == role_filter]
    if status_filter != "All Status":
        df_users = df_users[df_users['status'] == status_filter]

    # --- RENDER TABLE ---
    if not df_users.empty:
        # We'll use Streamlit's Dataframe styling for the "Super Cool" look
        st.markdown(f"""
        <div style="background: white; border-radius: 12px; border: 1px solid {COLORS['border']}; padding: 1px;">
        """, unsafe_allow_html=True)
        
        # Color mapper for status and roles
        df_styled = df_users.copy()
        
        edited_df = st.data_editor(
            df_styled,
            use_container_width=True,
            hide_index=True,
            column_config={
                "username": st.column_config.TextColumn("Login ID", width="small", disabled=True),
                "name": st.column_config.TextColumn("User Name", width="medium"),
                "email": st.column_config.TextColumn("Email", width="medium"),
                "role": st.column_config.SelectboxColumn(
                    "Role",
                    options=["Admin", "Executive", "Steward", "Developer"],
                    required=True,
                    width="small"
                ),
                "status": st.column_config.SelectboxColumn(
                    "Access Status",
                    options=["Active", "Inactive"],
                    width="small"
                ),
                "last_login": st.column_config.TextColumn("Last Activity", width="small", disabled=True)
            },
            num_rows="dynamic",
            key="user_editor_live"
        )
        st.markdown("</div>", unsafe_allow_html=True)

        # Sync back to PLATFORM_USERS if changes occurred
        if not edited_df.equals(df_styled):
            # Rebuild the dictionary from the edited dataframe
            new_registry = {}
            for _, row in edited_df.iterrows():
                uname = row['username']
                # Preserve password if it was there
                old_pass = st.session_state['PLATFORM_USERS'].get(uname, {}).get("pass", "welcome123")
                new_registry[uname] = {
                    "name": row['name'],
                    "email": row['email'],
                    "role": row['role'],
                    "status": row['status'],
                    "last_login": row['last_login'],
                    "pass": old_pass
                }
            # Sync updates to persistence layer
            # We can just iterate and add/update all, since we reconstructed the full registry from the editor
            # Ideally we'd detect diffs, but for now full sync is fine for small lists
            manager = st.session_state['user_manager']
            
            # 1. Update/Add existing
            for uname, udata in new_registry.items():
                manager.add_user(uname, udata)
            
            # 2. Handle Deletions (if any rows were removed)
            current_users = manager.get_users()
            for old_uname in list(current_users.keys()):
                if old_uname not in new_registry:
                    manager.delete_user(old_uname)
            
            st.session_state['PLATFORM_USERS'] = manager.get_users()
            st.rerun()
        
        # Total count display
        st.markdown(f"""<div style="display:flex; justify-content:flex-end; font-size:12px; color:{COLORS['slate_light']}; margin-top:12px;">
            <span>Total: {len(df_users)} Identities</span>
        </div>""", unsafe_allow_html=True)
    else:
        st.info("No identities matching your search criteria were found.")
        if st.button("Clear all filters"):
             st.rerun()

    # --- FOOTER ---
    st.markdown(f"""
    <div style="margin-top: 48px; padding-top: 24px; border-top: 1px solid {COLORS['border']}; text-align: center;">
        <p style="font-size: 12px; color: {COLORS['slate_light']}; margin: 0;">© 2026 iCORE Enterprise Identity Management</p>
    </div>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    render()
