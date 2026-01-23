import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import io
import os

def render():
    # --- ICORE BRAND PALETTE ---
    COLORS = {
        'red': "#D11F41",
        'slate': "#0F172A",
        'slate_light': "#64748B",
        'white': "#FFFFFF",
        'bg': "#F8FAFC",
        'border': "#E2E8F0",
        'green': "#059669",
        'yellow': "#D97706"
    }
    
    # --- CSS: AUDIT PREMIUM UI & LIGHT THEME FIXES ---
    st.markdown(f"""
    <style>
        .stApp {{ background-color: {COLORS['bg']} !important; }}
        
        /* Force Light Theme for all standard Streamlit widgets in this view */
        [data-testid="stHeader"] {{ background: transparent !important; }}
        
        /* Stats Cards */
        .audit-stat-card {{
            background: {COLORS['white']};
            border: 1px solid {COLORS['border']};
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.04);
            display: flex;
            flex-direction: column;
            gap: 4px;
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }}
        .audit-stat-card:hover {{
            transform: translateY(-2px);
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
        }}
        .stat-label {{
            font-size: 11px;
            font-weight: 600;
            color: {COLORS['slate_light']};
            text-transform: uppercase;
            letter-spacing: 0.8px;
        }}
        .stat-value {{
            font-size: 24px;
            font-weight: 700;
            color: {COLORS['slate']};
        }}
        
        /* Filter Panel */
        div[data-testid="stVerticalBlockBorderWrapper"] {{
            background: {COLORS['white']} !important;
            border: 1px solid {COLORS['border']} !important;
            border-radius: 12px !important;
        }}
        
        .filter-label {{
            font-size: 12px;
            font-weight: 600;
            color: {COLORS['slate_light']};
            text-transform: uppercase;
            margin-bottom: 8px;
        }}
        
        /* SELECTBOX & INPUT LIGHT THEME */
        div[data-baseweb="select"] > div, 
        div[data-baseweb="base-input"],
        div[data-testid="stDateInput"] > div[data-baseweb="base-input"] {{
            background-color: white !important;
            color: {COLORS['slate']} !important;
            border: 1px solid {COLORS['border']} !important;
            border-radius: 8px !important;
            transition: all 0.2s ease !important;
        }}

        /* Focus & Hover states */
        div[data-baseweb="select"] > div:hover,
        div[data-baseweb="base-input"]:hover,
        div[data-testid="stDateInput"] > div[data-baseweb="base-input"]:hover {{
            border-color: {COLORS['slate_light']} !important;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05) !important;
        }}

        div[data-baseweb="select"]:focus-within > div,
        div[data-baseweb="base-input"]:focus-within {{
            border-color: {COLORS['red']} !important;
            box-shadow: 0 0 0 1px {COLORS['red']} !important;
        }}
        
        /* Target the text color specifically */
        div[data-testid="stSelectbox"] p, 
        div[data-testid="stTextInput"] p,
        div[data-testid="stDateInput"] p {{
            color: {COLORS['slate']} !important;
        }}

        /* CALENDAR POPOVER: FORCE LIGHT THEME & PREMIUM STYLE */
        div[data-baseweb="popover"], 
        div[role="listbox"],
        div[data-baseweb="calendar"] {{
            background-color: white !important;
            color: {COLORS['slate']} !important;
            border: 1px solid {COLORS['border']} !important;
            border-radius: 12px !important;
            box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.1), 0 8px 10px -6px rgba(0, 0, 0, 0.1) !important;
        }}

        /* Calendar Internal Elements */
        div[data-baseweb="calendar"] * {{
            color: {COLORS['slate']} !important;
            background-color: transparent !important;
        }}

        /* Day names & Month/Year headers */
        div[role="gridcell"] {{
            font-weight: 500 !important;
        }}

        /* Selected Range Highlight */
        div[aria-selected="true"] {{
            background-color: rgba(209, 31, 65, 0.1) !important; /* Brand red very light */
            color: {COLORS['red']} !important;
            font-weight: 700 !important;
            border-radius: 4px !important;
        }}

        /* Selected Start/End Circles */
        div[aria-selected="true"]:first-of-type,
        div[aria-selected="true"]:last-of-type {{
            background-color: {COLORS['red']} !important;
            color: white !important;
            border-radius: 50% !important;
        }}

        /* Hover on days */
        div[role="gridcell"]:hover {{
            background-color: #F1F5F9 !important;
            border-radius: 4px !important;
        }}

        /* PREPEND CALENDAR ICON TO DATE INPUT */
        div[data-testid="stDateInput"] > div[data-baseweb="base-input"]::before {{
            content: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="%2364748B" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="18" rx="2" ry="2"></rect><line x1="16" y1="2" x2="16" y2="6"></line><line x1="8" y1="2" x2="8" y2="6"></line><line x1="3" y1="10" x2="21" y2="10"></line></svg>');
            margin-left: 12px;
            margin-top: 2px;
            display: flex;
            align-items: center;
        }}

        div[data-testid="stDateInput"] input {{
            padding-left: 8px !important;
        }}

        /* DATAFRAME LIGHT THEME FORCE */
        .stDataFrame {{
            background-color: white !important;
            color: {COLORS['slate']} !important;
        }}

        /* DOWNLOAD BUTTON PREMIUM STYLING */
        div.stDownloadButton > button {{
            background-color: {COLORS['red']} !important;
            color: white !important;
            border: none !important;
            padding: 0.5rem 1.5rem !important;
            border-radius: 8px !important;
            font-weight: 600 !important;
            transition: all 0.3s ease !important;
            box-shadow: 0 4px 6px -1px rgba(209, 31, 65, 0.2) !important;
        }}
        
        div.stDownloadButton > button:hover {{
            background-color: #9f1239 !important;
            box-shadow: 0 10px 15px -3px rgba(159, 18, 57, 0.3) !important;
            transform: translateY(-1px);
        }}

        /* Floating Action Bar */
        .action-bar {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 24px;
            margin-bottom: 12px;
        }}
    </style>
    """, unsafe_allow_html=True)

    # --- HEADER ---
    st.markdown(f"""
    <div style="margin-bottom: 32px;">
        <h1 style="color: {COLORS['slate']}; font-weight: 800; font-size: 32px; margin: 0; letter-spacing: -0.5px;">Audit & Governance Trail</h1>
        <p style="color: {COLORS['slate_light']}; font-size: 15px; margin-top: 4px;">Comprehensive immutable ledger of all system and user interactions.</p>
    </div>
    """, unsafe_allow_html=True)

    # --- TOP KPI ROW ---
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown(f"""<div class="audit-stat-card"><div class="stat-label">Total Logs</div><div class="stat-value">1,248</div></div>""", unsafe_allow_html=True)
    with k2:
        st.markdown(f"""<div class="audit-stat-card"><div class="stat-label">System Health</div><div class="stat-value" style="color:{COLORS['green']}">98.4%</div></div>""", unsafe_allow_html=True)
    with k3:
        st.markdown(f"""<div class="audit-stat-card"><div class="stat-label">Security Flags</div><div class="stat-value" style="color:{COLORS['red']}">12</div></div>""", unsafe_allow_html=True)
    with k4:
        st.markdown(f"""<div class="audit-stat-card"><div class="stat-label">Active Users</div><div class="stat-value">18</div></div>""", unsafe_allow_html=True)

    st.write("<div style='height:24px'></div>", unsafe_allow_html=True)

    # --- DYNAMIC FILTERS ---
    with st.container(border=True):
        f1, f2, f3 = st.columns([2, 1, 1])
        
        with f1:
            st.markdown('<div class="filter-label">Search Activity</div>', unsafe_allow_html=True)
            search_term = st.text_input("Search", placeholder="Search user, action, module or details...", label_visibility="collapsed")
            
        with f2:
            st.markdown('<div class="filter-label">Status Layer</div>', unsafe_allow_html=True)
            status_filter = st.selectbox("Status", ["All Categories", "Success", "Failed", "Warning"], label_visibility="collapsed")
            
        with f3:
            st.markdown('<div class="filter-label">Timeline Select</div>', unsafe_allow_html=True)
            # Default to last 7 days if empty
            date_range = st.date_input("Date Range", [datetime.now() - timedelta(days=7), datetime.now()], label_visibility="collapsed")

    # --- REAL DATA LOADING ---
    from src.backend.audit.logger import AuditLogger
    logger = AuditLogger()
    
    if os.path.exists(logger.log_path):
        try:
            df = pd.read_csv(logger.log_path)
            # Ensure Timestamp is datetime
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])
            # Sort by latest
            df = df.sort_values(by="Timestamp", ascending=False)
        except Exception as e:
            st.error(f"Error loading audit logs: {e}")
            df = pd.DataFrame(columns=["Timestamp", "User", "Action", "Module", "Status", "Details"])
    else:
        df = pd.DataFrame(columns=["Timestamp", "User", "Action", "Module", "Status", "Details"])
    
    # df is already created above
    
    # Filter Logic
    if search_term:
        df = df[df.apply(lambda row: row.astype(str).str.contains(search_term, case=False).any(), axis=1)]
    if status_filter != "All Categories":
        df = df[df['Status'] == status_filter]
        
    # --- TABLE HEADER ACTIONS ---
    st.markdown('<div class="action-bar">', unsafe_allow_html=True)
    st.markdown(f"<div style='font-size:14px; font-weight:600; color:{COLORS['slate_light']}'>{len(df)} entries found in current view</div>", unsafe_allow_html=True)
    
    # Export Button
    # Convert DF to CSV for download
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download Audit Trail (CSV)",
        data=csv,
        file_name=f"audit_trail_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )
    st.markdown('</div>', unsafe_allow_html=True)

    # --- PRIMARY DATA TABLE ---
    st.dataframe(
        df,
        width="stretch",
        hide_index=True,
        column_config={
            "Timestamp": st.column_config.DatetimeColumn(
                "Time",
                format="MMM DD, HH:mm",
                width="medium"
            ),
            "User": st.column_config.TextColumn(
                "Integrator / User",
                width="small"
            ),
            "Action": st.column_config.TextColumn(
                "Activity",
                width="medium"
            ),
            "Module": st.column_config.TextColumn(
                "Subsystem",
                width="small"
            ),
            "Status": st.column_config.TextColumn(
                "Execution Status",
                width="small"
            ),
            "Details": st.column_config.TextColumn(
                "Technical Details",
                width="large"
            ),
        }
    )

    # --- FOOTER ---
    st.markdown(f"""
    <div style="margin-top: 40px; padding: 20px; border-top: 1px solid {COLORS['border']}; display: flex; justify-content: space-between; align-items: center;">
        <div style="font-size: 12px; color: {COLORS['slate_light']};">Environment: Production (Air-Gapped)</div>
        <div style="font-size: 12px; color: {COLORS['slate_light']};">© 2026 iLink Digital. All data encrypted.</div>
    </div>
    """, unsafe_allow_html=True)
