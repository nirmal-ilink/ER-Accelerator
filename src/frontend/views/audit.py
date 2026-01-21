import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

def render():
    # --- I-LINK BRAND PALETTE ---
    PRIMARY_RED = "#D11F41"
    DARK_SLATE = "#0F172A"
    LIGHT_SLATE = "#64748B"
    WHITE = "#FFFFFF"
    
    # --- CSS: AUDIT SPECIFIC ---
    st.markdown(f"""
    <style>
        .audit-header-card {{
            background-color: {WHITE};
            border: 1px solid #E2E8F0;
            border-radius: 12px;
            padding: 24px;
            margin-bottom: 24px;
            box-shadow: 0 1px 2px rgba(0,0,0,0.05);
        }}
        
        .filter-label {{
            font-size: 12px;
            font-weight: 600;
            color: {LIGHT_SLATE};
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 8px;
        }}
        
        /* Table Styling Overrides (Streamlit standard table is tough to style fully via CSS, 
           but we can wrap it effectively) */
        .stDataFrame {{
            background-color: {WHITE};
            border: 1px solid #E2E8F0;
            border-radius: 12px;
            padding: 16px;
            box-shadow: 0 1px 2px rgba(0,0,0,0.05);
        }}
    </style>
    """, unsafe_allow_html=True)

    # --- HEADER ---
    st.markdown(f"""
    <div style="margin-bottom: 32px; padding-left: 4px;">
        <h1 style="color: {DARK_SLATE}; font-weight: 800; font-size: 28px; margin: 0; letter-spacing: -0.5px;">Audit Logs</h1>
        <p style="color: {LIGHT_SLATE}; font-size: 15px; margin-top: 4px;">Track system activities, data modifications, and user access history.</p>
    </div>
    """, unsafe_allow_html=True)

    # --- FILTERS ---
    st.markdown(f"""<div class="audit-header-card">""", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([2, 1, 1])
    
    with c1:
        st.markdown('<div class="filter-label">Search Activity</div>', unsafe_allow_html=True)
        search_term = st.text_input("Search", placeholder="Search by user, action, or details...", label_visibility="collapsed")
        
    with c2:
        st.markdown('<div class="filter-label">Status</div>', unsafe_allow_html=True)
        status_filter = st.selectbox("Status", ["All", "Success", "Failed", "Warning"], label_visibility="collapsed")
        
    with c3:
        st.markdown('<div class="filter-label">Time Range</div>', unsafe_allow_html=True)
        date_range = st.date_input("Date", [], label_visibility="collapsed")

    st.markdown("</div>", unsafe_allow_html=True)

    # --- DATA & TABLE ---
    # Mock Data
    data = [
        {"Timestamp": datetime.now() - timedelta(minutes=5), "User": "Admin", "Action": "User Login", "Module": "Auth", "Status": "Success", "Details": "Logged in from 192.168.1.5"},
        {"Timestamp": datetime.now() - timedelta(minutes=15), "User": "Data Steward", "Action": "Record Merge", "Module": "Match Review", "Status": "Success", "Details": "Merged ID-8821 with ID-9921"},
        {"Timestamp": datetime.now() - timedelta(hours=1), "User": "System", "Action": "Auto-Import", "Module": "Connectors", "Status": "Failed", "Details": "Connection timeout: EMR_DB_01"},
        {"Timestamp": datetime.now() - timedelta(hours=2), "User": "Executive", "Action": "Export Report", "Module": "Dashboard", "Status": "Success", "Details": "Exported 'Q1_Quality.pdf'"},
        {"Timestamp": datetime.now() - timedelta(hours=5), "User": "Admin", "Action": "Policy Update", "Module": "Settings", "Status": "Warning", "Details": "Changed threshold 0.8 -> 0.85"},
        {"Timestamp": datetime.now() - timedelta(days=1), "User": "Data Steward", "Action": "Unmerge", "Module": "Match Review", "Status": "Success", "Details": "Reverted merge #4421"},
    ]
    
    # Filter Logic (Basic)
    df = pd.DataFrame(data)
    if search_term:
        df = df[df.apply(lambda row: row.astype(str).str.contains(search_term, case=False).any(), axis=1)]
    if status_filter != "All":
        df = df[df['Status'] == status_filter]

    # Display using powerful column config
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Timestamp": st.column_config.DatetimeColumn(
                "Time",
                format="D MMM, HH:mm",
            ),
            "User": st.column_config.TextColumn("User"),
            "Action": st.column_config.TextColumn("Action", width="medium"),
            "Status": st.column_config.TextColumn(
                "Status",
                width="small", 
                # Basic coloring simulation via text - Streamlit 1.25+ supports this via pandas styler best, 
                # but column_config is cleaner for raw data.
            ),
        }
    )
