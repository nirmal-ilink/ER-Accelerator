"""
Data Connectors Page - Enterprise data source connection management.

This module provides a streamlined workflow for configuring data connectors:
1. Select connector type and enter credentials
2. Provide a unique connection name
3. Test connection and save configuration

Schema/table selection and load configuration are handled in the Pipeline Inspector.
Supports: SQL Server, Snowflake, Databricks, Oracle, SAP HANA, Delta Lake.
"""

import streamlit as st
import os
import base64
import time
import socketserver

# Windows Compatibility Patch for Spark/Databricks Connect
if not hasattr(socketserver, "UnixStreamServer"):
    class UnixStreamServer:
        pass
    socketserver.UnixStreamServer = UnixStreamServer


def get_img_as_base64(file_path):
    """Load image as base64 for embedding in HTML."""
    try:
        with open(file_path, "rb") as f:
            data = f.read()
        return base64.b64encode(data).decode()
    except Exception:
        return ""


def render():
    """Main render function for the connectors page."""
    assets_dir = os.path.join(os.path.dirname(__file__), "../../../assets")

    # ==========================================================================
    # CONNECTOR DEFINITIONS
    # ==========================================================================
    CONNECTORS = {
        "sqlserver": {
            "name": "SQL Server",
            "logo": "sqlserver_logo.png",
            "status": "Inactive",
            "desc": "Microsoft SQL Server / Azure SQL",
            "fields": ["Server", "Database", "User", "Password"],
            "supports_schema_browser": True,
        },
        "snowflake": {
            "name": "Snowflake",
            "logo": "snowflake_logo.png",
            "status": "Active",
            "desc": "Data Warehouse",
            "fields": ["Account", "User", "Password", "Warehouse", "Database", "Schema"],
            "supports_schema_browser": False,
        },
        "databricks": {
            "name": "Databricks",
            "logo": "databricks_logo.png",
            "status": "Inactive",
            "desc": "Unity Catalog - Lakehouse Platform",
            "fields": ["Host", "Token", "HTTP Path"],
            "supports_schema_browser": True,
            "supports_catalog_selection": True,  # Shows catalog dropdown after connection
        },
        "oracle": {
            "name": "Oracle DB",
            "logo": "oracle_logo.png",
            "status": "Inactive",
            "desc": "RDBMS",
            "fields": ["Host", "Port", "Service", "User", "Password"],
            "supports_schema_browser": False,
        },
        "sap": {
            "name": "SAP HANA",
            "logo": "sap_logo.png",
            "status": "Inactive",
            "desc": "In-Memory DB",
            "fields": ["Host", "Instance", "User", "Password"],
            "supports_schema_browser": False,
        },
        "deltalake": {
            "name": "Delta Lake",
            "logo": "deltalake_logo.png",
            "status": "Inactive",
            "desc": "Storage Layer",
            "fields": ["Account", "Container", "Key"],
            "supports_schema_browser": False,
        },
        "fabric": {
            "name": "Microsoft Fabric",
            "logo": "fabric_logo.png",
            "status": "Inactive",
            "desc": "Data Warehouse (Warehouse)",
            "fields": ["Tenant ID", "Client ID", "Workspace ID", "Client Secret", "Table Name"],
            "supports_schema_browser": True,
        },
    }

    # ==========================================================================
    # SESSION STATE INITIALIZATION
    # ==========================================================================
    if "conn_selection" not in st.session_state:
        st.session_state["conn_selection"] = "SQL Server"
    
    if "schema_metadata" not in st.session_state:
        st.session_state["schema_metadata"] = None
    
    if "selected_tables" not in st.session_state:
        st.session_state["selected_tables"] = {}
    
    if "connection_tested" not in st.session_state:
        st.session_state["connection_tested"] = False
    
    if "fetch_error" not in st.session_state:
        st.session_state["fetch_error"] = None
    
    if "existing_connector_config" not in st.session_state:
        st.session_state["existing_connector_config"] = None
    
    if "sync_history" not in st.session_state:
        st.session_state["sync_history"] = []
    
    # ==========================================================================
    # AUTO-DETECT EXISTING CONFIG (for incremental mode)
    # ==========================================================================
    def _check_existing_config(connector_type: str):
        """Check if existing config exists and load it."""
        try:
            from src.backend.connectors import get_connector_service
            service = get_connector_service()
            existing = service.load_configuration(connector_type)
            if existing:
                return {
                    "last_sync_time": existing.last_sync_time or "Never",
                    "schedule_enabled": existing.schedule_enabled,
                    "schedule_cron": existing.schedule_cron,
                    "schedule_timezone": existing.schedule_timezone,
                }
        except Exception:
            pass
        return None

    # ==========================================================================
    # CSS STYLES
    # ==========================================================================
    st.markdown("""
    <style>
        /* =====================================================================
           CONNECTOR PAGE STYLES
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
        
        /* DROPDOWN ITEMS */
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
        
        /* REMOVE ANY DARK BORDERS/OUTLINES (only ::after, ::before is used for logos) */
        li[role="option"]::after,
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
           CONNECTORS PAGE BUTTONS - PLATFORM STYLE (MAIN CONTENT ONLY)
           ================================================================= */
        
        /* Secondary Buttons - Default State */
        [data-testid="stMain"] button[data-testid="stBaseButton-secondary"] {
            background: #ffffff !important;
            background-color: #ffffff !important;
            border: 1px solid rgba(15, 23, 42, 0.2) !important;
            color: #0f172a !important;
            height: 42px !important;
            border-radius: 12px !important;
            font-size: 14px !important;
            font-weight: 500 !important;
            box-shadow: 0 1px 2px rgba(0, 0, 0, 0.05) !important;
            transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1) !important;
        }
        
        /* Text inside secondary buttons */
        [data-testid="stMain"] button[data-testid="stBaseButton-secondary"] p,
        [data-testid="stMain"] button[data-testid="stBaseButton-secondary"] span,
        [data-testid="stMain"] button[data-testid="stBaseButton-secondary"] div {
            color: #0f172a !important;
            transition: color 0.25s ease !important;
        }
        
        /* Hover State - Elevation + Red Glow */
        [data-testid="stMain"] button[data-testid="stBaseButton-secondary"]:hover {
            background: #d11f41 !important;
            background-color: #d11f41 !important;
            border-color: #d11f41 !important;
            color: #ffffff !important;
            transform: translateY(-2px) !important;
            box-shadow: 0 10px 20px -5px rgba(209, 31, 65, 0.4), 0 0 20px rgba(209, 31, 65, 0.15) !important;
        }
        
        [data-testid="stMain"] button[data-testid="stBaseButton-secondary"]:hover p,
        [data-testid="stMain"] button[data-testid="stBaseButton-secondary"]:hover span,
        [data-testid="stMain"] button[data-testid="stBaseButton-secondary"]:hover div {
            color: #ffffff !important;
        }
        
        /* Active/Click State - Tactile Press */
        [data-testid="stMain"] button[data-testid="stBaseButton-secondary"]:active {
            transform: scale(0.98) translateY(0) !important;
            transition: transform 0.1s !important;
            background: #b91c1c !important;
            background-color: #b91c1c !important;
            filter: brightness(0.95) !important;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1) !important;
        }
        
        /* Primary Buttons - Already Red */
        [data-testid="stMain"] button[data-testid="stBaseButton-primary"] {
            background: #d11f41 !important;
            background-color: #d11f41 !important;
            border-color: #d11f41 !important;
            color: #ffffff !important;
            height: 42px !important;
            border-radius: 12px !important;
            font-size: 14px !important;
            font-weight: 600 !important;
            box-shadow: 0 4px 6px -1px rgba(209, 31, 65, 0.25) !important;
            transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1) !important;
        }
        
        [data-testid="stMain"] button[data-testid="stBaseButton-primary"] p,
        [data-testid="stMain"] button[data-testid="stBaseButton-primary"] span,
        [data-testid="stMain"] button[data-testid="stBaseButton-primary"] div {
            color: #ffffff !important;
        }
        
        /* Primary Hover - Darker + Glow */
        [data-testid="stMain"] button[data-testid="stBaseButton-primary"]:hover {
            background: #9f1239 !important;
            background-color: #9f1239 !important;
            transform: translateY(-2px) !important;
            box-shadow: 0 10px 15px -3px rgba(159, 18, 57, 0.4) !important;
        }
        
        /* Primary Active - Tactile */
        [data-testid="stMain"] button[data-testid="stBaseButton-primary"]:active {
            transform: scale(0.98) translateY(0) !important;
            transition: transform 0.1s !important;
        }


        
        /* =================================================================
           SCHEMA BROWSER - EXPANDERS & CHECKBOXES
           ================================================================= */
        
        /* Expander Container */
        [data-testid="stMain"] [data-testid="stExpander"] {
            background: #ffffff !important;
            border: 1px solid #e5e7eb !important;
            border-radius: 12px !important;
            margin-bottom: 12px !important;
            overflow: hidden !important;
        }
        
        /* Expander Header - Fix Dark Background */
        [data-testid="stMain"] [data-testid="stExpander"] summary,
        [data-testid="stMain"] [data-testid="stExpander"] [data-testid="stExpanderHeader"],
        [data-testid="stMain"] .streamlit-expanderHeader {
            background: #f8fafc !important;
            background-color: #f8fafc !important;
            color: #1e293b !important;
            padding: 14px 18px !important;
            font-weight: 600 !important;
            font-size: 14px !important;
            border-bottom: 1px solid #e5e7eb !important;
        }
        
        /* Expander Header Text - Force Visibility */
        [data-testid="stMain"] [data-testid="stExpander"] summary p,
        [data-testid="stMain"] [data-testid="stExpander"] summary span,
        [data-testid="stMain"] [data-testid="stExpanderHeader"] p,
        [data-testid="stMain"] .streamlit-expanderHeader p,
        [data-testid="stMain"] .streamlit-expanderHeader span {
            color: #1e293b !important;
            background: transparent !important;
        }
        
        /* Expander Header Hover */
        [data-testid="stMain"] [data-testid="stExpander"] summary:hover,
        [data-testid="stMain"] .streamlit-expanderHeader:hover {
            background: #f1f5f9 !important;
        }
        
        /* Expander Content Area */
        [data-testid="stMain"] [data-testid="stExpander"] [data-testid="stExpanderDetails"],
        [data-testid="stMain"] .streamlit-expanderContent {
            background: #ffffff !important;
            padding: 16px !important;
        }
        
        /* Checkbox Styling - Make Labels Visible */
        [data-testid="stMain"] [data-testid="stCheckbox"] label,
        [data-testid="stMain"] .stCheckbox label {
            color: #374151 !important;
            font-size: 13px !important;
            font-weight: 500 !important;
        }
        
        [data-testid="stMain"] [data-testid="stCheckbox"] label p,
        [data-testid="stMain"] [data-testid="stCheckbox"] label span {
            color: #374151 !important;
        }
        
        /* Checkbox Box Styling */
        [data-testid="stMain"] [data-testid="stCheckbox"] input[type="checkbox"] {
            accent-color: #d11f41 !important;
        }
        
        /* Selected Checkbox Label */
        [data-testid="stMain"] [data-testid="stCheckbox"] input:checked + label,
        [data-testid="stMain"] [data-testid="stCheckbox"] input:checked ~ label {
            color: #0f172a !important;
            font-weight: 600 !important;
        }
        
        /* Schema Count Badges */
        .schema-badge {
            display: inline-flex;
            align-items: center;
            padding: 4px 10px;
            border-radius: 16px;
            font-size: 12px;
            font-weight: 600;
        }
        .schema-badge.total {
            background: #e2e8f0;
            color: #475569;
        }
        .schema-badge.selected {
            background: #dcfce7;
            color: #166534;
        }
        
        /* Table Count Badge in Expander Header */
        .table-count-badge {
            background: #e5e7eb;
            color: #374151;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 600;
        }
        
        .selected-count-badge {
            background: #dcfce7;
            color: #166534;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 600;
        }
        
        /* =================================================================
           CONNECTION ID CODE BLOCK - LIGHT THEME
           ================================================================= */
        
        /* Override Streamlit's dark code block with light theme */
        [data-testid="stMain"] [data-testid="stCode"] {
            background: #f8fafc !important;
        }
        
        [data-testid="stMain"] [data-testid="stCode"] > div {
            background: #f8fafc !important;
        }
        
        [data-testid="stMain"] [data-testid="stCode"] pre {
            background: #f8fafc !important;
            border: 1px solid #e2e8f0 !important;
            border-radius: 8px !important;
            padding: 12px 16px !important;
        }
        
        /* Disable the hover highlight overlay that hides text */
        [data-testid="stMain"] [data-testid="stCode"] pre::before,
        [data-testid="stMain"] [data-testid="stCode"] pre::after,
        [data-testid="stMain"] [data-testid="stCode"] > div::before,
        [data-testid="stMain"] [data-testid="stCode"] > div::after {
            display: none !important;
            content: none !important;
        }
        
        /* Keep background stable on all hover states */
        [data-testid="stMain"] [data-testid="stCode"]:hover,
        [data-testid="stMain"] [data-testid="stCode"] > div:hover,
        [data-testid="stMain"] [data-testid="stCode"] pre:hover {
            background: #f8fafc !important;
        }
        
        [data-testid="stMain"] [data-testid="stCode"] code {
            color: #1e293b !important;
            font-family: 'SF Mono', 'Monaco', 'Consolas', 'Courier New', monospace !important;
            font-size: 13px !important;
            background: transparent !important;
        }
        
        /* Force text to stay visible in all hover states */
        [data-testid="stMain"] [data-testid="stCode"]:hover code,
        [data-testid="stMain"] [data-testid="stCode"] code:hover,
        [data-testid="stMain"] [data-testid="stCode"] pre:hover code,
        [data-testid="stMain"] [data-testid="stCode"] > div:hover code {
            color: #1e293b !important;
            background: transparent !important;
            opacity: 1 !important;
            visibility: visible !important;
        }
        
        /* Polished copy button styling - Red accent */
        [data-testid="stMain"] [data-testid="stCode"] button {
            background: linear-gradient(135deg, #fff5f5 0%, #fee2e2 100%) !important;
            border: 1px solid #fca5a5 !important;
            border-radius: 6px !important;
            padding: 6px 10px !important;
            box-shadow: 0 1px 3px rgba(220, 38, 38, 0.1) !important;
            transition: all 0.2s ease !important;
        }
        
        [data-testid="stMain"] [data-testid="stCode"] button:hover {
            background: linear-gradient(135deg, #fee2e2 0%, #fecaca 100%) !important;
            border-color: #f87171 !important;
            box-shadow: 0 2px 6px rgba(220, 38, 38, 0.2) !important;
            transform: translateY(-1px) !important;
        }
        
        [data-testid="stMain"] [data-testid="stCode"] button svg {
            stroke: #dc2626 !important;
            width: 16px !important;
            height: 16px !important;
        }
        
        [data-testid="stMain"] [data-testid="stCode"] button:hover svg {
            stroke: #b91c1c !important;
        }

    </style>
    """, unsafe_allow_html=True)

    # ==========================================================================
    # DYNAMIC LOGO CSS INJECTION (must come AFTER main CSS to override)
    # ==========================================================================
    logo_css_rules = []
    for idx, (_, conn_data) in enumerate(CONNECTORS.items()):
        logo_file = conn_data.get("logo")
        if logo_file:
            logo_path = os.path.join(assets_dir, logo_file)
            b64_logo = get_img_as_base64(logo_path)
            if b64_logo:
                # nth-child is 1-based; use high-specificity selectors
                rule = f"""
                div[data-baseweb="popover"] ul li:nth-child({idx + 1})::before,
                ul[data-testid="stSelectboxVirtualDropdown"] li:nth-child({idx + 1})::before,
                ul[role="listbox"] li:nth-child({idx + 1})::before {{
                    content: "" !important;
                    display: inline-block !important;
                    width: 22px !important;
                    height: 22px !important;
                    min-width: 22px !important;
                    margin-right: 10px !important;
                    background-image: url("data:image/png;base64,{b64_logo}") !important;
                    background-size: contain !important;
                    background-repeat: no-repeat !important;
                    background-position: center !important;
                    vertical-align: middle !important;
                    opacity: 1 !important;
                    visibility: visible !important;
                    flex-shrink: 0 !important;
                }}
                div[data-baseweb="popover"] ul li:nth-child({idx + 1}),
                ul[data-testid="stSelectboxVirtualDropdown"] li:nth-child({idx + 1}),
                ul[role="listbox"] li:nth-child({idx + 1}) {{
                    display: flex !important;
                    align-items: center !important;
                }}
                """
                logo_css_rules.append(rule)

    if logo_css_rules:
        st.markdown(f"<style>{''.join(logo_css_rules)}</style>", unsafe_allow_html=True)

    # ==========================================================================
    # PAGE HEADER
    # ==========================================================================
    st.markdown("### Data Connectors")
    st.markdown(
        "<p style='color:#6b7280; font-size:14px; margin-top:-10px;'>"
        "Configure and save your enterprise data source connections."
        "</p>",
        unsafe_allow_html=True,
    )

    # ==========================================================================
    # CONNECTOR SELECTOR
    # ==========================================================================
    conn_list = list(CONNECTORS.values())
    option_names = [c["name"] for c in conn_list]

    def get_key(name):
        for k, v in CONNECTORS.items():
            if v["name"] == name:
                return k
        return list(CONNECTORS.keys())[0]

    current_idx = (
        option_names.index(st.session_state.get("conn_selection", "SQL Server"))
        if st.session_state.get("conn_selection") in option_names
        else 0
    )

    col1, col2 = st.columns([1, 3])
    with col1:
        selected_name = st.selectbox(
            "Select",
            options=option_names,
            index=current_idx,
            label_visibility="collapsed",
            key="connector_select",
        )

    # Handle connector change - reset state
    if st.session_state.get("conn_selection") != selected_name:
        st.session_state["schema_metadata"] = None
        st.session_state["selected_tables"] = {}
        st.session_state["connection_tested"] = False
        st.session_state["fetch_error"] = None
    
    st.session_state["conn_selection"] = selected_name

    # Get active connector data
    active_data = next(
        (item for item in conn_list if item["name"] == selected_name), conn_list[0]
    )
    conn_key = get_key(selected_name)
    logo_path = os.path.join(assets_dir, active_data["logo"])
    logo_b64 = get_img_as_base64(logo_path)
    # Status is active if historically active OR if connection test passed in this session
    is_active = active_data["status"] == "Active" or st.session_state.get("connection_tested", False)
    supports_schema_browser = active_data.get("supports_schema_browser", False)

    st.write("")

    # ==========================================================================
    # CONNECTOR CARD
    # ==========================================================================
    with st.container(border=True):
        # Header row
        hc1, hc2 = st.columns([6, 1])
        with hc1:
            st.markdown(
                f"""
            <div style="display:flex; align-items:center; gap:14px;">
                <img src="data:image/png;base64,{logo_b64}" style="width:44px; height:44px; object-fit:contain;">
                <div>
                    <div style="font-size:17px; font-weight:600; color:#111827; margin-bottom:2px;">{active_data['name']}</div>
                    <div style="font-size:13px; color:#6b7280;">{active_data['desc']}</div>
                </div>
            </div>
            """,
                unsafe_allow_html=True,
            )

        with hc2:
            # Blinker updates based on test connection result
            test_status = st.session_state.get(f"{conn_key}_test_status", "inactive" if not is_active else "active")
            if test_status == "active":
                st.markdown(
                    """<div style="background:#dcfce7; color:#166534; padding:6px 12px; border-radius:20px; font-size:12px; font-weight:600; display:inline-flex; align-items:center; gap:6px; float:right;">
                    <span style="width:6px; height:6px; background:#22c55e; border-radius:50%; animation: blink-green 1.5s infinite;"></span>Active</div>
                    <style>@keyframes blink-green { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }</style>""",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    """<div style="background:#f3f4f6; color:#6b7280; padding:6px 12px; border-radius:20px; font-size:12px; font-weight:600; display:inline-flex; align-items:center; gap:6px; float:right;">
                    <span style="width:6px; height:6px; background:#9ca3af; border-radius:50%;"></span>Inactive</div>""",
                    unsafe_allow_html=True,
                )

        st.markdown(
            "<hr style='border:none; border-top:1px solid #e5e7eb; margin:16px 0;'>",
            unsafe_allow_html=True,
        )

        # ==========================================================================
        # STEP 1: CONNECTION CONFIGURATION
        # ==========================================================================
        st.markdown(
            "<div style='font-size:12px; font-weight:600; color:#6b7280; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px;'>"
            "Step 1: Connection Configuration"
            "</div>",
            unsafe_allow_html=True,
        )

        # Connection Name input (user-scoped, unique)
        st.text_input(
            "CONNECTION NAME",
            key=f"{conn_key}_connection_name",
            placeholder="e.g. Production SQL, Dev Warehouse",
            help="A unique name for this connection. Used to identify it in the Pipeline Inspector.",
        )

        # Input fields (2 columns)
        c1, c2 = st.columns(2)
        for i, f in enumerate(active_data["fields"]):
            with c1 if i % 2 == 0 else c2:
                is_secret = any(s in f.lower() for s in ["password", "token", "key", "secret"])
                st.text_input(
                    f.upper(),
                    key=f"{conn_key}_{f.lower().replace(' ', '_')}",
                    type="password" if is_secret else "default",
                )

        st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)

        # ==========================================================================
        # ACTION BUTTONS: Test Connection + Save Connection
        # ==========================================================================
        btn_col1, _, btn_col3 = st.columns([1.2, 2, 1.2])
        
        with btn_col1:
            test_clicked = st.button(
                "Test Connection",
                key=f"test_{conn_key}",
                use_container_width=True,
            )
        
        with btn_col3:
            save_clicked = st.button(
                "Save Connection",
                key=f"save_{conn_key}",
                type="primary",
                use_container_width=True,
            )
        
        # Handle Test Connection
        if test_clicked:
            _handle_test_connection(conn_key, active_data)
        
        # Handle Save Connection
        if save_clicked:
            _handle_save_configuration(conn_key, active_data)



# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def _count_selected_tables() -> int:
    """Count total selected tables across all schemas."""
    selected = st.session_state.get("selected_tables", {})
    return sum(len(tables) for tables in selected.values())


def _get_config_from_session(conn_key: str, fields: list) -> dict:
    """Extract configuration values from session state."""
    config = {}
    for field in fields:
        key = f"{conn_key}_{field.lower().replace(' ', '_')}"
        config[field.lower().replace(' ', '_')] = st.session_state.get(key, "")
    return config


def _handle_test_connection(conn_key: str, connector_data: dict):
    """Handle the Test Connection button click — updates blinker status + shows toast."""
    st.session_state["fetch_error"] = None
    
    with st.spinner("Testing connection..."):
        try:
            from src.backend.connectors import get_connector_service, reset_connector_service
            
            service = get_connector_service()
            config = _get_config_from_session(conn_key, connector_data["fields"])
            
            # Check if adapter is registered, if not, reset and retry
            if conn_key.lower() not in service._adapters:
                reset_connector_service()
                service = get_connector_service()
            
            # FAST-FAIL: Check if Spark session is alive
            adapter = service.get_adapter(conn_key)
            if getattr(adapter, 'requires_spark_for_test', True):
                from src.backend.bootstrapper import get_bootstrapper
                bs = get_bootstrapper()
                if not bs.is_session_alive():
                    st.session_state["connection_tested"] = False
                    st.session_state[f"{conn_key}_test_status"] = "inactive"
                    st.error("Databricks Cluster is not reachable or Spark Session is inactive.")
                    if st.button("Re-initialize Spark Session", key="reset_spark_precheck_btn"):
                        with st.spinner("Re-initializing..."):
                            bs.reset_spark()
                        st.toast("Spark Session re-initialized! Try again.")
                        st.rerun()
                    return

            result = service.test_connection(conn_key, config)
            
            if result.success:
                st.session_state["connection_tested"] = True
                st.session_state[f"{conn_key}_test_status"] = "active"
                st.toast("\u2705 Connection successful!")
                st.rerun()  # Rerun to update blinker
            else:
                st.session_state["connection_tested"] = False
                st.session_state[f"{conn_key}_test_status"] = "inactive"
                st.error(f"{result.message}")
                
        except Exception as e:
            st.session_state["connection_tested"] = False
            st.session_state[f"{conn_key}_test_status"] = "inactive"
            error_msg = str(e)
            if "[NO_ACTIVE_SESSION]" in error_msg:
                st.error("Databricks Cluster is not reachable or Spark Session is inactive.")
                if st.button("Re-initialize Spark Session", key="reset_spark_btn"):
                    from src.backend.bootstrapper import get_bootstrapper
                    with st.spinner("Re-initializing..."):
                        get_bootstrapper().reset_spark()
                    st.toast("Spark Session re-initialized! Try again.")
                    st.rerun()
            else:
                st.error(f"Connection test failed: {error_msg}")


def _handle_fetch_catalogs(conn_key: str, connector_data: dict):
    """Handle the Fetch Catalogs button click (Databricks only)."""
    st.session_state["fetch_error"] = None
    
    with st.spinner("Fetching available catalogs..."):
        try:
            from src.backend.connectors import get_connector_service, reset_connector_service
            
            service = get_connector_service()
            
            # Check if adapter is registered, if not, reset and retry
            if conn_key.lower() not in service._adapters:
                reset_connector_service()
                service = get_connector_service()
            
            config = _get_config_from_session(conn_key, connector_data["fields"])
            
            catalogs = service.fetch_catalogs(conn_key, config)
            
            st.session_state["databricks_catalogs"] = catalogs
            # Auto-select first catalog
            if catalogs:
                st.session_state["databricks_selected_catalog"] = catalogs[0]
            
            st.success(f"Found {len(catalogs)} catalog(s)")
            st.rerun()  # Refresh to show catalog dropdown
            
        except Exception as e:
            st.session_state["fetch_error"] = f"Failed to fetch catalogs: {str(e)}"
            st.session_state["databricks_catalogs"] = None


def _handle_fetch_schemas(conn_key: str, connector_data: dict):
    """Handle the Fetch Schemas button click."""
    st.session_state["fetch_error"] = None
    
    with st.spinner("Fetching schemas and tables... This may take a moment."):
        try:
            from src.backend.connectors import get_connector_service, reset_connector_service
            
            service = get_connector_service()
            
            # Check if adapter is registered, if not, reset and retry
            if conn_key.lower() not in service._adapters:
                reset_connector_service()
                service = get_connector_service()
            
            config = _get_config_from_session(conn_key, connector_data["fields"])
            
            # For Databricks, add the selected catalog to config
            if conn_key == "databricks" and st.session_state.get("databricks_selected_catalog"):
                config["catalog"] = st.session_state["databricks_selected_catalog"]
            
            metadata = service.fetch_metadata(conn_key, config)
            
            st.session_state["schema_metadata"] = metadata
            st.session_state["connection_tested"] = True
            
            # Initialize selected_tables with empty selections
            if not st.session_state.get("selected_tables"):
                st.session_state["selected_tables"] = {
                    schema: [] for schema in metadata.schemas.keys()
                }
            
            # Check for existing configuration (enables incremental mode)
            existing = service.load_configuration(conn_key)
            if existing:
                st.session_state["existing_connector_config"] = {
                    "last_sync_time": existing.last_sync_time or "Never",
                    "schedule_enabled": existing.schedule_enabled,
                    "schedule_cron": existing.schedule_cron,
                }
            else:
                st.session_state["existing_connector_config"] = None
            
            st.success(
                f"Found {metadata.total_schemas} schemas with {metadata.total_tables} tables "
                f"in {metadata.fetch_time_seconds}s"
            )
            st.rerun()
            
        except ConnectionError as e:
            st.session_state["fetch_error"] = f"Connection Error: {str(e)}"
            st.session_state["schema_metadata"] = None
        except Exception as e:
            st.session_state["fetch_error"] = f"Failed to fetch schemas: {str(e)}"
            st.session_state["schema_metadata"] = None


def _render_schema_browser(schemas: dict, conn_key: str):
    """Render the schema/table selection browser with checkboxes."""
    
    # Sort schemas alphabetically
    sorted_schemas = sorted(schemas.keys())
    
    for schema_name in sorted_schemas:
        tables = schemas[schema_name]
        selected = st.session_state.get("selected_tables", {}).get(schema_name, [])
        
        # Schema expander with count badges
        selected_count = len(selected)
        total_count = len(tables)
        
        with st.expander(
            f"**{schema_name}** — {selected_count}/{total_count} tables selected",
            expanded=False,
        ):
            # Select All / Deselect All buttons
            # Select All / Deselect All buttons
            col1, col2, col3 = st.columns([1.5, 1.5, 5])
            with col1:
                if st.button(
                    "Select All",
                    key=f"select_all_{conn_key}_{schema_name}",
                    use_container_width=True,
                    type="secondary"
                ):
                    if schema_name not in st.session_state.get("selected_tables", {}):
                        st.session_state.setdefault("selected_tables", {})[schema_name] = []
                    
                    st.session_state["selected_tables"][schema_name] = tables.copy()
                    
                    # Force update UI state
                    for t in tables:
                        k = f"table_{conn_key}_{schema_name}_{t}"
                        st.session_state[k] = True
                    st.rerun()
            
            with col2:
                if st.button(
                    "Deselect All",
                    key=f"deselect_all_{conn_key}_{schema_name}",
                    use_container_width=True,
                    type="secondary"
                ):
                    if "selected_tables" in st.session_state:
                         st.session_state["selected_tables"][schema_name] = []
                    
                    # Force update UI state
                    for t in tables:
                        k = f"table_{conn_key}_{schema_name}_{t}"
                        st.session_state[k] = False
                    st.rerun()
            
            st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)
            
            # Table checkboxes in 3 columns
            cols = st.columns(3)
            for idx, table_name in enumerate(sorted(tables)):
                with cols[idx % 3]:
                    # Key must match the one updated above
                    chk_key = f"table_{conn_key}_{schema_name}_{table_name}"
                    
                    # Ensure state exists if not set
                    is_historically_selected = table_name in selected
                    if chk_key not in st.session_state:
                         st.session_state[chk_key] = is_historically_selected
                    
                    # Checkbox
                    checked = st.checkbox(
                        table_name,
                        key=chk_key
                    )
                    
                    # Sync UI -> Model
                    if schema_name not in st.session_state.get("selected_tables", {}):
                        st.session_state.setdefault("selected_tables", {})[schema_name] = []
                        
                    current_list = st.session_state["selected_tables"][schema_name]
                    
                    if checked and table_name not in current_list:
                        current_list.append(table_name)
                    elif not checked and table_name in current_list:
                        current_list.remove(table_name)


def _handle_save_configuration(conn_key: str, connector_data: dict):
    """Handle saving the connector configuration (connection details only, no table selection)."""
    # Validate connection name
    connection_name = st.session_state.get(f"{conn_key}_connection_name", "").strip()
    
    # helper to get current user
    current_user = st.session_state.get("user_name")
    if not current_user:
        current_user = "System"
        print("WARNING: User context missing during save. Defaulting to 'System'.")
    else:
        print(f"INFO: Saving connection as user '{current_user}'")
    
    if not connection_name:
        st.warning("Please enter a Connection Name before saving.")
        return
    
    with st.status("Saving connection...", expanded=True) as status:
        try:
            from src.backend.connectors import get_connector_service
            
            status.update(label="Connecting to Databricks...")
            service = get_connector_service()
            
            # Check for duplicate connection name (user-scoped)
            status.update(label="Checking connection name availability...")
            if service.check_connection_name_exists(connection_name, current_user):
                status.update(label="Duplicate name detected", state="error")
                st.warning(
                    f"You already have a connection named **'{connection_name}'**. "
                    "Please choose a different name."
                )
                return
            
            status.update(label="Processing configuration...")
            config = _get_config_from_session(conn_key, connector_data["fields"])
            
            status.update(label="Storing credentials securely...")
            
            status.update(label="Saving to Delta table...")
            connection_id = service.save_configuration(
                connector_type=conn_key,
                connector_name=connector_data["name"],
                config=config,
                connection_name=connection_name,
                created_by=current_user,
                status="active",
            )
            
            status.update(label="Connection saved successfully!", state="complete")
            
            st.success(
                f"Connection **'{connection_name}'** saved for {connector_data['name']}"
            )
            
            # Display Connection ID with custom HTML component
            import streamlit.components.v1 as components
            
            copy_html = f'''
            <div style="
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                margin: 12px 0 8px 0;
            ">
                <div style="
                    font-size: 11px;
                    font-weight: 600;
                    color: #6b7280;
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                    margin-bottom: 6px;
                ">Connection ID</div>
                <div style="
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    background: #f8fafc;
                    border: 1px solid #e2e8f0;
                    border-radius: 8px;
                    padding: 10px 14px;
                ">
                    <code id="conn-id" style="
                        font-family: 'SF Mono', Monaco, Consolas, 'Courier New', monospace;
                        font-size: 13px;
                        color: #1e293b;
                        background: transparent;
                        letter-spacing: 0.3px;
                        flex: 1;
                    ">{connection_id}</code>
                    <button id="copy-btn" onclick="
                        navigator.clipboard.writeText('{connection_id}');
                        var btn = document.getElementById('copy-btn');
                        var icon = document.getElementById('copy-icon');
                        var text = document.getElementById('copy-text');
                        icon.innerHTML = '<polyline points=&quot;20 6 9 17 4 12&quot;></polyline>';
                        icon.style.stroke = '#059669';
                        text.textContent = 'Copied!';
                        btn.style.background = '#ecfdf5';
                        btn.style.borderColor = '#6ee7b7';
                        btn.style.color = '#059669';
                        setTimeout(function() {{
                            icon.innerHTML = '<rect x=&quot;9&quot; y=&quot;9&quot; width=&quot;13&quot; height=&quot;13&quot; rx=&quot;2&quot; ry=&quot;2&quot;></rect><path d=&quot;M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1&quot;></path>';
                            icon.style.stroke = '#dc2626';
                            text.textContent = 'Copy';
                            btn.style.background = '#fef2f2';
                            btn.style.borderColor = '#fecaca';
                            btn.style.color = '#dc2626';
                        }}, 1500);
                    " style="
                        display: inline-flex;
                        align-items: center;
                        justify-content: center;
                        gap: 6px;
                        min-width: 80px;
                        background: #fef2f2;
                        border: 1px solid #fecaca;
                        border-radius: 6px;
                        padding: 7px 14px;
                        font-size: 12px;
                        font-weight: 500;
                        color: #dc2626;
                        cursor: pointer;
                        transition: all 0.15s ease;
                        white-space: nowrap;
                    " onmouseover="
                        if (document.getElementById('copy-text').textContent !== 'Copied!') {{
                            this.style.background = '#fee2e2';
                            this.style.borderColor = '#fca5a5';
                        }}
                    " onmouseout="
                        if (document.getElementById('copy-text').textContent !== 'Copied!') {{
                            this.style.background = '#fef2f2';
                            this.style.borderColor = '#fecaca';
                        }}
                    ">
                        <svg id="copy-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#dc2626" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                            <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
                            <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
                        </svg>
                        <span id="copy-text">Copy</span>
                    </button>
                </div>
            </div>
            '''
            components.html(copy_html, height=75)
            
            st.caption("💡 Select this connection in Pipeline Inspector → Ingestion to browse schemas and configure tables.")
            st.toast("Connection saved!")
            
            # Store connection_id in session for potential reference
            st.session_state["last_saved_connection_id"] = connection_id
            
            # Invalidate Inspector Cache so the dropdown refreshes
            for key in ["ingestion_config_cached", "ingestion_connector_config", "_cached_saved_connections", "_cached_saved_connections_v2"]:
                st.session_state.pop(key, None)
            
        except Exception as e:
            status.update(label="Save failed", state="error")
            st.error(f"Failed to save: {str(e)}")


def _handle_legacy_save(conn_key: str, connector_data: dict):
    """Handle save for non-schema-browser connectors (legacy behavior)."""
    try:
        with st.status("Saving...", expanded=True) as status:
            from src.backend.bootstrapper import get_bootstrapper
            from src.backend.tools.secret_manager import get_secret_manager
            import json
            
            status.update(label="Connecting to Databricks...")
            bs = get_bootstrapper()
            spark = bs.spark
            sm = get_secret_manager()
            
            status.update(label="Processing configuration...")
            config_data = {}
            for field in connector_data["fields"]:
                key = f"{conn_key}_{field.lower().replace(' ', '_')}"
                val = st.session_state.get(key, "")
                
                is_secret = any(s in field.lower() for s in ["password", "token", "key"])
                if is_secret and val:
                    status.update(label=f"Storing secure {field.lower()}...")
                    secret_key = f"{conn_key}_{field.lower().replace(' ', '_')}"
                    sm.put_secret(secret_key, val)
                    config_data[field.lower()] = sm.get_secret_metadata_pointer(secret_key)
                else:
                    config_data[field.lower()] = val
            
            # Write to Databricks
            target_catalog = st.secrets.get("DATABRICKS_CATALOG", "unity_catalog2")
            target_schema = st.secrets.get("DATABRICKS_SCHEMA", "mdm")
            target_table = f"{target_catalog}.{target_schema}.ingestion_connectors"
            
            config_json_str = json.dumps(config_data).replace("'", "''")
            connector_name = connector_data.get("name", conn_key)
            
            def escape(v):
                if v is None:
                    return "NULL"
                val = str(v).replace("'", "''")
                return f"'{val}'"
            
            sql_insert = f"""
                INSERT INTO {target_table} 
                (connector_type, connector_name, config_json, updated_at)
                VALUES (
                    {escape(conn_key)},
                    {escape(connector_name)},
                    '{config_json_str}',
                    CURRENT_TIMESTAMP()
                )
            """
            
            try:
                status.update(label="Sending to Databricks...")
                spark.sql(sql_insert).collect()
            except Exception as e:
                if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                    status.update(label="Creating table...")
                    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
                    spark.sql(f"""
                        CREATE TABLE IF NOT EXISTS {target_table} (
                            connector_type STRING,
                            connector_name STRING,
                            config_json STRING,
                            status STRING,
                            updated_at TIMESTAMP
                        ) USING DELTA
                    """)
                    spark.sql(sql_insert).collect()
                else:
                    raise e
            
            status.update(label="Saved!", state="complete", expanded=False)
            st.success(f"Connection '{connector_name}' saved!")
            st.toast("Configuration Saved!")
            
            # Invalidate Inspector Cache
            if "ingestion_config_cached" in st.session_state:
                del st.session_state["ingestion_config_cached"]
            if "ingestion_connector_config" in st.session_state:
                del st.session_state["ingestion_connector_config"]
            
    except Exception as e:
        st.error(f"Failed to save: {str(e)}")
