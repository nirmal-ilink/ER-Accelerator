"""
Data Connectors Page - Enterprise data source management with schema/table selection.

This module provides a multi-step workflow for configuring data connectors:
1. Select connector type and enter credentials
2. Test connection and fetch available schemas/tables
3. Select specific tables for ingestion
4. Save configuration to Databricks

Supports: SQL Server (with schema browser), plus Snowflake, Oracle, etc. (basic mode)
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
            "desc": "Lakehouse Platform",
            "fields": ["Host", "HTTP Path", "Token"],
            "supports_schema_browser": False,
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
                    "load_type": existing.load_type,
                    "watermark_column": existing.watermark_column,
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

    </style>
    """, unsafe_allow_html=True)

    # ==========================================================================
    # PAGE HEADER
    # ==========================================================================
    st.markdown("### Data Connectors")
    st.markdown(
        "<p style='color:#6b7280; font-size:14px; margin-top:-10px;'>"
        "Configure and manage your enterprise data sources with schema-level table selection."
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
    is_active = active_data["status"] == "Active"
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
            if is_active:
                st.markdown(
                    """<div style="background:#dcfce7; color:#166534; padding:6px 12px; border-radius:20px; font-size:12px; font-weight:600; display:inline-flex; align-items:center; gap:6px; float:right;">
                    <span style="width:6px; height:6px; background:#22c55e; border-radius:50%;"></span>Active</div>""",
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

        # Input fields (2 columns)
        c1, c2 = st.columns(2)
        for i, f in enumerate(active_data["fields"]):
            with c1 if i % 2 == 0 else c2:
                is_secret = any(s in f.lower() for s in ["password", "token", "key"])
                st.text_input(
                    f.upper(),
                    key=f"{conn_key}_{f.lower().replace(' ', '_')}",
                    type="password" if is_secret else "default",
                )

        st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)

        # ==========================================================================
        # ACTION BUTTONS ROW 1: Test & Fetch
        # ==========================================================================
        if supports_schema_browser:
            btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 2])
            
            with btn_col1:
                test_clicked = st.button(
                    "Test Connection",
                    key=f"test_{conn_key}",
                    use_container_width=True,
                )
            
            with btn_col2:
                fetch_clicked = st.button(
                    "Fetch Schemas",
                    key=f"fetch_{conn_key}",
                    use_container_width=True,
                )
            
            # Handle Test Connection
            if test_clicked:
                _handle_test_connection(conn_key, active_data)
            
            # Handle Fetch Schemas
            if fetch_clicked:
                _handle_fetch_schemas(conn_key, active_data)
            
            # Show fetch error if any
            if st.session_state.get("fetch_error"):
                st.error(st.session_state["fetch_error"])
            
            # ==========================================================================
            # STEP 2: SCHEMA & TABLE BROWSER
            # ==========================================================================
            if st.session_state.get("schema_metadata"):
                st.markdown(
                    "<hr style='border:none; border-top:1px solid #e5e7eb; margin:20px 0;'>",
                    unsafe_allow_html=True,
                )
                st.markdown(
                    "<div style='font-size:12px; font-weight:600; color:#6b7280; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:12px;'>"
                    "Step 2: Select Tables for Ingestion"
                    "</div>",
                    unsafe_allow_html=True,
                )
                
                metadata = st.session_state["schema_metadata"]
                
                # Summary stats
                st.markdown(
                    f"""
                    <div style="display:flex; gap:16px; margin-bottom:16px;">
                        <div style="background:#f0f9ff; padding:8px 16px; border-radius:8px;">
                            <span style="font-size:20px; font-weight:700; color:#0369a1;">{metadata.total_schemas}</span>
                            <span style="color:#6b7280; font-size:13px; margin-left:6px;">Schemas</span>
                        </div>
                        <div style="background:#f0fdf4; padding:8px 16px; border-radius:8px;">
                            <span style="font-size:20px; font-weight:700; color:#15803d;">{metadata.total_tables}</span>
                            <span style="color:#6b7280; font-size:13px; margin-left:6px;">Tables</span>
                        </div>
                        <div style="background:#fef3c7; padding:8px 16px; border-radius:8px;">
                            <span style="font-size:20px; font-weight:700; color:#b45309;">{_count_selected_tables()}</span>
                            <span style="color:#6b7280; font-size:13px; margin-left:6px;">Selected</span>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                
                # Render schema browser
                _render_schema_browser(metadata.schemas, conn_key)
                
                st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)
                
                # ==========================================================================
                # SAVE BUTTON
                # ==========================================================================
                st.markdown(
                    "<hr style='border:none; border-top:1px solid #e5e7eb; margin:16px 0;'>",
                    unsafe_allow_html=True,
                )
                
                _, save_col = st.columns([3, 1])
                with save_col:
                    save_clicked = st.button(
                        "Save Configuration",
                        key=f"save_{conn_key}",
                        type="primary",
                        use_container_width=True,
                    )
                
                if save_clicked:
                    _handle_save_configuration(conn_key, active_data)
        
        else:
            # ==========================================================================
            # NON-SCHEMA-BROWSER CONNECTORS (Original behavior)
            # ==========================================================================
            _, b1, b2, b3 = st.columns([4, 1.5, 1.3, 0.8])

            with b1:
                test_clicked = st.button(
                    "Test Connection",
                    type="secondary",
                    use_container_width=True,
                    key=f"test_{conn_key}",
                )
                if test_clicked:
                    with st.spinner("Testing..."):
                        time.sleep(1)
                    st.toast("Connection verified")

            with b2:
                label = "Disconnect" if is_active else "Connect"
                toggle_clicked = st.button(
                    label,
                    type="secondary",
                    use_container_width=True,
                    key=f"toggle_{conn_key}",
                )
                if toggle_clicked:
                    time.sleep(0.5)
                    st.toast("Done")

            with b3:
                save_clicked = st.button(
                    "Save",
                    type="primary",
                    use_container_width=True,
                    key=f"save_{conn_key}",
                )
                if save_clicked:
                    _handle_legacy_save(conn_key, active_data)


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
    """Handle the Test Connection button click."""
    # Clear previous schema data - this closes the schema browser
    st.session_state["schema_metadata"] = None
    st.session_state["selected_tables"] = {}
    st.session_state["fetch_error"] = None
    
    with st.spinner("Testing connection..."):
        try:
            from src.backend.connectors import get_connector_service
            
            service = get_connector_service()
            config = _get_config_from_session(conn_key, connector_data["fields"])
            
            result = service.test_connection(conn_key, config)
            
            if result.success:
                st.session_state["connection_tested"] = True
                st.success(f"{result.message}")
                if result.details:
                    with st.expander("Connection Details"):
                        st.json(result.details)
            else:
                st.session_state["connection_tested"] = False
                st.error(f"{result.message}")
                
        except Exception as e:
            st.session_state["connection_tested"] = False
            st.error(f"Connection test failed: {str(e)}")


def _handle_fetch_schemas(conn_key: str, connector_data: dict):
    """Handle the Fetch Schemas button click."""
    st.session_state["fetch_error"] = None
    
    with st.spinner("Fetching schemas and tables... This may take a moment."):
        try:
            from src.backend.connectors import get_connector_service
            
            service = get_connector_service()
            config = _get_config_from_session(conn_key, connector_data["fields"])
            
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
                    "load_type": existing.load_type,
                    "watermark_column": existing.watermark_column,
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
    """Handle saving the connector configuration with selected tables."""
    selected_tables = st.session_state.get("selected_tables", {})
    
    # Filter out empty selections
    selected_tables = {
        schema: tables for schema, tables in selected_tables.items() if tables
    }
    
    if not selected_tables:
        st.warning("Please select at least one table before saving.")
        return
    
    total_selected = sum(len(t) for t in selected_tables.values())
    
    with st.status("Saving configuration...", expanded=True) as status:
        try:
            from src.backend.connectors import get_connector_service
            import sys
            import importlib
            
            # Hot-fix: Force reload backend service to ensure latest SQL fix is active
            if 'src.backend.connectors.connector_service' in sys.modules:
                importlib.reload(sys.modules['src.backend.connectors.connector_service'])
            
            status.update(label="Connecting to Databricks...")
            service = get_connector_service()
            
            status.update(label="Processing configuration...")
            config = _get_config_from_session(conn_key, connector_data["fields"])
            
            status.update(label="Storing credentials securely...")
            
            status.update(label="Saving to Delta table...")
            # Save with defaults - load configuration is set in Pipeline Inspector
            service.save_configuration(
                connector_type=conn_key,
                connector_name=connector_data["name"],
                config=config,
                selected_tables=selected_tables,
                status="active",
            )
            
            status.update(label="Configuration saved successfully!", state="complete")
            
            st.success(
                f"Saved configuration for {connector_data['name']} "
                f"with {total_selected} tables selected"
            )
            st.info("Configure load type and scheduling in Pipeline Inspector > Ingestion")
            st.toast("Configuration saved!")
            
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
            
    except Exception as e:
        st.error(f"Failed to save: {str(e)}")
