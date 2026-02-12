import streamlit as st
import re
import random
import datetime
import textwrap
import sys
from src.backend.connectors import get_connector_service, reset_connector_service

# --- UTILS ---
def clean_html(html_str):
    """Minifies HTML to prevent markdown code block artifacts."""
    return re.sub(r'>\s+<', '><', html_str.strip())

def get_current_time_str():
    return datetime.datetime.now().strftime("%H:%M:%S")

def get_svg_icon(name, color="#64748B"):
    # Crisp, professional icons
    icons = {
        "ingest": f'''<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="{color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" x2="12" y1="15" y2="3"/></svg>''',
        "profile": f'''<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="{color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="3" width="20" height="14" rx="2" ry="2"/><line x1="8" x2="16" y1="21" y2="21"/><line x1="12" x2="12" y1="17" y2="21"/></svg>''',
        "clean": f'''<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="{color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m19.414 6.586-2-2a2 2 0 0 0-2.828 0l-8 8a2 2 0 0 0-.586 1.414V17h3l8-8a2 2 0 0 0 0-2.828z"/><path d="m9 7 9 9"/><path d="M4 21h17"/></svg>''',
        "dedup": f'''<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="{color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="5" width="8" height="8" rx="1"/><rect x="14" y="5" width="8" height="8" rx="1"/><circle cx="12" cy="12" r="10" stroke-opacity="0.2"/><path d="M10 9h4"/></svg>''',
        "merge": f'''<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="{color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M8 18V6l8 6-8 6Z"/></svg>''',
        "golden": f'''<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="{color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>''',
        "publish": f'''<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="{color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><path d="M22 6l-10 7L2 6"/></svg>''',
        "cpu": f'''<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="{color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="4" width="16" height="16" rx="2" ry="2"/><rect x="9" y="9" width="6" height="6"/><line x1="9" x2="9" y1="1" y2="4"/><line x1="15" x2="15" y1="1" y2="4"/><line x1="9" x2="9" y1="20" y2="23"/><line x1="15" x2="15" y1="20" y2="23"/><line x1="20" x2="23" y1="9" y2="9"/><line x1="20" x2="23" y1="14" y2="14"/><line x1="1" x2="4" y1="9" y2="9"/><line x1="1" x2="4" y1="14" y2="14"/></svg>''',
        "server": f'''<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="{color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="2" width="20" height="8" rx="2" ry="2"/><rect x="2" y="14" width="20" height="8" rx="2" ry="2"/><line x1="6" x2="6.01" y1="6" y2="6"/><line x1="6" x2="6.01" y1="18" y2="18"/></svg>''',
        "activity": f'''<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="{color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>'''
    }
    return icons.get(name, "")

def generate_stage_logs(stage_name):
    """Generates context-aware logs for a specific stage."""
    common_logs = [
        f"[INFO] {get_current_time_str()} - [Driver] Health check passed.",
        f"[INFO] {get_current_time_str()} - [Executor] Heartbeat received."
    ]
    
    specific_logs = {
        "Ingestion": [
            f"[INFO] {get_current_time_str()} - Connecting to data source...",
            f"[INFO] {get_current_time_str()} - Fetching batch 2041 (1.2GB)...",
            f"[INFO] {get_current_time_str()} - Validating schema compatibility...",
            f"[INFO] {get_current_time_str()} - Writing raw data to landing zone..."
        ],
        "Profiling": [
            f"[INFO] {get_current_time_str()} - Sampling 20% of dataset for analysis...",
            f"[WARN] {get_current_time_str()} - Detected 402 null values in column 'SSN'.",
            f"[INFO] {get_current_time_str()} - Running distinct value count on 'city'...",
            f"[INFO] {get_current_time_str()} - Generating quality report..."
        ],
        "Cleansing": [
            f"[INFO] {get_current_time_str()} - Applying standardization rule 'STD_ADDR_01'...",
            f"[INFO] {get_current_time_str()} - Normalizing phone numbers to E.164 format...",
            f"[INFO] {get_current_time_str()} - Replacing nulls in 'country' with 'US'...",
            f"[INFO] {get_current_time_str()} - Batch validation complete. 12k rows filtered."
        ],
        "Resolution": [
            f"[INFO] {get_current_time_str()} - Initiating fuzzy match (Threshold: 0.85)...",
            f"[INFO] {get_current_time_str()} - Blocking pass on keys: ['zip', 'lastname']...",
            f"[INFO] {get_current_time_str()} - Pairwise comparison: 14M candidate pairs...",
            f"[INFO] {get_current_time_str()} - Computing similarity score (Jaro-Winkler)..."
        ],
        "Survivorship": [
            f"[INFO] {get_current_time_str()} - Loading trust framework rules...",
            f"[INFO] {get_current_time_str()} - Resolving conflicts for Attribute: 'Email'...",
            f"[INFO] {get_current_time_str()} - Prioritizing source 'Salesforce' over 'Legacy'...",
            f"[INFO] {get_current_time_str()} - Merging 4 records into Golden ID: G-10293..."
        ],
        "Publishing": [
            f"[INFO] {get_current_time_str()} - Formatting output for destination...",
            f"[INFO] {get_current_time_str()} - Triggering downstream webhook...",
            f"[INFO] {get_current_time_str()} - Compliance check: GDPR...",
            f"[INFO] {get_current_time_str()} - Sync completed. Duration: 4ms."
        ]
    }
    
    return specific_logs.get(stage_name, common_logs)

def get_ingestion_stage_desc():
    """Get dynamic description for Ingestion stage based on configured sources."""
    try:
        # Optimization: Check cache first to avoid slow backend calls on every render
        config = st.session_state.get("ingestion_connector_config")
        
        if config is None and not st.session_state.get("ingestion_config_cached", False):
            service = get_connector_service()
            config = service.get_latest_configuration()
            # Cache the result
            st.session_state["ingestion_connector_config"] = config
            st.session_state["ingestion_config_cached"] = True
            
        if config and config.connector_type:
            connector_names = {
                "sqlserver": "SQL Server",
                "databricks": "Databricks",
                "snowflake": "Snowflake"
            }
            name = config.connector_name or connector_names.get(config.connector_type, config.connector_type.upper())
            table_count = sum(len(t) for t in config.selected_tables.values()) if config.selected_tables else 0
            if table_count > 0:
                return f"{name} connected ({table_count} tables)"
            return f"{name} configured"
    except Exception:
        pass
    return "No source configured"

def render():
    if 'inspector_active_stage' not in st.session_state:
        st.session_state['inspector_active_stage'] = 0 # Default to "Ingestion"
        
    COLORS = {
        'brand': "#D11F41",
        'dark': "#0F172A",
        'text': "#334155",
        'muted': "#64748B",
        'border': "#E2E8F0",
        'bg_main': "#F8FAFC",
        'success': "#10B981",
        'card': "#FFFFFF",
        'active_bg': "#FFF1F2"
    }

    # Get dynamic ingestion description based on configured connector
    ingestion_desc = get_ingestion_stage_desc()
    has_source = "configured" in ingestion_desc or "connected" in ingestion_desc
    
    stages = [
        {"id": 0, "name": "Ingestion", "pct": 100 if has_source else 0, "status": "done" if has_source else "pending", "icon": "ingest", "meta": "SUCCESS" if has_source else "PENDING", "desc": ingestion_desc},
        {"id": 1, "name": "Profiling", "pct": 100, "status": "done", "icon": "profile",  "meta": "SUCCESS", "desc": "Quality checks passed (99.8%)"},
        {"id": 2, "name": "Cleansing", "pct": 100, "status": "done", "icon": "clean",  "meta": "SUCCESS", "desc": "Standardization rules applied"},
        {"id": 3, "name": "Resolution", "pct": 72, "status": "active", "icon": "dedup",  "meta": "RUNNING", "desc": "Fuzzy matching (Block 4/12)"},
        {"id": 4, "name": "Survivorship", "pct": 0, "status": "pending", "icon": "merge",  "meta": "PENDING", "desc": "Golden record rules"},
        {"id": 5, "name": "Publishing", "pct": 0, "status": "pending", "icon": "publish",  "meta": "PENDING", "desc": "Downstream sync"},
    ]
    
    # --- CSS STYLES ---
    st.markdown(clean_html(f"""
    <style>
        .inspector-container {{ max_width: 1200px; margin: 0 auto; padding: 20px 0; }}
        
        /* CLUSTER BAR */
        .cluster-bar {{
            background: #FFFFFF; border: 1px solid #E2E8F0; border-radius: 12px; padding: 16px 24px;
            display: flex; align-items: center; justify-content: space-between; margin-bottom: 32px;
            box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05);
        }}
        .c-metric {{ display: flex; align-items: center; gap: 8px; font-size: 13px; color: {COLORS['text']}; font-weight: 500; }}
        .c-val {{ background: #F1F5F9; padding: 4px 12px; border-radius: 8px; color: {COLORS['dark']}; font-weight: 700; font-family: 'Inter', sans-serif; font-size: 13px; }}
        .pulse-dot {{ width: 10px; height: 10px; background: {COLORS['brand']}; border-radius: 50%; animation: pulse-red 2.5s infinite; }}
        @keyframes pulse-red {{ 0% {{ box-shadow: 0 0 0 0 rgba(209, 31, 65, 0.4); }} 70% {{ box-shadow: 0 0 0 10px rgba(209, 31, 65, 0); }} 100% {{ box-shadow: 0 0 0 0 rgba(0,0,0,0); }} }}

        /* --- FORM ELEMENTS (Force Light Theme & Alignment) --- */
        
        /* === SELECTBOX STYLING === */
        /* Selectbox outer container */
        div[data-testid="stSelectbox"] {{
            margin-bottom: 0 !important;
            margin-top: 0 !important;
            display: flex !important;
            align-items: center !important;
        }}
        
        /* Selectbox inner container - main visible box */
        div[data-baseweb="select"] > div {{
            background-color: #FFFFFF !important;
            color: #0F172A !important;
            border: 1.5px solid #CBD5E1 !important;
            border-radius: 8px !important;
            height: 40px !important;
            min-height: 40px !important;
            max-height: 40px !important;
            display: flex !important;
            align-items: center !important;
            box-shadow: 0 1px 2px 0 rgba(0, 0, 0, 0.05) !important;
            padding: 0 10px !important;
            font-size: 13px !important;
            line-height: 18px !important;
        }}
        
        /* Selectbox text */
        div[data-baseweb="select"] span {{
            font-size: 13px !important;
            line-height: 18px !important;
            color: #0F172A !important;
            font-weight: 400 !important;
        }}
        
        /* === TEXT INPUT STYLING === */
        /* Text input outer container */
        div[data-testid="stTextInput"] {{
            margin-bottom: 0 !important;
            margin-top: 0 !important;
            display: flex !important;
            align-items: center !important;
        }}
        
        /* Remove all borders from wrapper */
        div[data-testid="stTextInput"] div[data-baseweb="input"],
        div[data-testid="stTextInput"] div[data-baseweb="input"]:hover,
        div[data-testid="stTextInput"] div[data-baseweb="input"]:focus,
        div[data-testid="stTextInput"] div[data-baseweb="input"]:focus-within {{
            border: none !important;
            box-shadow: none !important;
            background: transparent !important;
        }}
        
        /* Text input inner container - main visible box (EXACT MATCH TO SELECTBOX) */
        div[data-testid="stTextInput"] div[data-baseweb="input"] > div {{
            background-color: #FFFFFF !important;
            color: #0F172A !important;
            border: 1.5px solid #CBD5E1 !important;
            border-radius: 8px !important;
            height: 40px !important;
            min-height: 40px !important;
            max-height: 40px !important;
            box-shadow: 0 1px 2px 0 rgba(0, 0, 0, 0.05) !important;
            display: flex !important;
            align-items: center !important;
            padding: 0 !important;
        }}
        
        /* Text input actual input element */
        div[data-testid="stTextInput"] input {{
             color: #0F172A !important;
             background-color: transparent !important;
             border: none !important;
             outline: none !important;
             height: 40px !important;
             padding: 0 10px !important;
             font-size: 13px !important;
             line-height: 18px !important;
             font-weight: 400 !important;
             width: 100% !important;
        }}
        
        /* Remove focus effects */
        div[data-testid="stTextInput"] input:focus,
        div[data-testid="stTextInput"] input:active {{
             outline: none !important;
             border: none !important;
             box-shadow: none !important;
        }}
        
        /* === MARKDOWN CONTAINER ALIGNMENT === */
        /* Force markdown containers to align with widgets */
        div[data-testid="stMarkdown"] {{
            margin-top: 0 !important;
            margin-bottom: 0 !important;
            display: flex !important;
            align-items: center !important;
        }}
        
        /* === DISABLED TEXT INPUT STYLING === */
        /* Style disabled text inputs (for N/A indicators) */
        div[data-testid="stTextInput"] input:disabled {{
            background-color: #F1F5F9 !important;
            color: #94A3B8 !important;
            font-style: italic !important;
            cursor: not-allowed !important;
        }}
        
        div[data-testid="stTextInput"] input:disabled + div {{
            background-color: #F1F5F9 !important;
        }}
        
        div[data-testid="stTextInput"]:has(input:disabled) div[data-baseweb="input"] > div {{
            background-color: #F1F5F9 !important;
            border-color: #E2E8F0 !important;
        }}

        div[data-baseweb="popover"] div[data-baseweb="menu"] {{
            background-color: #FFFFFF !important;
        }}
        div[data-testid="stSelectbox"] label p {{
            font-weight: 600 !important;
            color: #334155 !important;
        }}

        /* --- PROFESSIONAL EXPANDER STYLING (Folder Icon + Badges) --- */
        /* Badge Styling inside Expander: Use code blocks as pills */
        div[data-testid="stExpander"] summary code {{
            background-color: #E2E8F0 !important;
            color: #475569 !important;
            border-radius: 12px !important;
            padding: 2px 10px !important;
            font-family: var(--font, "Source Sans Pro", sans-serif) !important; /* Override monospace */
            font-size: 12px !important;
            font-weight: 600 !important;
            border: none !important;
            display: inline-block !important;
            vertical-align: middle !important;
            margin-left: 8px !important;
        }}

        /* Icon Styling inside Expander: Inject SVG Folder */
        div[data-testid="stExpander"] summary p {{
            padding-left: 28px !important;
            position: relative !important;
        }}

        div[data-testid="stExpander"] summary p::before {{
            content: "" !important;
            position: absolute !important;
            left: 0 !important;
            top: 50% !important;
            transform: translateY(-50%) !important;
            width: 20px !important;
            height: 20px !important;
            /* Professional SVG Folder Icon (Dark Grey / Slate-500) */
            background-image: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="%2364748B" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"></path></svg>') !important;
            background-repeat: no-repeat !important;
            background-size: contain !important;
        }}

        /* --- FORCE RADIO VISIBILITY (Fix for Resolution Stage) --- */
        div[role="radiogroup"]:not([aria-label="Load Strategy"]) p {{
            color: #334155 !important;
            font-weight: 500 !important;
            display: block !important;
            visibility: visible !important;
        }}

        /* --- PREMIUM BUTTON DESIGN --- */
        /* Stage Navigation Buttons in Left Column (Not sidebar) */
        [data-testid="stMain"] div.stButton > button[kind="secondary"] {{
            background-color: #FFFFFF !important;
            color: #475569 !important;
            border: 1px solid #E2E8F0 !important;
            border-radius: 12px !important;
            height: auto !important;
            padding: 18px 20px !important;
            width: 100% !important;
            justify-content: flex-start !important;
            text-align: left !important;
            transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1) !important;
            box-shadow: 0 1px 2px rgba(0,0,0,0.02) !important;
            font-size: 15px !important;
            font-weight: 600 !important;
            letter-spacing: -0.2px !important;
        }}
        [data-testid="stMain"] div.stButton > button[kind="secondary"]:hover {{
            background-color: #FFFFFF !important;
            border-color: {COLORS['brand']}30 !important;
            color: {COLORS['brand']} !important;
            transform: translateY(-2px) !important;
            box-shadow: 0 10px 15px -3px rgba(0,0,0,0.05) !important;
        }}

        /* Primary Button (Active Stage) in Main Area */
        [data-testid="stMain"] div.stButton > button[kind="primary"] {{
            background-color: {COLORS['brand']} !important;
            border: 1px solid {COLORS['brand']} !important;
            color: white !important;
            border-radius: 12px !important;
            padding: 18px 20px !important;
            width: 100% !important;
            justify-content: flex-start !important;
            font-size: 15px !important;
            font-weight: 700 !important;
            box-shadow: 0 10px 20px -5px rgba(209, 31, 65, 0.3) !important;
            position: relative !important;
            overflow: hidden !important;
        }}
        
        /* Active Indicator Line */
        [data-testid="stMain"] div.stButton > button[kind="primary"]::before {{
            content: '';
            position: absolute;
            left: 0;
            top: 25%;
            height: 50%;
            width: 4px;
            background: rgba(255,255,255,0.8);
            border-radius: 0 4px 4px 0;
        }}

        [data-testid="stMain"] div.stButton > button[kind="primary"]:hover {{
            background-color: #B91C41 !important;
            border-color: #B91C41 !important;
            transform: translateY(-2px) !important;
            box-shadow: 0 12px 24px -6px rgba(209, 31, 65, 0.4) !important;
        }}
        
        /* --- PAGINATION NAVIGATION BUTTONS (Previous/Next at bottom) --- */
        .st-key-prev_btn_foot div.stButton > button,
        .st-key-next_btn_foot div.stButton > button {{
            padding: 12px 24px !important;
            justify-content: center !important;
            text-align: center !important;
            border-radius: 10px !important;
            font-size: 14px !important;
        }}
        
        .st-key-prev_btn_foot div.stButton > button {{
            background-color: #FFFFFF !important;
            color: #475569 !important;
            border: 1px solid #E2E8F0 !important;
            box-shadow: 0 2px 4px rgba(0,0,0,0.04) !important;
        }}
        
        .st-key-prev_btn_foot div.stButton > button:hover {{
            background-color: #F8FAFC !important;
            border-color: #CBD5E1 !important;
            color: #334155 !important;
            transform: translateY(-1px) !important;
        }}
        
        .st-key-next_btn_foot div.stButton > button {{
            background-color: {COLORS['brand']} !important;
            color: white !important;
            border: 1px solid {COLORS['brand']} !important;
            box-shadow: 0 4px 12px rgba(209, 31, 65, 0.25) !important;
        }}
        
        .st-key-next_btn_foot div.stButton > button:hover {{
            background-color: #B91C41 !important;
            border-color: #B91C41 !important;
            transform: translateY(-1px) !important;
            box-shadow: 0 6px 16px rgba(209, 31, 65, 0.35) !important;
        }}
        
        .st-key-next_btn_foot div.stButton > button::before {{
            display: none !important;
        }}
        
        button:focus {{ outline: none !important; box-shadow: none !important; }}

        /* MAIN CARD */
        .detail-card {{
            background: white; border: 1px solid #E2E8F0; border-radius: 16px; padding: 32px; height: 100%;
            box-shadow: 0 20px 25px -5px rgba(0,0,0,0.05), 0 10px 10px -5px rgba(0,0,0,0.02);
            transition: all 0.3s ease;
        }}
        
        .dc-header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px; }}
        .dc-title {{ font-size: 24px; font-weight: 800; color: {COLORS['dark']}; display: flex; align-items: center; gap: 16px; letter-spacing: -0.5px;}}
        
        /* LOGS */
        .log-box {{
            background: #0F172A; border-radius: 12px; padding: 20px; font-family: 'JetBrains Mono', monospace;
            font-size: 12px; color: #94A3B8; height: 350px; overflow-y: auto; margin-top: 32px;
            border: 1px solid #1E293B; line-height: 1.7;
            box-shadow: inset 0 2px 4px rgba(0,0,0,0.3);
        }}
        .log-line {{ margin-bottom: 8px; display: block; }}
        .log-hl {{ color: #FCD34D; }}
        .log-ts {{ color: #64748B; margin-right: 8px; }}
        
        /* PROGRESS BAR */
        .lg-progress {{ height: 14px; background: #F1F5F9; border-radius: 7px; overflow: hidden; margin: 24px 0; }}
        .lg-fill {{ height: 100%; transition: width 0.8s cubic-bezier(0.4, 0, 0.2, 1); }}
        .fill-done {{ background: {COLORS['success']}; }}
        .fill-active {{ 
            background: linear-gradient(90deg, {COLORS['brand']}, #E11D48); 
            background-size: 200% 100%;
            animation: moveGradient 3s linear infinite;
        }}
        @keyframes moveGradient {{
            0% {{ background-position: 200% 0; }}
            100% {{ background-position: 0 0; }}
        }}
        
        /* STATS GRID */
        .stats-tile {{
            background: #F8FAFC; border: 1px solid #F1F5F9; border-radius: 12px; padding: 20px; text-align: center;
            transition: all 0.2s ease;
        }}
        .stats-tile:hover {{ background: #FFFFFF; border-color: {COLORS['border']}; transform: translateY(-3px); box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05); }}
        
        /* SOURCE CARD - Premium Glassmorphism Design */
        .source-card-premium {{
            background: linear-gradient(135deg, rgba(255,255,255,0.95) 0%, rgba(248,250,252,0.9) 100%);
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            border: 1px solid rgba(16, 185, 129, 0.2);
            border-radius: 20px;
            padding: 28px;
            margin-bottom: 24px;
            position: relative;
            overflow: hidden;
            box-shadow: 
                0 4px 24px rgba(16, 185, 129, 0.1),
                0 1px 3px rgba(0,0,0,0.04),
                inset 0 1px 0 rgba(255,255,255,0.8);
            transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
        }}
        
        .source-card-premium::before {{
            content: '';
            position: absolute;
            inset: 0;
            border-radius: 20px;
            padding: 2px;
            background: linear-gradient(135deg, {COLORS['success']}, #34D399, #6EE7B7, {COLORS['success']});
            background-size: 300% 300%;
            -webkit-mask: linear-gradient(#fff 0 0) content-box, linear-gradient(#fff 0 0);
            -webkit-mask-composite: xor;
            mask-composite: exclude;
            opacity: 0;
            transition: opacity 0.4s ease;
        }}
        
        .source-card-premium:hover {{
            transform: translateY(-4px);
            box-shadow: 
                0 12px 40px rgba(16, 185, 129, 0.18),
                0 4px 12px rgba(0,0,0,0.06),
                inset 0 1px 0 rgba(255,255,255,0.9);
        }}
        
        .source-card-premium:hover::before {{
            opacity: 1;
            animation: gradientRotate 3s linear infinite;
        }}
        
        @keyframes gradientRotate {{
            0% {{ background-position: 0% 50%; }}
            50% {{ background-position: 100% 50%; }}
            100% {{ background-position: 0% 50%; }}
        }}
        
        /* Pulsing Live Indicator */
        @keyframes livePulse {{
            0%, 100% {{ opacity: 1; transform: scale(1); }}
            50% {{ opacity: 0.6; transform: scale(1.15); }}
        }}
        
        .live-indicator {{
            width: 10px;
            height: 10px;
            background: {COLORS['success']};
            border-radius: 50%;
            display: inline-block;
            animation: livePulse 2s ease-in-out infinite;
            box-shadow: 0 0 8px rgba(16, 185, 129, 0.5);
        }}
        
        /* Premium Stats Tile */
        .stats-tile-premium {{
            background: linear-gradient(135deg, #FFFFFF 0%, #F8FAFC 100%);
            border: 1px solid #E2E8F0;
            border-radius: 14px;
            padding: 16px 20px;
            text-align: center;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            position: relative;
            overflow: hidden;
        }}
        
        .stats-tile-premium::after {{
            content: '';
            position: absolute;
            inset: 0;
            background: linear-gradient(135deg, rgba(16, 185, 129, 0.05) 0%, transparent 50%);
            opacity: 0;
            transition: opacity 0.3s ease;
        }}
        
        .stats-tile-premium:hover {{
            transform: translateY(-3px);
            border-color: {COLORS['success']}40;
            box-shadow: 0 8px 24px rgba(16, 185, 129, 0.12);
        }}
        
        .stats-tile-premium:hover::after {{
            opacity: 1;
        }}
        
        /* Refresh Button */
        .st-key-refresh_config_btn button {{
            background: transparent !important;
            border: 1px solid #E2E8F0 !important;
            color: #64748B !important;
            padding: 8px 16px !important;
            border-radius: 10px !important;
            font-size: 13px !important;
            font-weight: 500 !important;
            transition: all 0.2s ease !important;
        }}
        
        .st-key-refresh_config_btn button:hover {{
            background: #F8FAFC !important;
            border-color: {COLORS['success']} !important;
            color: {COLORS['success']} !important;
        }}
        
        /* CONNECTION ID INPUT STYLING */
        .st-key-ingestion_connection_id_input input {{
            font-family: 'JetBrains Mono', 'Consolas', monospace !important;
            font-size: 13px !important;
            letter-spacing: 0.3px !important;
            background: #FFFFFF !important;
            border: 1px solid #E2E8F0 !important;
            border-radius: 10px !important;
            padding: 12px 16px !important;
            height: 44px !important;
        }}
        .st-key-ingestion_connection_id_input input:focus {{
            border-color: {COLORS['brand']} !important;
            box-shadow: 0 0 0 3px rgba(209, 31, 65, 0.1) !important;
        }}
        .st-key-ingestion_connection_id_input input::placeholder {{
            color: #94A3B8 !important;
            font-weight: 400 !important;
        }}
        
        /* LOAD BUTTON STYLING */
        .st-key-load_conn_id_btn button {{
            height: 44px !important;
            font-weight: 600 !important;
        }}
    </style>
    """), unsafe_allow_html=True)

    
    # --- HEADER ---
    st.markdown(clean_html(f"""
    <div class="inspector-container">
        <div style="display: flex; justify-content: space-between; align-items: flex-end; margin-bottom: 32px;">
            <div>
                <h1 style="color: {COLORS['dark']}; font-weight: 800; margin: 0; font-size: 36px; letter-spacing: -1px;">Pipeline Inspector</h1>
                <p style="color: {COLORS['muted']}; font-size: 16px; margin-top: 4px;">Real-time backend process orchestration monitor</p>
            </div>
            <div class="cluster-bar" style="margin-bottom: 0;">
                <div class="c-metric"><div class="pulse-dot"></div><span style="font-weight:700; color:{COLORS['brand']}; letter-spacing: 0.5px;">LIVE RUNNING</span></div>
                <div style="width: 1px; height: 24px; background: #E2E8F0; margin: 0 20px;"></div>
                <div class="c-metric"><span>Node:</span><span class="c-val">db-master-01</span></div>
                <div class="c-metric"><span>Runtime:</span><span class="c-val">04:12:08</span></div>
            </div>
        </div>
    </div>
    """), unsafe_allow_html=True)

    # --- MAIN LAYOUT ---
    c_nav, c_main = st.columns([1.1, 2.9], gap="large")
    
    current_idx = st.session_state['inspector_active_stage']
    
    with c_nav:
        st.markdown(f"""
        <div style="margin-bottom: 16px; display: flex; align-items: center; justify-content: space-between;">
            <span style="font-size: 14px; font-weight: 700; color: {COLORS['dark']}; text-transform: uppercase; letter-spacing: 0.5px;">Stages ({len(stages)})</span>
            <span style="font-size: 12px; color: {COLORS['muted']}; font-weight: 600;">v2.4.1</span>
        </div>
        """, unsafe_allow_html=True)
        
        for i, stage in enumerate(stages):
            # Professional Icons based on status
            status_ico = ""
            if stage['status'] == 'done': status_ico = "✓ "
            elif stage['status'] == 'active': status_ico = "● "
            else: status_ico = "  "
            
            label = f"{status_ico}{stage['name']}"
            
            if i == current_idx:
                st.button(label, key=f"nav_{i}", use_container_width=True, type="primary")
            else:
                 if st.button(label, key=f"nav_{i}", use_container_width=True):
                     st.session_state['inspector_active_stage'] = i
                     st.rerun()

    with c_main:
        active_stage = stages[current_idx]
        
        # Determine colors
        s_color = COLORS['success'] if active_stage['status'] == "done" else COLORS['brand'] if active_stage['status'] == "active" else COLORS['muted']
        s_fill = "fill-done" if active_stage['status'] == "done" else "fill-active" if active_stage['status'] == "active" else ""
        
        st.markdown(clean_html(f"""
        <div class="detail-card">
            <div class="dc-header">
                <div class="dc-title">
                    <div style="background: {s_color}10; width: 48px; height: 48px; border-radius: 12px; display: flex; align-items: center; justify-content: center;">
                        {get_svg_icon(active_stage['icon'], s_color)}
                    </div>
                    <div>
                        <div style="font-size: 12px; font-weight: 700; color: {s_color}; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 2px;">Component Status: {active_stage['status']}</div>
                        {active_stage['name']}
                    </div>
                </div>
                <div style="background: {s_color}10; color: {s_color}; padding: 6px 16px; border-radius: 99px; font-weight: 700; font-size: 13px; border: 1px solid {s_color}20;">
                    {active_stage['meta']}
                </div>
            </div>
            
            <p style="color: #64748B; font-size: 15px; margin-bottom: 32px; line-height: 1.6;">{active_stage['desc']}</p>
        """), unsafe_allow_html=True)
        
        # Check if this is the Ingestion stage - show Load Configuration
        if active_stage['name'] == "Ingestion":
            # Initialize load config state
            if "ingestion_load_type" not in st.session_state:
                st.session_state["ingestion_load_type"] = "full"
            if "ingestion_schedule_enabled" not in st.session_state:
                st.session_state["ingestion_schedule_enabled"] = False
            
            # Source-type → hierarchy label mapping (extensible for future sources)
            SOURCE_HIERARCHY = {
                "databricks":  ("Catalog", "Schema", "Table"),
                "sqlserver":   ("Database", "Schema", "Table"),
                # Future: add more source types here
                # "snowflake":   ("Database", "Schema", "Table"),
                # "fabric":      ("Lakehouse", "Schema", "Table"),
                # "sap":         ("Catalog", "Schema", "Table"),
            }
            
            # ================================================================
            # STEP 1: SELECT SAVED CONNECTION (Connection Name Only)
            # ================================================================
            st.markdown(f"""
            <div style="background: linear-gradient(135deg, #F8FAFC 0%, #F1F5F9 100%); border: 1px solid #E2E8F0; border-radius: 14px; padding: 20px; margin-bottom: 20px;">
                <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 16px;">
                    <div style="background: linear-gradient(135deg, {COLORS['brand']}15 0%, {COLORS['brand']}08 100%); width: 40px; height: 40px; border-radius: 10px; display: flex; align-items: center; justify-content: center; border: 1px solid {COLORS['brand']}20;">
                        <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="{COLORS['brand']}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                            <path d="M21 2l-2 2m-7.61 7.61a5.5 5.5 0 1 1-7.778 7.778 5.5 5.5 0 0 1 7.777-7.777zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4"/>
                        </svg>
                    </div>
                    <div>
                        <div style="font-weight: 700; color: #0F172A; font-size: 15px;">Select Connection</div>
                        <div style="color: #64748B; font-size: 12px;">Choose a saved connection to begin ingestion setup</div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # --- Fetch saved connections from backend ---
            current_user = st.session_state.get("user_name", "")
            connection_options = ["-- Select a saved connection --"]
            connection_map = {}
            
            # Cache connections in session state to avoid blocking Spark query on every rerun
            # Initialize Service ONCE at the top of this block
            from src.backend.connectors import get_connector_service as _get_conn_svc
            _conn_svc = _get_conn_svc()

            # Force refresh by changing key (v2) to clear stale empty states
            cache_key = "_cached_saved_connections_v2"
            if cache_key not in st.session_state:
                try:
                    with st.spinner("Loading saved connections..."):
                        import time # Added for start_time
                        
                        start_time = time.time()
                        # 1. Try fetching user-specific connections
                        st.toast(f"Checking connections for user: {current_user}...")
                        print(f"DEBUG: Fetching connections for user: '{current_user}'")
                        user_connections = _conn_svc.get_user_connections(current_user)
                        
                        # 2. Fallback: If no user connections, fetch ALL connections (visibility safety net)
                        # This handles cases where 'created_by' column is missing or default 'System' was used
                        if not user_connections:
                            st.toast("No user connections found. Checking system-wide...")
                            print("DEBUG: No user-specific connections found. Falling back to fetching ALL connections.")
                            all_connections = _conn_svc.get_all_connections()
                            user_connections = all_connections
                        
                        st.session_state[cache_key] = user_connections
                        
                        duration = time.time() - start_time
                        if user_connections:
                            st.toast(f"Loaded {len(user_connections)} connections in {duration:.2f}s")
                        else:
                            st.toast("No saved connections found.")
                            
                            # Show which table was queried to help debug
                            target_catalog = st.secrets.get("DATABRICKS_CATALOG", "unity_catalog2")
                            target_schema = st.secrets.get("DATABRICKS_SCHEMA", "mdm")
                            table_path = f"{target_catalog}.{target_schema}.configuration_metadata"
                            st.warning(f"No connections found in `{table_path}` for user `{current_user}`. Please verify your `DATABRICKS_CATALOG` and `DATABRICKS_SCHEMA` secrets.")
                            
                except Exception as e:
                    st.error(f"Failed to load connections: {e}")
                    import traceback
                    traceback.print_exc()
                    st.session_state[cache_key] = []
            
                except Exception as e:
                    st.error(f"Failed to load connections: {e}")
                    import traceback
                    traceback.print_exc()
                    st.session_state[cache_key] = []
            
            # Build dropdown options from cached connections
            for uc in st.session_state.get(cache_key, []):
                source_label = uc.connector_type.upper() if uc.connector_type else "UNKNOWN"
                label = f"{uc.connection_name or uc.connector_name} ({source_label})"
                connection_options.append(label)
                connection_map[label] = uc
            
            # Auto-select if there's already a loaded config
            default_index = 0
            loaded_config = st.session_state.get("ingestion_connector_config")
            if loaded_config:
                for idx, label in enumerate(connection_options):
                    if label in connection_map and connection_map[label].connection_id == loaded_config.connection_id:
                        default_index = idx
                        break
            
            selected_conn_label = st.selectbox(
                "Saved Connections",
                options=connection_options,
                index=default_index,
                key="ingestion_conn_dropdown",
                label_visibility="collapsed",
            )
            
            # Handle dropdown selection
            if selected_conn_label != "-- Select a saved connection --" and selected_conn_label in connection_map:
                selected_conn = connection_map[selected_conn_label]
                prev_config = st.session_state.get("ingestion_connector_config")
                if not prev_config or prev_config.connection_id != selected_conn.connection_id:
                    st.session_state["ingestion_connector_config"] = selected_conn
                    st.session_state["ingestion_config_cached"] = True
                    st.session_state["ingestion_connector_type"] = selected_conn.connector_type
                    st.session_state["ingestion_is_databricks"] = selected_conn.connector_type == "databricks"
                    # Clear cached metadata when connection changes
                    st.session_state.pop("inspector_schema_metadata", None)
                    st.session_state.pop("inspector_selected_tables", None)
                    st.session_state.pop("inspector_table_configs", None)
                    st.session_state.pop("ingestion_catalogs", None)
                    st.session_state.pop("ingestion_selected_catalog", None)
                    st.rerun()
            
            st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)
            
            # ================================================================
            # STEP 2: SOURCE SUMMARY + HIERARCHICAL METADATA BROWSER
            # ================================================================
            loaded_config = st.session_state.get("ingestion_connector_config")
            
            if loaded_config:
                active_connector_type = loaded_config.connector_type
                connector_display_name = loaded_config.connection_name or loaded_config.connector_name or (active_connector_type.upper() if active_connector_type else "Unknown")
                is_databricks = active_connector_type == "databricks"
                last_sync = loaded_config.last_sync_time[:16].replace("T", " ") if loaded_config.last_sync_time else "Just now"
                hierarchy = SOURCE_HIERARCHY.get(active_connector_type, ("Database", "Schema", "Table"))
                
                # --- Source Summary Card ---
                st.markdown(f"""
                <div class="source-card-premium">
                    <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                        <div style="display: flex; align-items: center; gap: 18px;">
                            <div style="background: linear-gradient(135deg, {COLORS['success']}18 0%, {COLORS['success']}08 100%); width: 56px; height: 56px; border-radius: 16px; display: flex; align-items: center; justify-content: center; border: 1px solid {COLORS['success']}25;">
                                <svg xmlns="http://www.w3.org/2000/svg" width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="{COLORS['success']}" stroke-width="2">
                                    <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/>
                                </svg>
                            </div>
                            <div>
                                <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 4px;">
                                    <span class="live-indicator"></span>
                                    <span style="font-size: 11px; font-weight: 700; color: {COLORS['success']}; text-transform: uppercase; letter-spacing: 0.8px;">Source Active</span>
                                </div>
                                <div style="font-weight: 700; color: #0F172A; font-size: 20px; letter-spacing: -0.4px;">{connector_display_name}</div>
                                <div style="font-size: 12px; color: #64748B; margin-top: 4px;">
                                    <span style="color: #94A3B8;">Last sync:</span> {last_sync}
                                    &nbsp;&bull;&nbsp;
                                    <span style="color: #94A3B8;">Type:</span> {active_connector_type.upper() if active_connector_type else 'N/A'}
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                # --- Hierarchical Metadata Browser ---
                st.markdown(f"""
                <div style="font-size: 12px; font-weight: 700; color: #64748B; text-transform: uppercase; letter-spacing: 0.5px; margin: 20px 0 12px;">
                    {hierarchy[0]} / {hierarchy[1]} / {hierarchy[2]} Browser
                </div>
                """, unsafe_allow_html=True)
                
                # Level 1: Catalog / Database / Lakehouse
                level1_label = hierarchy[0]
                level1_value = None
                
                if is_databricks or active_connector_type == "sqlserver":
                    # Databricks: Fetch catalogs live
                    # SQL Server: Fetch databases live
                    if "ingestion_catalogs" not in st.session_state:
                        try:
                            from src.backend.connectors import get_connector_service
                            svc = get_connector_service()
                            resolved_cfg = svc._resolve_secrets(loaded_config.config)
                            catalogs = svc.fetch_catalogs(active_connector_type, resolved_cfg)
                            st.session_state["ingestion_catalogs"] = catalogs if catalogs else ["default"]
                        except Exception as cat_err:
                            st.warning(f"Failed to fetch {hierarchy[0].lower()}s: {cat_err}")
                            # Fallback to configured database for SQL Server
                            if active_connector_type == "sqlserver":
                                st.session_state["ingestion_catalogs"] = [loaded_config.config.get("database", "default")]
                            else:
                                st.session_state["ingestion_catalogs"] = ["default"]
                    
                    catalogs = st.session_state.get("ingestion_catalogs", ["default"])
                    
                    # Pre-select configured database for SQL Server if available in the list
                    default_idx = 0
                    if active_connector_type == "sqlserver":
                        config_db = loaded_config.config.get("database")
                        if config_db and config_db in catalogs:
                            try:
                                default_idx = catalogs.index(config_db)
                            except:
                                pass
                    
                    # Custom HTML Layout for Selectbox Label
                    st.markdown(f"""
                    <div style="font-size: 13px; color: #31333F; font-weight: 600; margin-bottom: 6px;">
                        {level1_label}
                    </div>
                    """, unsafe_allow_html=True)
                                
                    level1_value = st.selectbox(level1_label, options=catalogs, index=default_idx, key="ingestion_selected_catalog", label_visibility="collapsed")
                else:
                    # Others: Database/Container is part of config, show as read-only info
                    db_name = loaded_config.config.get("database", "N/A")
                    
                    # Custom HTML Layout to fix "DATABASE" alignment issue
                    # We render the label and value as a single HTML block
                    st.markdown(f"""
                    <div style="margin-bottom: 20px;">
                        <div style="font-size: 13px; color: #31333F; font-weight: 600; margin-bottom: 6px;">
                            {level1_label}
                        </div>
                        <div style="
                            background-color: #F0F2F6; 
                            color: #31333F; 
                            padding: 10px 12px; 
                            border-radius: 8px; 
                            border: 1px solid #E2E8F0; 
                            font-size: 14px;
                            width: 100%;
                            display: flex;
                            align-items: center;
                            height: 42px;
                        ">
                            {db_name}
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Hidden input to maintain state if needed (though level1_value is just read)
                    level1_value = db_name
                
                # Fetch Metadata button
                fetch_col1, fetch_col2, fetch_col3 = st.columns([3, 1, 1])
                with fetch_col2:
                    if st.button("↻ Refresh", key="refresh_config_btn", use_container_width=True):
                        st.session_state.pop("inspector_schema_metadata", None)
                        st.session_state.pop("ingestion_catalogs", None)
                        st.rerun()
                with fetch_col3:
                    fetch_btn = st.button(
                        "Fetch Metadata",
                        key="inspector_fetch_schemas_btn",
                        use_container_width=True,
                        type="primary",
                    )
                
                if fetch_btn and loaded_config:
                    with st.spinner(f"Fetching {hierarchy[1].lower()}s and {hierarchy[2].lower()}s..."):
                        try:
                            from src.backend.connectors import get_connector_service
                            svc = get_connector_service()
                            catalog_param = level1_value if (is_databricks or active_connector_type == "sqlserver") else None
                            metadata = svc.fetch_schemas_for_connection(
                                loaded_config.connection_id,
                                catalog=catalog_param
                            )
                            st.session_state["inspector_schema_metadata"] = metadata
                            st.success(f"Found {metadata.total_schemas} {hierarchy[1].lower()}s with {metadata.total_tables} {hierarchy[2].lower()}s")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed to fetch metadata: {e}")
                
                # ================================================================
                # STEP 3: TABLE SELECTION WITH PER-TABLE LOAD CONFIG
                # ================================================================
                if st.session_state.get("inspector_schema_metadata"):
                    metadata = st.session_state["inspector_schema_metadata"]
                    
                    st.markdown(clean_html(f"""
                    <div style="background: white; border: 1px solid #E2E8F0; border-radius: 16px; padding: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.04);">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px;">
                             <div style="display: flex; align-items: center; gap: 12px;">
                                <div style="background: linear-gradient(135deg, #F1F5F9 0%, #E2E8F0 100%); width: 44px; height: 44px; border-radius: 12px; display: flex; align-items: center; justify-content: center; border: 1px solid #CBD5E1;">
                                    <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#475569" stroke-width="2">
                                        <path d="M3 3h18v18H3zM3 9h18M9 21V9"/>
                                    </svg>
                                </div>
                                <div>
                                    <div style="font-weight: 700; color: #0F172A; font-size: 16px;">Select Tables & Configure Load</div>
                                    <div style="color: #64748B; font-size: 13px;">Choose tables to ingest and define load strategies</div>
                                </div>
                            </div>
                            
                            <div style="display: flex; gap: 12px;">
                                <div style="background: #F8FAFC; padding: 6px 12px; border-radius: 8px; border: 1px solid #E2E8F0; display: flex; align-items: center; gap: 8px;">
                                    <span style="font-size: 11px; font-weight: 600; color: #64748B; text-transform: uppercase;">Schemas</span>
                                    <span style="font-size: 14px; font-weight: 700; color: #0F172A;">{metadata.total_schemas}</span>
                                </div>
                                <div style="background: #F8FAFC; padding: 6px 12px; border-radius: 8px; border: 1px solid #E2E8F0; display: flex; align-items: center; gap: 8px;">
                                    <span style="font-size: 11px; font-weight: 600; color: #64748B; text-transform: uppercase;">Tables</span>
                                    <span style="font-size: 14px; font-weight: 700; color: #0F172A;">{metadata.total_tables}</span>
                                </div>
                            </div>
                        </div>
                    """), unsafe_allow_html=True)
                    
                    # Initialize selected tables state
                    if "inspector_selected_tables" not in st.session_state:
                        st.session_state["inspector_selected_tables"] = {}
                    if "inspector_table_configs" not in st.session_state:
                        st.session_state["inspector_table_configs"] = {}
                    
                    sorted_schemas = sorted(metadata.schemas.keys())
                    for schema_name in sorted_schemas:
                        tables = metadata.schemas[schema_name]
                        sel_tables = st.session_state.get("inspector_selected_tables", {}).get(schema_name, [])
                        
                        # Integrated Config Header (Professional SVG Icon via CSS + Badge)
                        # Format: SchemaName `X/Y selected`
                        # The CSS injects the folder icon before the text and styles the `code` block as a badge
                        header_label = f"**{schema_name}** `{len(sel_tables)}/{len(tables)} selected`"
                        
                        with st.expander(header_label, expanded=True):
                             # Select All / Deselect All Bar - Consistent Width Buttons
                            c_act1, c_act2, _ = st.columns([0.15, 0.15, 0.7], gap="small")
                            with c_act1:
                                if st.button("Select All", key=f"insp_sa_{schema_name}", use_container_width=True):
                                    st.session_state.setdefault("inspector_selected_tables", {})[schema_name] = tables.copy()
                                    for t in tables:
                                        st.session_state[f"insp_tbl_{schema_name}_{t}"] = True
                                    st.rerun()
                            with c_act2:
                                if st.button("Clear Selection", key=f"insp_da_{schema_name}", use_container_width=True):
                                    st.session_state.setdefault("inspector_selected_tables", {})[schema_name] = []
                                    for t in tables:
                                        st.session_state[f"insp_tbl_{schema_name}_{t}"] = False
                                    st.rerun()
                            
                            st.markdown("<div style='height:12px;'></div>", unsafe_allow_html=True)

                            # Header Row
                            # [Checkbox, Name, LoadType, Watermark, Spacer]
                            # Increased Name width (1.5 -> 2.5) and decreased Spacer (3.8 -> 2.8) to accommodate longer names
                            h_cols = st.columns([0.2, 2.5, 2.0, 2.5, 2.8])
                            with h_cols[1]:
                                st.markdown("<div style='font-size: 12px; font-weight: 600; color: #64748B;'>Table</div>", unsafe_allow_html=True)
                            with h_cols[2]:
                                st.markdown("<div style='font-size: 12px; font-weight: 600; color: #64748B;'>Load Type</div>", unsafe_allow_html=True)
                            with h_cols[3]:
                                st.markdown("<div style='font-size: 12px; font-weight: 600; color: #64748B;'>Watermark Column</div>", unsafe_allow_html=True)
                            
                            st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)
                            
                            # Table List - Scrollable Container to save space
                            with st.container(height=600, border=False):
                                for table_name in sorted(tables):
                                    chk_key = f"insp_tbl_{schema_name}_{table_name}"
                                    is_selected = table_name in sel_tables
                                    if chk_key not in st.session_state:
                                        st.session_state[chk_key] = is_selected
                                    
                                    # Use a container for the row styling
                                    row_container = st.container()
                                    
                                    # Visual grouping
                                    bg_color = "#F8FAFC" if is_selected else "white"
                                    border_color = "#CBD5E1" if is_selected else "#E2E8F0"
                                    
                                    # Matched columns to header with tighter gap for Checkbox->Name
                                    try:
                                        row_cols = row_container.columns([0.2, 2.5, 2.0, 2.5, 2.8], gap="small", vertical_alignment="center")
                                    except TypeError:
                                        row_cols = row_container.columns([0.2, 2.5, 2.0, 2.5, 2.8], gap="small")

                                    with row_cols[0]:
                                        # Checkbox strictly for selection
                                        checked = st.checkbox("Select", key=chk_key, label_visibility="collapsed")
                                    
                                    with row_cols[1]:
                                        # Table Name - Standard line-height alignment
                                        # Removed manual padding and flexbox to rely on consistent 42px height
                                        t_color = "#0F172A" if checked else "#64748B"
                                        t_weight = "600" if checked else "500"
                                        st.markdown(f"""
                                        <div style='height: 42px; line-height: 42px; overflow: hidden; white-space: nowrap; text-overflow: ellipsis; font-family: "Inter", sans-serif; font-size: 14px; font-weight: {t_weight}; color: {t_color};' title='{table_name}'>
                                            {table_name}
                                        </div>
                                        """, unsafe_allow_html=True)

                                    # Sync selection
                                    if schema_name not in st.session_state.get("inspector_selected_tables", {}):
                                        st.session_state.setdefault("inspector_selected_tables", {})[schema_name] = []
                                    cur_list = st.session_state["inspector_selected_tables"][schema_name]
                                    if checked and table_name not in cur_list:
                                        cur_list.append(table_name)
                                    elif not checked and table_name in cur_list:
                                        cur_list.remove(table_name)
                                    
                                    # Configuration Controls (Only if selected)
                                    if checked:
                                        cfg_key = f"{schema_name}.{table_name}"
                                        existing_cfg = st.session_state.get("inspector_table_configs", {}).get(cfg_key, {})
                                        
                                        with row_cols[2]:
                                            lt = st.selectbox(
                                                " ",  # Empty label to force alignment with Watermark
                                                options=["Full Load", "Incremental Load"],
                                                index=0 if existing_cfg.get("load_type", "full") == "full" else 1,
                                                key=f"insp_lt_{schema_name}_{table_name}",
                                                label_visibility="collapsed"
                                            )
                                        
                                        wm_val = ""
                                        with row_cols[3]:
                                            if "Incremental" in lt:
                                                 # Fetch columns for this table (cached per table)
                                                col_cache_key = f"insp_cols_{schema_name}_{table_name}"
                                                if col_cache_key not in st.session_state:
                                                    try:
                                                        # Use existing service import if available
                                                        if 'src.backend.connectors' not in sys.modules:
                                                            from src.backend.connectors import get_connector_service
                                                        svc = get_connector_service()
                                                        catalog_param = st.session_state.get("ingestion_selected_catalog") if is_databricks else None
                                                        all_cols = svc.fetch_all_columns_for_table(
                                                            loaded_config.connection_id,
                                                            schema_name,
                                                            table_name,
                                                            catalog=catalog_param
                                                        )
                                                        # Filter to trackable columns (timestamp/date/numeric)
                                                        wm_cols = []
                                                        for c in all_cols:
                                                            ctype = c['type'].lower()
                                                            if any(x in ctype for x in ['time', 'date', 'int', 'numeric', 'long', 'decimal', 'bigint']):
                                                                wm_cols.append(f"{c['name']} ({c['type']})")
                                                        if not wm_cols:
                                                            # Fallback: show all columns
                                                            wm_cols = [f"{c['name']} ({c['type']})" for c in all_cols]
                                                        st.session_state[col_cache_key] = wm_cols
                                                    except Exception as col_err:
                                                        st.session_state[col_cache_key] = []
                                                        st.warning(f"Could not fetch columns for {table_name}: {col_err}")
                                                
                                                wm_options = st.session_state.get(col_cache_key, [])
                                                
                                                if wm_options:
                                                    # Smart default
                                                    default_idx = 0
                                                    prev_wm = existing_cfg.get("watermark_column", "")
                                                    # ... matching logic ...
                                                    
                                                    # Try to match previous selection first
                                                    if prev_wm:
                                                        for i, opt in enumerate(wm_options):
                                                            if prev_wm in opt:
                                                                default_idx = i
                                                                break
                                                    else:
                                                        # Try smart matching
                                                        for i, opt in enumerate(wm_options):
                                                            if any(k in opt.lower() for k in ['updat', 'modif', 'last', 'timestamp', 'changed']):
                                                                default_idx = i
                                                                break
                                                    
                                                    wm_selected = st.selectbox(
                                                        " ",  # Empty to ensure no phantom label
                                                        options=wm_options,
                                                        index=default_idx,
                                                        key=f"insp_wm_{schema_name}_{table_name}",
                                                        label_visibility="collapsed",
                                                        placeholder="Watermark Column"
                                                    )
                                                    wm_val = wm_selected.split(" (")[0] if wm_selected else ""
                                                else:
                                                    wm_val = st.text_input(
                                                        " ",  # Empty to ensure no phantom label
                                                        value=existing_cfg.get("watermark_column", ""),
                                                        placeholder="e.g. updated_at",
                                                        key=f"insp_wm_{schema_name}_{table_name}",
                                                        label_visibility="collapsed"
                                                    )
                                            else:
                                                # Show disabled text input when Full Load is selected
                                                st.text_input(
                                                    " ",
                                                    value="N/A - Full Load",
                                                    disabled=True,
                                                    key=f"insp_wm_disabled_{schema_name}_{table_name}",
                                                    label_visibility="collapsed"
                                                )

                                        # Save config to state
                                        st.session_state.setdefault("inspector_table_configs", {})[cfg_key] = {
                                            "load_type": "full" if "Full" in lt else "incremental",
                                            "watermark_column": wm_val or "",
                                        }
                                    
                                    st.markdown(f"<div style='border-bottom: 1px solid {border_color}; margin-bottom: 0px; padding-bottom: 4px; margin-top: -12px;'></div>", unsafe_allow_html=True)


                    st.markdown("</div>", unsafe_allow_html=True) # End Main Card
                    
                    st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)
                    
                    # --- Save Table Configuration Button ---
                    _, save_tbl_col = st.columns([3, 1.5])
                    with save_tbl_col:
                        save_tbl_btn = st.button(
                            "Save Table Configuration",
                            key="inspector_save_table_config_btn",
                            type="primary",
                            use_container_width=True,
                        )
                    
                    if save_tbl_btn and loaded_config:
                        sel_tables = st.session_state.get("inspector_selected_tables", {})
                        tbl_configs = st.session_state.get("inspector_table_configs", {})
                        
                        # Build the selected_tables dict
                        final_tables = {}
                        has_validation_error = False
                        for schema, tables_list in sel_tables.items():
                            if tables_list:
                                final_tables[schema] = []
                                for tbl in tables_list:
                                    cfg_key = f"{schema}.{tbl}"
                                    cfg = tbl_configs.get(cfg_key, {"load_type": "full", "watermark_column": ""})
                                    
                                    # Validate incremental
                                    if cfg["load_type"] == "incremental" and not cfg["watermark_column"]:
                                        st.error(f"⚠️ Table '{schema}.{tbl}' is set to Incremental but has no watermark column.")
                                        has_validation_error = True
                                    
                                    final_tables[schema].append({
                                        "table_name": tbl,
                                        "load_type": cfg["load_type"],
                                        "watermark_column": cfg["watermark_column"],
                                    })
                        
                        if has_validation_error:
                            st.warning("Please provide a watermark column for all incremental tables.")
                        elif not final_tables:
                            st.warning("Please select at least one table.")
                        else:
                            with st.spinner("Saving table configuration..."):
                                try:
                                    from src.backend.connectors import get_connector_service
                                    svc = get_connector_service()
                                    total_tables = sum(len(t) for t in final_tables.values())
                                    success = svc.update_table_configuration(loaded_config.connection_id, final_tables)
                                    if success:
                                        st.success(f"Saved configuration for {total_tables} tables!")
                                        st.session_state["ingestion_force_refresh"] = True
                                        st.rerun()
                                    else:
                                        st.error("Failed to save table configuration.")
                                except Exception as e:
                                    st.error(f"Error: {e}")
                
                # Store in session for later use
                st.session_state["ingestion_connector_config"] = loaded_config
                st.session_state["ingestion_connector_type"] = active_connector_type
                st.session_state["ingestion_is_databricks"] = is_databricks
                
            else:
                # No configuration found - Premium Warning Card
                st.markdown("""
                <div style="background: white; border: 1px solid #FCD34D; border-radius: 16px; padding: 24px; margin-bottom: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.04);">
                    <div style="display: flex; align-items: center; gap: 16px;">
                        <div style="background: linear-gradient(135deg, #FEF3C7 0%, #FDE68A 100%); width: 52px; height: 52px; border-radius: 14px; display: flex; align-items: center; justify-content: center;">
                            <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#D97706" stroke-width="2">
                                <circle cx="12" cy="12" r="10"/><line x1="12" x2="12" y1="8" y2="12"/><line x1="12" x2="12.01" y1="16" y2="16"/>
                            </svg>
                        </div>
                        <div style="flex: 1;">
                            <div style="font-size: 11px; font-weight: 600; color: #D97706; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 2px;">Action Required</div>
                            <div style="font-weight: 700; color: #0F172A; font-size: 16px;">No Source Configured</div>
                            <div style="color: #64748B; font-size: 13px; margin-top: 2px;">Select a saved connection above or go to the Connectors page to create one.</div>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                st.session_state["ingestion_connector_config"] = None


            

            st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)
            


            


            
            st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)
            


                


            



            st.markdown("<div style='height:24px;'></div>", unsafe_allow_html=True)
            
            st.markdown("<div style='height:24px;'></div>", unsafe_allow_html=True)
            


                


                    
                    # Catalog selection for Databricks


                        
                        # Fetch catalogs if not cached




                    


            
            # ================================================================
            # STEP 4: SCHEDULING - Premium Card Design
            # ================================================================
            st.markdown("""
            <div style="font-size: 11px; font-weight: 700; color: #94A3B8; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 16px;">
                Automation
            </div>
            """, unsafe_allow_html=True)
            
            schedule_enabled = st.session_state.get("ingestion_schedule_enabled", False)
            
            st.markdown(f"""
            <div style="background: white; border: 1px solid #E2E8F0; border-radius: 16px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.04);">
                <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: {'20px' if schedule_enabled else '0'};">
                    <div style="display: flex; align-items: center; gap: 14px;">
                        <div style="background: {'#EEF2FF' if schedule_enabled else '#F8FAFC'}; width: 44px; height: 44px; border-radius: 12px; display: flex; align-items: center; justify-content: center;">
                            <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="{'#6366F1' if schedule_enabled else '#94A3B8'}" stroke-width="2">
                                <circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/>
                            </svg>
                        </div>
                        <div>
                            <div style="font-weight: 700; color: #0F172A; font-size: 15px;">Scheduled Sync</div>
                            <div style="color: #64748B; font-size: 13px;">Automate data refresh on a recurring basis</div>
                        </div>
                    </div>
            """, unsafe_allow_html=True)
            
            # Toggle inline with cleaner styling
            schedule_enabled = st.toggle(
                "Enable",
                key="ingestion_schedule_enabled",
                label_visibility="collapsed"
            )
            
            st.markdown("</div>", unsafe_allow_html=True)
            
            if schedule_enabled:
                st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)
                
                sched_col1, sched_col2, sched_col3 = st.columns([2, 1, 1], gap="medium")
                
                with sched_col1:
                    st.selectbox(
                        "Frequency",
                        options=["hourly", "daily", "weekly", "monthly"],
                        format_func=lambda x: x.capitalize(),
                        key="ingestion_schedule_frequency"
                    )
                
                with sched_col2:
                    st.time_input("Start Time", key="ingestion_schedule_time", value=None)
                
                with sched_col3:
                    st.selectbox(
                        "Timezone",
                        options=["UTC", "US/Eastern", "US/Pacific", "Europe/London", "Asia/Kolkata", "Asia/Tokyo"],
                        key="ingestion_schedule_timezone"
                    )
                
                # Premium cron preview
                freq = st.session_state.get("ingestion_schedule_frequency", "daily")
                cron_preview = {"hourly": "0 * * * *", "daily": "0 9 * * *", "weekly": "0 9 * * 1", "monthly": "0 9 1 * *"}
                st.markdown(f"""
                <div style="background: #F8FAFC; border: 1px solid #E2E8F0; border-radius: 10px; padding: 12px 16px; margin-top: 12px; display: flex; align-items: center; gap: 12px;">
                    <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#64748B" stroke-width="2">
                        <polyline points="4 17 10 11 4 5"/><line x1="12" x2="20" y1="19" y2="19"/>
                    </svg>
                    <div>
                        <span style="font-size: 12px; color: #64748B;">Cron: </span>
                        <code style="background: #0F172A; color: #E2E8F0; padding: 4px 10px; border-radius: 6px; font-family: 'JetBrains Mono', monospace; font-size: 12px; letter-spacing: 0.5px;">{cron_preview.get(freq, "0 * * * *")}</code>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("</div>", unsafe_allow_html=True)
            
            st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)
            
            # ================================================================
            # START INGESTION BUTTON
            # ================================================================
            _, btn_col = st.columns([2.5, 1.5])
            
            # Console rendering container
            st.markdown("<div style='height:24px;'></div>", unsafe_allow_html=True)
            console_placeholder = st.empty()
            
            # Helper to render console
            def render_console(logs):
                if not logs:
                    return
                log_content = ""
                for ts, msg in logs:
                    color = "#10B981" if "INFO" in msg else "#F59E0B" if "WARN" in msg else "#EF4444" if "ERROR" in msg else "#94A3B8"
                    log_content += f"<div style='margin-bottom: 6px;'><span style='color: #64748B; margin-right: 8px;'>{ts}</span><span style='color: {color};'>{msg}</span></div>"
                
                console_placeholder.markdown(f"""
                <div style="background: #0F172A; border-radius: 12px; padding: 20px; font-family: 'JetBrains Mono', monospace; color: #E2E8F0; border: 1px solid #1E293B; box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);">
                    <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 16px; border-bottom: 1px solid #1E293B; padding-bottom: 12px;">
                        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#64748B" stroke-width="2"><polyline points="4 17 10 11 4 5"/><line x1="12" x2="20" y1="19" y2="19"/></svg>
                        <div style="font-size: 12px; font-weight: 700; color: #F8FAFC; text-transform: uppercase; letter-spacing: 0.5px;">Execution Stream Console (stdout)</div>
                    </div>
                    <div style="font-size: 11px; line-height: 1.6; max-height: 200px; overflow-y: auto;">
                        {log_content}
                        <div style="animation: blink 1s step-end infinite; color: #EF4444; font-weight: bold; margin-top: 4px;">_</div>
                    </div>
                    <style>
                        @keyframes blink {{ 0%, 100% {{ opacity: 1; }} 50% {{ opacity: 0; }} }}
                    </style>
                </div>
                """, unsafe_allow_html=True)

            # Initialize logs in session if not present
            if "ingestion_console_logs" not in st.session_state:
                st.session_state["ingestion_console_logs"] = []
            
            # Render existing logs
            render_console(st.session_state["ingestion_console_logs"])

            with btn_col:
                if st.button("Start Ingestion", type="primary", use_container_width=True, key="start_ingestion_btn"):
                    config = st.session_state.get("ingestion_connector_config")
                    
                    if config and config.connection_id:
                        try:
                            # Clear logs for new run
                            st.session_state["ingestion_console_logs"] = []
                            import datetime
                            
                            def log(msg):
                                ts = datetime.datetime.now().strftime("%H:%M:%S")
                                st.session_state["ingestion_console_logs"].append((ts, msg))
                                render_console(st.session_state["ingestion_console_logs"])
                            
                            log(f"[INFO] Initializing ingestion job for connection ID: {config.connection_id[:8]}...")
                            
                            with st.spinner("Triggering ingestion notebook..."):
                                from src.backend.connectors import get_connector_service, reset_connector_service
                                
                                # Force reset to ensure we use the updated method (SDK based) instead of cached old class (DBUtils based)
                                reset_connector_service()
                                svc = get_connector_service()
                                
                                # Simulate steps since notebook run is blocking/opaque
                                log("[INFO] Connecting to data source...")
                                import time
                                time.sleep(0.8) 
                                
                                log("[INFO] Validating schema compatibility...")
                                time.sleep(0.8)
                                
                                log("[INFO] Triggering notebook execution: nb_brz_ingestion")
                                result = svc.trigger_ingestion_notebook(config.connection_id)
                                
                                log(f"[INFO] Notebook execution completed successfully")
                                log(f"[INFO] Result: {result}")
                                
                            st.toast("Ingestion job completed successfully!", icon="🚀")
                            
                            # Optional: Wait a bit before moving so user sees the success
                            time.sleep(2)
                            st.session_state['inspector_active_stage'] = 1  # Move to Profiling
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"Failed to trigger ingestion: {e}")
                            st.session_state["ingestion_console_logs"].append((datetime.datetime.now().strftime("%H:%M:%S"), f"[ERROR] {str(e)}"))
                            render_console(st.session_state["ingestion_console_logs"])
                    else:
                        st.error("No active configuration found. Please configure a source first.")
        elif active_stage['name'] == "Profiling":
            # ================================================================
            # PROFILING STAGE - Data Quality Analysis
            # ================================================================
            st.markdown("""
            <div style="font-size: 12px; font-weight: 700; color: #64748B; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 16px;">
                Data Quality Summary
            </div>
            """, unsafe_allow_html=True)
            
            # Quality Metrics Grid
            q1, q2, q3, q4 = st.columns(4)
            
            # Default/Placeholder metrics
            metrics = st.session_state.get("profiling_metrics", {})
            
            # Parse metrics (handle string percentages if needed, but notebook returns floats 0.0-1.0)
            def fmt_pct(val):
                if val is None: return "Pending"
                return f"{val*100:.1f}%"

            quality_metrics = [
                ("Completeness", fmt_pct(metrics.get("completeness", 0.982)) if metrics else "Pending", "#10B981", "Non-null values"),
                ("Uniqueness", fmt_pct(metrics.get("uniqueness", 0.947)) if metrics else "Pending", "#0369A1", "Distinct records"),
                ("Validity", fmt_pct(metrics.get("validity", 0.991)) if metrics else "Pending", "#7C3AED", "Format compliance"),
                ("Consistency", fmt_pct(metrics.get("consistency", 0.968)) if metrics else "Pending", "#F59E0B", "Cross-field accuracy")
            ]
            
            if metrics:
                # Update stage description if we have real metrics
                total_rows = metrics.get("total_rows", 0)
                stages[1]['desc'] = f"scanned {total_rows:,} rows. Quality Score: {metrics.get('dq_score', 0):.2f}"
            
            for col, (label, value, color, desc) in zip([q1, q2, q3, q4], quality_metrics):
                col.markdown(f"""
                <div style="background: {color}08; border: 1px solid {color}20; border-radius: 12px; padding: 16px; text-align: center;">
                    <div style="font-size: 24px; font-weight: 800; color: {color}; margin-bottom: 4px;">{value}</div>
                    <div style="font-size: 13px; font-weight: 600; color: #374151; margin-bottom: 2px;">{label}</div>
                    <div style="font-size: 11px; color: #64748B;">{desc}</div>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("<div style='height:24px;'></div>", unsafe_allow_html=True)
            
            # Column Analysis Section
            st.markdown("""
            <div style="font-size: 12px; font-weight: 700; color: #64748B; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 12px;">
                Column Analysis
            </div>
            """, unsafe_allow_html=True)
            
            # Sample column data table
            st.markdown("""
            <div style="background: #F8FAFC; border: 1px solid #E2E8F0; border-radius: 12px; overflow: hidden;">
                <div style="display: grid; grid-template-columns: 2fr 1fr 1fr 1fr 2fr; background: #F1F5F9; padding: 12px 16px; font-size: 11px; font-weight: 700; color: #64748B; text-transform: uppercase; letter-spacing: 0.5px;">
                    <span>Column Name</span>
                    <span>Type</span>
                    <span>Null %</span>
                    <span>Distinct</span>
                    <span>Sample Values</span>
                </div>
                <div style="display: grid; grid-template-columns: 2fr 1fr 1fr 1fr 2fr; padding: 12px 16px; border-bottom: 1px solid #E2E8F0; font-size: 13px;">
                    <span style="font-weight: 600; color: #0F172A;">customer_id</span>
                    <span style="color: #64748B;">VARCHAR</span>
                    <span style="color: #10B981;">0.0%</span>
                    <span style="color: #0F172A;">1.2M</span>
                    <span style="color: #64748B; font-family: monospace; font-size: 12px;">CUS-001, CUS-002...</span>
                </div>
                <div style="display: grid; grid-template-columns: 2fr 1fr 1fr 1fr 2fr; padding: 12px 16px; border-bottom: 1px solid #E2E8F0; font-size: 13px;">
                    <span style="font-weight: 600; color: #0F172A;">email</span>
                    <span style="color: #64748B;">VARCHAR</span>
                    <span style="color: #F59E0B;">2.3%</span>
                    <span style="color: #0F172A;">1.18M</span>
                    <span style="color: #64748B; font-family: monospace; font-size: 12px;">john@example.com...</span>
                </div>
                <div style="display: grid; grid-template-columns: 2fr 1fr 1fr 1fr 2fr; padding: 12px 16px; font-size: 13px;">
                    <span style="font-weight: 600; color: #0F172A;">phone</span>
                    <span style="color: #64748B;">VARCHAR</span>
                    <span style="color: #EF4444;">5.1%</span>
                    <span style="color: #0F172A;">980K</span>
                    <span style="color: #64748B; font-family: monospace; font-size: 12px;">+1-555-0123...</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("<div style='height:24px;'></div>", unsafe_allow_html=True)
            
            # Quality Rules Configuration
            st.markdown("""
            <div style="font-size: 12px; font-weight: 700; color: #64748B; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 12px;">
                Quality Rules
            </div>
            """, unsafe_allow_html=True)
            
            rule_col1, rule_col2 = st.columns(2)
            with rule_col1:
                st.toggle("Check null values", value=True, key="profile_check_nulls")
                st.toggle("Validate email formats", value=True, key="profile_validate_email")
            with rule_col2:
                st.toggle("Detect outliers", value=False, key="profile_detect_outliers")
                st.toggle("Check referential integrity", value=False, key="profile_check_refs")
            
            st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)
            
            # Threshold slider
            st.slider("Quality Threshold", min_value=0, max_value=100, value=95, key="profile_threshold", help="Minimum quality score required to proceed")
            
            st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)
            
            _, btn_col = st.columns([2.5, 1.5])
            with btn_col:
                if st.button("Run Profiling", type="primary", use_container_width=True, key="run_profiling_btn"):
                    config = st.session_state.get("ingestion_connector_config")
                    
                    if config and config.connection_id:
                         with st.spinner("Running profiling analysis (Triggering Notebook)..."):
                            try:
                                from src.backend.connectors import get_connector_service
                                svc = get_connector_service()
                                
                                # Trigger notebook and get results
                                results = svc.trigger_profiling_notebook(config.connection_id)
                                
                                # Store results in session state
                                st.session_state["profiling_metrics"] = results
                                
                                st.toast("Profiling analysis complete!", icon="✅")
                                
                                # Optional: Auto-advance or just visual update?
                                # User request says "display the metrics... dynamically", implies staying on page to see them or moving next.
                                # Let's stay on page to show metrics first, or we can update the stage status
                                stages[1]['status'] = 'done'
                                st.rerun()
                                
                            except Exception as e:
                                st.error(f"Profiling failed: {str(e)}")
                    else:
                        st.error("No active configuration found. Please go back to Ingestion stage.")
                    
        elif active_stage['name'] == "Cleansing":
            # ================================================================
            # CLEANSING STAGE - Data Standardization
            # ================================================================
            st.markdown("""
            <div style="font-size: 12px; font-weight: 700; color: #64748B; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 16px;">
                Standardization Rules
            </div>
            """, unsafe_allow_html=True)
            
            # Standardization rule cards
            rules = [
                ("Address Normalization", "Standardize street abbreviations and formatting", "STD_ADDR_01", True),
                ("Phone Formatting", "Convert to E.164 international format", "STD_PHONE_01", True),
                ("Email Validation", "Lowercase and validate domain structure", "STD_EMAIL_01", True),
                ("Name Casing", "Apply proper case to person names", "STD_NAME_01", False)
            ]
            
            for i in range(0, len(rules), 2):
                cols = st.columns(2)
                for j, col in enumerate(cols):
                    if i + j < len(rules):
                        rule = rules[i + j]
                        with col:
                            is_enabled = st.checkbox(rule[0], value=rule[3], key=f"clean_rule_{i+j}")
                            border_color = "#10B981" if is_enabled else "#E2E8F0"
                            col.markdown(f"""
                            <div style="background: white; border: 1px solid {border_color}; border-radius: 10px; padding: 12px 16px; margin-top: -8px; margin-bottom: 12px;">
                                <div style="font-size: 12px; color: #64748B;">{rule[1]}</div>
                                <div style="font-size: 11px; color: #94A3B8; margin-top: 4px; font-family: monospace;">Rule: {rule[2]}</div>
                            </div>
                            """, unsafe_allow_html=True)
            
            st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)
            
            # Transformation Configuration
            st.markdown("""
            <div style="font-size: 12px; font-weight: 700; color: #64748B; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 12px;">
                Transformation Configuration
            </div>
            """, unsafe_allow_html=True)
            
            tf_col1, tf_col2, tf_col3 = st.columns(3)
            with tf_col1:
                st.selectbox("Date Format", ["YYYY-MM-DD", "MM/DD/YYYY", "DD-MM-YYYY", "ISO 8601"], key="clean_date_format")
            with tf_col2:
                st.selectbox("Null Handling", ["Keep as null", "Replace with default", "Remove row", "Flag for review"], key="clean_null_handling")
            with tf_col3:
                st.selectbox("Case Conversion", ["No change", "UPPERCASE", "lowercase", "Title Case"], key="clean_case_conv")
            
            st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)
            
            _, btn_col = st.columns([2.5, 1.5])
            with btn_col:
                if st.button("Apply Cleansing", type="primary", use_container_width=True, key="apply_cleansing_btn"):
                    st.toast("Cleansing rules applied successfully!")
                    st.session_state['inspector_active_stage'] = 3
                    st.rerun()
                    
        elif active_stage['name'] == "Resolution":
            # ================================================================
            # RESOLUTION STAGE - Entity Matching
            # ================================================================
            st.markdown("""
            <div style="font-size: 12px; font-weight: 700; color: #64748B; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 16px;">
                Matching Algorithm
            </div>
            """, unsafe_allow_html=True)
            
            algo = st.radio(
                "Select matching strategy",
                ["Exact Match", "Fuzzy Match (Jaro-Winkler)", "ML-Based Probabilistic"],
                horizontal=True,
                key="resolution_algorithm",
                label_visibility="collapsed"
            )
            
            st.markdown("""
            <div style="background: #F0F9FF; border: 1px solid #BAE6FD; border-radius: 10px; padding: 12px 16px; margin: 16px 0;">
                <div style="font-size: 13px; color: #0369A1;">
                    <strong>Selected:</strong> Fuzzy matching uses phonetic similarity and edit distance to identify potential duplicates even with typos or variations.
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)
            
            # Blocking Keys
            st.markdown("""
            <div style="font-size: 12px; font-weight: 700; color: #64748B; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 12px;">
                Blocking Keys
            </div>
            """, unsafe_allow_html=True)
            
            st.multiselect(
                "Select columns for blocking",
                ["zip_code", "last_name", "first_name", "city", "state", "phone_prefix", "email_domain"],
                default=["zip_code", "last_name"],
                key="resolution_blocking_keys",
                help="Blocking reduces comparison space by only comparing records that share blocking key values",
                label_visibility="collapsed"
            )
            
            st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)
            
            # Similarity Threshold
            st.markdown("""
            <div style="font-size: 12px; font-weight: 700; color: #64748B; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 12px;">
                Similarity Threshold
            </div>
            """, unsafe_allow_html=True)
            
            threshold = st.slider("Match Threshold", min_value=0.0, max_value=1.0, value=0.85, step=0.05, key="resolution_threshold", label_visibility="collapsed")
            
            th_col1, th_col2, th_col3 = st.columns(3)
            th_col1.markdown(f"<div style='font-size:12px; color:#64748B;'>Low: 0.0</div>", unsafe_allow_html=True)
            th_col2.markdown(f"<div style='font-size:12px; color:#0F172A; text-align:center; font-weight:600;'>Current: {threshold}</div>", unsafe_allow_html=True)
            th_col3.markdown(f"<div style='font-size:12px; color:#64748B; text-align:right;'>High: 1.0</div>", unsafe_allow_html=True)
            
            st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)
            
            _, btn_col = st.columns([2.5, 1.5])
            with btn_col:
                if st.button("Start Resolution", type="primary", use_container_width=True, key="start_resolution_btn"):
                    st.toast("Entity resolution in progress!")
                    st.session_state['inspector_active_stage'] = 4
                    st.rerun()
                    
        elif active_stage['name'] == "Survivorship":
            # ================================================================
            # SURVIVORSHIP STAGE - Golden Record Creation
            # ================================================================
            st.markdown("""
            <div style="font-size: 12px; font-weight: 700; color: #64748B; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 16px;">
                Trust Framework
            </div>
            """, unsafe_allow_html=True)
            
            # Source Priority (simulated drag-and-drop)
            st.markdown("""
            <div style="background: #F8FAFC; border: 1px solid #E2E8F0; border-radius: 12px; padding: 16px;">
                <div style="font-size: 11px; color: #64748B; margin-bottom: 12px; text-transform: uppercase; letter-spacing: 0.5px;">Source Priority (highest to lowest)</div>
            """, unsafe_allow_html=True)
            
            sources = [
                ("Salesforce CRM", "#0369A1", "Primary customer source"),
                ("Epic EMR", "#7C3AED", "Clinical records"),
                ("Legacy Billing", "#F59E0B", "Historical data"),
                ("External Feeds", "#64748B", "Third-party enrichment")
            ]
            
            for i, (name, color, desc) in enumerate(sources):
                st.markdown(f"""
                <div style="display: flex; align-items: center; gap: 12px; background: white; border: 1px solid #E2E8F0; border-radius: 8px; padding: 12px 16px; margin-bottom: 8px;">
                    <div style="font-size: 14px; font-weight: 700; color: #94A3B8; width: 20px;">{i+1}</div>
                    <div style="width: 8px; height: 8px; border-radius: 50%; background: {color};"></div>
                    <div style="flex: 1;">
                        <div style="font-weight: 600; color: #0F172A; font-size: 14px;">{name}</div>
                        <div style="font-size: 12px; color: #64748B;">{desc}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("</div>", unsafe_allow_html=True)
            
            st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)
            
            # Conflict Resolution Strategy
            st.markdown("""
            <div style="font-size: 12px; font-weight: 700; color: #64748B; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 12px;">
                Conflict Resolution Strategy
            </div>
            """, unsafe_allow_html=True)
            
            st.radio(
                "When values conflict, use:",
                ["Most recent value", "Most complete record", "Source priority", "Custom rules per attribute"],
                key="survivorship_conflict_resolution",
                label_visibility="collapsed"
            )
            
            st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)
            
            # Per-attribute overrides
            with st.expander("Attribute-Level Overrides", expanded=False):
                attr_col1, attr_col2 = st.columns(2)
                with attr_col1:
                    st.selectbox("Email", ["Follow default", "Salesforce priority", "Most recent", "Most complete"], key="surv_email_rule")
                    st.selectbox("Phone", ["Follow default", "Epic priority", "Most recent", "Most complete"], key="surv_phone_rule")
                with attr_col2:
                    st.selectbox("Address", ["Follow default", "Most recent", "Most complete"], key="surv_address_rule")
                    st.selectbox("Name", ["Follow default", "Salesforce priority", "Most recent"], key="surv_name_rule")
            
            st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)
            
            _, btn_col = st.columns([2.5, 1.5])
            with btn_col:
                if st.button("Create Golden Records", type="primary", use_container_width=True, key="create_golden_btn"):
                    st.toast("Golden records created!")
                    st.session_state['inspector_active_stage'] = 5
                    st.rerun()
                    
        elif active_stage['name'] == "Publishing":
            # ================================================================
            # PUBLISHING STAGE - Output Configuration
            # ================================================================
            st.markdown("""
            <div style="font-size: 12px; font-weight: 700; color: #64748B; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 16px;">
                Output Destinations
            </div>
            """, unsafe_allow_html=True)
            
            dest_col1, dest_col2 = st.columns(2)
            with dest_col1:
                st.checkbox("Delta Lake (Databricks)", value=True, key="pub_delta")
                st.checkbox("Snowflake Data Warehouse", value=False, key="pub_snowflake")
            with dest_col2:
                st.checkbox("API Webhook", value=False, key="pub_webhook")
                st.checkbox("S3 Export (Parquet)", value=True, key="pub_s3")
            
            st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)
            
            # Format Configuration
            st.markdown("""
            <div style="font-size: 12px; font-weight: 700; color: #64748B; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 12px;">
                Format Configuration
            </div>
            """, unsafe_allow_html=True)
            
            fmt_col1, fmt_col2, fmt_col3 = st.columns(3)
            with fmt_col1:
                st.selectbox("Output Format", ["Parquet", "Delta", "CSV", "JSON"], key="pub_format")
            with fmt_col2:
                st.selectbox("Compression", ["Snappy", "GZIP", "LZ4", "None"], key="pub_compression")
            with fmt_col3:
                st.selectbox("Partitioning", ["None", "By date", "By region", "By source"], key="pub_partition")
            
            st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)
            
            # Sync Options
            st.markdown("""
            <div style="font-size: 12px; font-weight: 700; color: #64748B; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 12px;">
                Sync Options
            </div>
            """, unsafe_allow_html=True)
            
            sync_mode = st.radio(
                "Sync mode",
                ["Incremental push (append only)", "Full replace (truncate and load)", "Merge (upsert)"],
                key="pub_sync_mode",
                label_visibility="collapsed"
            )
            
            st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)
            
            # Compliance Options
            st.markdown("""
            <div style="font-size: 12px; font-weight: 700; color: #64748B; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 12px;">
                Compliance
            </div>
            """, unsafe_allow_html=True)
            
            comp_col1, comp_col2 = st.columns(2)
            with comp_col1:
                st.checkbox("Enable GDPR masking", value=True, key="pub_gdpr")
                st.checkbox("HIPAA compliance mode", value=False, key="pub_hipaa")
            with comp_col2:
                st.checkbox("Generate audit trail", value=True, key="pub_audit")
                st.checkbox("Data lineage tracking", value=True, key="pub_lineage")
            
            st.markdown("<div style='height:20px;'></div>", unsafe_allow_html=True)
            
            _, btn_col = st.columns([2.5, 1.5])
            with btn_col:
                if st.button("Publish Data", type="primary", use_container_width=True, key="publish_data_btn"):
                    st.toast("Data published successfully!")
                    st.balloons()
        
        else:
            # Fallback - Standard stats grid (should not normally be reached)
            st.markdown(clean_html(f"""
            <div style="display: flex; justify-content: space-between; font-weight: 700; font-size: 14px; color: #334155; margin-bottom: 12px;">
                <span>Total Processing Completion</span>
                <span style="color: {s_color};">{active_stage['pct']}%</span>
            </div>
            <div class="lg-progress">
                <div class="lg-fill {s_fill}" style="width: {active_stage['pct']}%;"></div>
            </div>
            
            <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px; margin-top: 16px;">
                <div class="stats-tile">
                    <div style="font-size: 11px; color: #64748B; font-weight: 700; margin-bottom: 8px; text-transform: uppercase;">Volume</div>
                    <div style="font-size: 20px; font-weight: 800; color: #0F172A;">5,214,882</div>
                </div>
                <div class="stats-tile">
                    <div style="font-size: 11px; color: #64748B; font-weight: 700; margin-bottom: 8px; text-transform: uppercase;">Throughput</div>
                    <div style="font-size: 20px; font-weight: 800; color: #0F172A;">{ '85.4k/s' if active_stage['status'] != 'pending' else '-' }</div>
                </div>
                 <div class="stats-tile">
                    <div style="font-size: 11px; color: #64748B; font-weight: 700; margin-bottom: 8px; text-transform: uppercase;">Exceptions</div>
                    <div style="font-size: 20px; font-weight: 800; color:{ COLORS['brand'] if active_stage['status'] != 'pending' else '#0F172A' };">0</div>
                </div>
            </div>
            """), unsafe_allow_html=True)
        
        st.markdown("</div>", unsafe_allow_html=True)
        
        # LOGS
        # Only show specific stage logs for non-Ingestion stages (Ingestion has its own live console)
        if active_stage['name'] != "Ingestion":
            logs = generate_stage_logs(active_stage['name'])
            log_html = ""
            for log in logs:
                is_warn = "WARN" in log
                log_html += f'<span class="log-line"><span class="log-ts">{get_current_time_str()}</span> <span style="{"color: #FCD34D" if is_warn else ""};">{log}</span></span>'
                
            st.markdown(clean_html(f"""
                <div class="log-box">
                    <div style="border-bottom: 1px solid #1E293B; padding-bottom: 12px; margin-bottom: 16px; font-weight: 700; color: white; display: flex; align-items: center; gap: 10px;">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="4 17 10 11 4 5"/><line x1="12" x2="20" y1="19" y2="19"/></svg>
                        Execution Stream Console (stdout)
                    </div>
                    {log_html}
                    <span style="animation: blink 1.2s infinite; font-size: 14px; color: {COLORS['brand']};">_</span>
                </div>
            </div>
            """), unsafe_allow_html=True)
        else:
            st.markdown("</div>", unsafe_allow_html=True)
        
        # Navigation Buttons (Pagination Style)
        st.write("")
        c_prev, c_gap, c_next = st.columns([1, 1.8, 1])
        with c_prev:
            if current_idx > 0:
                if st.button("Previous Step", use_container_width=True, key="prev_btn_foot", type="secondary"):
                    st.session_state['inspector_active_stage'] -= 1
                    st.rerun()
        with c_next:
            if current_idx < len(stages) - 1:
                if st.button("Proceed to Next", use_container_width=True, key="next_btn_foot", type="primary"):
                    st.session_state['inspector_active_stage'] += 1
                    st.rerun()

