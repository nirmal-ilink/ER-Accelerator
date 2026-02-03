import streamlit as st
import re
import random
import datetime
import textwrap
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
            f"[INFO] {get_current_time_str()} - Connecting to Oracle DB source...",
            f"[INFO] {get_current_time_str()} - Fetching batch 2041 (1.2GB)...",
            f"[INFO] {get_current_time_str()} - Validating schema compatibility...",
            f"[INFO] {get_current_time_str()} - Writing raw data to S3 landing zone..."
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
            f"[INFO] {get_current_time_str()} - Formatting output for Snowflake...",
            f"[INFO] {get_current_time_str()} - Triggering downstream webhook...",
            f"[INFO] {get_current_time_str()} - Compliance check: GDPR...",
            f"[INFO] {get_current_time_str()} - Sync completed. Duration: 4ms."
        ]
    }
    
    return specific_logs.get(stage_name, common_logs)

def render():
    if 'inspector_active_stage' not in st.session_state:
        st.session_state['inspector_active_stage'] = 3 # Default to "Resolution"
        
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

    stages = [
        {"id": 0, "name": "Ingestion", "pct": 100, "status": "done", "icon": "ingest", "meta": "SUCCESS", "desc": "Oracle, SAP, CSV loaded"},
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

        /* --- FORM ELEMENTS (Force Light Theme) --- */
        div[data-baseweb="select"] > div {{
            background-color: #FFFFFF !important;
            color: #0F172A !important;
            border-color: #E2E8F0 !important;
        }}
        div[data-baseweb="popover"] div[data-baseweb="menu"] {{
            background-color: #FFFFFF !important;
        }}
        div[data-testid="stSelectbox"] label p {{
            font-weight: 600 !important;
            color: #334155 !important;
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
            
            # ================================================================
            # STEP 1: SOURCE CONFIGURATION SUMMARY
            # ================================================================
            # Load saved configuration first
            # Load saved configuration (with Caching)
            try:
                # Check cache first to avoid slow backend calls
                loaded_config = st.session_state.get("ingestion_connector_config")
                is_cached = st.session_state.get("ingestion_config_cached", False)
                
                if not is_cached:
                    service = get_connector_service()
                    
                    # If the service is stale (from before code reload), reset it
                    if not hasattr(service, 'get_latest_configuration'):
                        reset_connector_service()
                        service = get_connector_service()
                    
                    # Get the MOST RECENTLY saved configuration across all connector types
                    loaded_config = service.get_latest_configuration()
                    
                    # Store in cache
                    st.session_state["ingestion_connector_config"] = loaded_config
                    st.session_state["ingestion_config_cached"] = True
                
                active_connector_type = loaded_config.connector_type if loaded_config else None
                
                if loaded_config and loaded_config.selected_tables:
                    total_schemas = len(loaded_config.selected_tables)
                    total_tables = sum(len(t) for t in loaded_config.selected_tables.values())
                    
                    # Show source summary card - Premium Design
                    connector_display_name = loaded_config.connector_name or active_connector_type.upper()
                    is_databricks = active_connector_type == "databricks"
                    
                    st.markdown(f"""
                    <div class="source-card">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <div style="display: flex; align-items: center; gap: 16px;">
                                <div style="background: linear-gradient(135deg, {COLORS['success']}15 0%, {COLORS['success']}08 100%); width: 52px; height: 52px; border-radius: 14px; display: flex; align-items: center; justify-content: center; border: 1px solid {COLORS['success']}20;">
                                    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="{COLORS['success']}" stroke-width="2">
                                        <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/>
                                    </svg>
                                </div>
                                <div>
                                    <div style="font-size: 11px; font-weight: 600; color: {COLORS['success']}; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 2px;">Source Active</div>
                                    <div style="font-weight: 700; color: #0F172A; font-size: 18px; letter-spacing: -0.3px;">{connector_display_name}</div>
                                </div>
                            </div>
                            <div style="display: flex; gap: 24px;">
                                <div style="text-align: center; px-4 py-3 bg-slate-50 rounded-xl">
                                    <div style="font-size: 24px; font-weight: 800; color: #0F172A; line-height: 1;">{total_schemas}</div>
                                    <div style="font-size: 11px; color: #64748B; text-transform: uppercase; margin-top: 4px; font-weight: 600;">Schemas</div>
                                </div>
                                <div style="text-align: center; px-4 py-3 bg-slate-50 rounded-xl">
                                    <div style="font-size: 24px; font-weight: 800; color: #0F172A; line-height: 1;">{total_tables}</div>
                                    <div style="font-size: 11px; color: #64748B; text-transform: uppercase; margin-top: 4px; font-weight: 600;">Tables</div>
                                </div>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
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
                                <div style="color: #64748B; font-size: 13px; margin-top: 2px;">Configure a data connector in the Connectors page to begin.</div>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    st.session_state["ingestion_connector_config"] = None
            except Exception as e:
                st.warning(f"Unable to load source configuration: {e}")
                st.session_state["ingestion_connector_config"] = None
            
            # ================================================================
            # STEP 2: LOAD TYPE SELECTION - Premium Radio Card Design
            # ================================================================
            # ================================================================
            # STEP 2: LOAD TYPE SELECTION - Intuitive Radio Cards
            # ================================================================
            # Header is already present in previous block if not replaced, preventing duplication
            
            st.markdown(f"""
            <style>
                /* Radio Card Styling */
                div[role="radiogroup"][aria-label="Load Strategy"] {{
                    display: flex;
                    gap: 16px;
                    width: 100%;
                }}
                @media (max-width: 640px) {{
                    div[role="radiogroup"][aria-label="Load Strategy"] {{
                        flex-direction: column;
                    }}
                }}
                div[role="radiogroup"][aria-label="Load Strategy"] > label {{
                    flex: 1;
                    background: white;
                    border: 1px solid #E2E8F0;
                    border-radius: 12px;
                    padding: 20px;
                    transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
                    cursor: pointer;
                    display: flex;
                    flex-direction: column;
                    align-items: flex-start;
                    gap: 8px;
                    box-shadow: 0 1px 2px rgba(0,0,0,0.03);
                }}
                div[role="radiogroup"][aria-label="Load Strategy"] > label:hover {{
                    border-color: {COLORS['brand']}60;
                    background: #FEF2F2; /* Very light brand tint */
                    transform: translateY(-1px);
                    box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05);
                }}
                /* Active State - Elegant */
                div[role="radiogroup"][aria-label="Load Strategy"] > label:has(input:checked) {{
                    border-color: {COLORS['brand']};
                    background: #FFFFFF; /* Keep white for cleanliness */
                    box-shadow: 0 0 0 1px {COLORS['brand']}, 0 4px 12px -2px {COLORS['brand']}25; /* Glow effect */
                }}
                /* Hide default radio circle ONLY for Load Strategy */
                div[role="radiogroup"][aria-label="Load Strategy"] input[type="radio"] {{
                    accent-color: {COLORS['brand']};
                    display: none;
                }}
                
                /* Custom Indicator for Active State */
                 div[role="radiogroup"][aria-label="Load Strategy"] > label:has(input:checked)::before {{
                    content: '✓';
                    position: absolute;
                    top: 16px;
                    right: 16px;
                    width: 20px;
                    height: 20px;
                    background: {COLORS['brand']};
                    color: white;
                    border-radius: 50%;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    font-size: 12px;
                    font-weight: 800;
                    box-shadow: 0 2px 4px {COLORS['brand']}40;
                }}
                
                /* Typography in Radio */
                div[role="radiogroup"][aria-label="Load Strategy"] p {{
                    font-size: 13px;
                    line-height: 1.5;
                    color: #64748B; /* Check caption color */
                    margin: 0;
                }}
                /* Title Styling */
                div[role="radiogroup"][aria-label="Load Strategy"] p strong {{
                    font-size: 16px;
                    color: #1E293B;
                    font-weight: 700;
                    display: block;
                    margin-bottom: 4px;
                }}
                div[role="radiogroup"][aria-label="Load Strategy"] > label:has(input:checked) p strong {{
                    color: {COLORS['brand']};
                }}
            </style>
            """, unsafe_allow_html=True)
            
            # Radio button that acts as the single selector mechanism
            load_choice = st.radio(
                "Load Strategy",
                options=["Full Load", "Incremental"],
                format_func=lambda x: f"**{x}**" if x == "Full Load" else f"**{x}**",
                captions=[
                    "Replaces all existing data with fresh source data. Best for full synchronization.",
                    "Appends only new and modified records. Optimized for performance at scale."
                ],
                key="ingestion_load_type_radio",
                horizontal=True,
                label_visibility="collapsed"
            )
            
            # Map radio selection to backend state
            new_load_type = "full" if "Full" in load_choice else "incremental"
            if st.session_state.get("ingestion_load_type") != new_load_type:
                st.session_state["ingestion_load_type"] = new_load_type
                if new_load_type == "full":
                    st.session_state.pop("ingestion_watermark_options", None)
                st.rerun()

            st.markdown("<div style='height:24px;'></div>", unsafe_allow_html=True)
            
            st.markdown("<div style='height:24px;'></div>", unsafe_allow_html=True)
            
            # ================================================================
            # STEP 3: WATERMARK CONFIGURATION (Incremental Only)
            # ================================================================
            if st.session_state.get("ingestion_load_type") == "incremental":
                st.markdown("""
                <div style="font-size: 12px; font-weight: 700; color: #64748B; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 12px;">
                    Watermark Configuration
                </div>
                """, unsafe_allow_html=True)
                
                config = st.session_state.get("ingestion_connector_config")
                is_databricks = st.session_state.get("ingestion_is_databricks", False)
                conn_type = st.session_state.get("ingestion_connector_type", "sqlserver")
                
                if config and config.selected_tables:
                    # Build schema -> tables mapping
                    schemas = list(config.selected_tables.keys())
                    
                    # Modern card wrapper
                    st.markdown("""
                    <div style="background: linear-gradient(135deg, #FAFAFA 0%, #F5F5F5 100%); border: 1px solid #E5E7EB; border-radius: 16px; padding: 24px; margin-bottom: 20px;">
                    """, unsafe_allow_html=True)
                    
                    # Catalog selection for Databricks
                    if is_databricks:
                        st.markdown(f"""
                        <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 20px;">
                            <div style="background: linear-gradient(135deg, #7C3AED 0%, #6D28D9 100%); width: 42px; height: 42px; border-radius: 12px; display: flex; align-items: center; justify-content: center; box-shadow: 0 4px 12px rgba(124, 58, 237, 0.25);">
                                <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2">
                                    <ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>
                                </svg>
                            </div>
                            <div>
                                <div style="font-weight: 700; color: #1F2937; font-size: 16px;">Databricks Unity Catalog</div>
                                <div style="color: #6B7280; font-size: 13px;">Select catalog, schema, and table for change tracking</div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Fetch catalogs if not cached
                        if "ingestion_catalogs" not in st.session_state:
                            try:
                                from src.backend.connectors import get_connector_service
                                service = get_connector_service()
                                catalogs = service.fetch_catalogs(conn_type, config.config)
                                st.session_state["ingestion_catalogs"] = catalogs if catalogs else ["default"]
                            except:
                                st.session_state["ingestion_catalogs"] = ["default"]
                        
                        catalogs = st.session_state.get("ingestion_catalogs", ["default"])
                        
                        cat_col, schema_col, table_col = st.columns(3)
                        
                        with cat_col:
                            selected_catalog = st.selectbox(
                                "Catalog",
                                options=catalogs,
                                key="ingestion_selected_catalog",
                                help="Databricks Unity Catalog"
                            )
                        
                        with schema_col:
                            selected_schema = st.selectbox(
                                "Schema",
                                options=schemas,
                                key="ingestion_selected_schema",
                                help="Database schema"
                            )
                        
                        with table_col:
                            tables_in_schema = config.selected_tables.get(selected_schema, [])
                            selected_table = st.selectbox(
                                "Table",
                                options=tables_in_schema if tables_in_schema else ["No tables"],
                                key="ingestion_selected_table",
                                help="Table to track changes"
                            )
                    else:
                        # Non-Databricks: Schema and Table selectors
                        st.markdown(f"""
                        <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 20px;">
                            <div style="background: linear-gradient(135deg, #0369A1 0%, #0284C7 100%); width: 42px; height: 42px; border-radius: 12px; display: flex; align-items: center; justify-content: center; box-shadow: 0 4px 12px rgba(3, 105, 161, 0.25);">
                                <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2">
                                    <ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>
                                </svg>
                            </div>
                            <div>
                                <div style="font-weight: 700; color: #1F2937; font-size: 16px;">SQL Server Source</div>
                                <div style="color: #6B7280; font-size: 13px;">Select schema and table for change tracking</div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        schema_col, table_col = st.columns(2)
                        
                        with schema_col:
                            selected_schema = st.selectbox(
                                "Schema",
                                options=schemas,
                                key="ingestion_selected_schema",
                                help="Database schema"
                            )
                        
                        with table_col:
                            tables_in_schema = config.selected_tables.get(selected_schema, [])
                            selected_table = st.selectbox(
                                "Table",
                                options=tables_in_schema if tables_in_schema else ["No tables"],
                                key="ingestion_selected_table",
                                help="Table to track changes"
                            )
                    
                    st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)
                    
                    # Divider
                    st.markdown("""
                    <div style="height: 1px; background: linear-gradient(90deg, transparent, #E5E7EB, transparent); margin: 8px 0 20px 0;"></div>
                    """, unsafe_allow_html=True)
                    
                    # Watermark column selection with detect button
                    st.markdown("""
                    <div style="font-size: 13px; font-weight: 600; color: #374151; margin-bottom: 12px;">
                        Watermark Column Selection
                    </div>
                    """, unsafe_allow_html=True)
                    
                    detect_col, wm_col = st.columns([1, 2])
                    
                    with detect_col:
                        if st.button("Auto-Detect", key="detect_wm_columns", use_container_width=True, type="secondary"):
                            selected_schema = st.session_state.get("ingestion_selected_schema")
                            selected_table = st.session_state.get("ingestion_selected_table")
                            
                            if selected_schema and selected_table and selected_table != "No tables":
                                try:
                                    # Ensure service is instantiated (in case of cached load)
                                    from src.backend.connectors import get_connector_service
                                    service = get_connector_service()
                                    
                                    with st.spinner(f"Analyzing {selected_table}..."):
                                        cols = service.fetch_columns(conn_type, config.config, selected_schema, selected_table)
                                        
                                        # Filter and format columns
                                        wm_columns = []
                                        for c in cols:
                                            cname = c['name']
                                            ctype = c['type'].lower()
                                            # Accept time/date or numeric fields
                                            if any(x in ctype for x in ['time', 'date', 'int', 'numeric', 'long', 'decimal', 'bigint']):
                                                wm_columns.append(f"{cname} ({c['type']})")
                                        
                                        if wm_columns:
                                            st.session_state["ingestion_watermark_options"] = wm_columns
                                            # Smart default selection
                                            default_idx = 0
                                            for i, opt in enumerate(wm_columns):
                                                if any(k in opt.lower() for k in ['updat', 'modif', 'last', 'timestamp', 'changed']):
                                                    default_idx = i
                                                    break
                                            st.session_state["ingestion_watermark_default_index"] = default_idx
                                            st.success(f"Found {len(wm_columns)} trackable columns")
                                        else:
                                            st.warning("No timestamp/date columns found")
                                            st.session_state["ingestion_watermark_options"] = ["updated_at", "modified_at", "created_at"]
                                except Exception as e:
                                    st.error(f"Detection failed: {str(e)[:80]}")
                                    st.session_state["ingestion_watermark_options"] = ["updated_at", "modified_at", "created_at"]
                    
                    with wm_col:
                        wm_options = st.session_state.get("ingestion_watermark_options", ["updated_at", "modified_at", "created_at", "timestamp"])
                        wm_idx = st.session_state.get("ingestion_watermark_default_index", 0)
                        
                        st.selectbox(
                            "Watermark Column",
                            options=wm_options,
                            index=wm_idx if wm_idx < len(wm_options) else 0,
                            key="ingestion_watermark_column",
                            help="Column used to identify new/updated records for incremental sync"
                        )
                    
                    # Tip box
                    st.markdown("""
                    <div style="background: #ECFDF5; border: 1px solid #A7F3D0; border-radius: 10px; padding: 12px 16px; margin-top: 16px;">
                        <div style="display: flex; align-items: flex-start; gap: 10px;">
                            <div style="background: #D1FAE5; width: 24px; height: 24px; border-radius: 6px; display: flex; align-items: center; justify-content: center;">
                                <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#059669" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" x2="12" y1="16" y2="12"/><line x1="12" x2="12.01" y1="8" y2="8"/></svg>
                            </div>
                            <div style="color: #047857; font-size: 13px; line-height: 1.5;">
                                <strong>Tip:</strong> Choose a column that reliably updates when records change. Common choices are <code style="background:#D1FAE5; padding:2px 6px; border-radius:4px;">updated_at</code>, <code style="background:#D1FAE5; padding:2px 6px; border-radius:4px;">modified_date</code>, or an auto-incrementing ID.
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Close card wrapper
                    st.markdown("</div>", unsafe_allow_html=True)
                    
                else:
                    st.markdown("""
                    <div style="background: #FEF3C7; border: 1px solid #FCD34D; border-radius: 12px; padding: 20px; text-align: center;">
                        <div style="background: #FDE68A; width: 40px; height: 40px; border-radius: 10px; display: flex; align-items: center; justify-content: center; margin: 0 auto 12px;">
                            <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#92400E" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" x2="8" y1="13" y2="13"/><line x1="16" x2="8" y1="17" y2="17"/><polyline points="10 9 9 9 8 9"/></svg>
                        </div>
                        <div style="font-weight: 600; color: #92400E; font-size: 14px;">No Source Configured</div>
                        <div style="color: #B45309; font-size: 13px; margin-top: 4px;">Configure a data connector first to enable watermark detection.</div>
                    </div>
                    """, unsafe_allow_html=True)
            
            st.markdown("<div style='height:16px;'></div>", unsafe_allow_html=True)
            
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
            with btn_col:
                if st.button("Start Ingestion", type="primary", use_container_width=True, key="start_ingestion_btn"):
                    st.toast("Ingestion job queued!")
                    st.session_state['inspector_active_stage'] = 1  # Move to Profiling
                    st.rerun()
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
            
            quality_metrics = [
                ("Completeness", "98.2%", "#10B981", "Non-null values"),
                ("Uniqueness", "94.7%", "#0369A1", "Distinct records"),
                ("Validity", "99.1%", "#7C3AED", "Format compliance"),
                ("Consistency", "96.8%", "#F59E0B", "Cross-field accuracy")
            ]
            
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
                    st.toast("Profiling analysis complete!")
                    st.session_state['inspector_active_stage'] = 2
                    st.rerun()
                    
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

