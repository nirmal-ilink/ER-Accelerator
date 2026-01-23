import streamlit as st
import re
import random
import datetime
import textwrap

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

        /* --- PREMIUM BUTTON DESIGN --- */
        /* Secondary Buttons (Nav & Pagination) */
        div.stButton > button[kind="secondary"] {{
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
        div.stButton > button[kind="secondary"]:hover {{
            background-color: #FFFFFF !important;
            border-color: {COLORS['brand']}30 !important;
            color: {COLORS['brand']} !important;
            transform: translateY(-2px) !important;
            box-shadow: 0 10px 15px -3px rgba(0,0,0,0.05) !important;
        }}

        /* Primary Button (Active Nav) */
        div.stButton > button[kind="primary"] {{
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
        div.stButton > button[kind="primary"]::before {{
            content: '';
            position: absolute;
            left: 0;
            top: 25%;
            height: 50%;
            width: 4px;
            background: rgba(255,255,255,0.8);
            border-radius: 0 4px 4px 0;
        }}

        div.stButton > button[kind="primary"]:hover {{
            background-color: #B91C41 !important;
            border-color: #B91C41 !important;
            transform: translateY(-2px) !important;
            box-shadow: 0 12px 24px -6px rgba(209, 31, 65, 0.4) !important;
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
                if st.button("Previous Step", use_container_width=True, key="prev_btn_foot"):
                    st.session_state['inspector_active_stage'] -= 1
                    st.rerun()
        with c_next:
            if current_idx < len(stages) - 1:
                if st.button("Proceed to Next", use_container_width=True, key="next_btn_foot", type="primary"):
                    st.session_state['inspector_active_stage'] += 1
                    st.rerun()

