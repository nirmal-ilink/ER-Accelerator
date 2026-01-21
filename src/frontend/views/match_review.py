import streamlit as st
import pandas as pd
import os
import time
import textwrap

# =============================================================================
# CONSTANTS & CONFIG
# =============================================================================
ILINK_RED = "#ED1C24"
ILINK_DARK = "#0F172A"
ILINK_SLATE = "#F8FAFC"
ILINK_BORDER = "#E2E8F0"

SOURCE_METADATA = {
    "EMR": {"name": "Clinical EMR"},
    "NPI_Registry": {"name": "NPI Registry"},
    "Claims_DB": {"name": "Claims Vault"},
    "Snowflake": {"name": "Snowflake DW"},
    "Delta Lake": {"name": "Delta Lakehouse"},
    "SAP": {"name": "SAP ERP"},
}

HEALTHCARE_FIELDS = [
    ("NPI", "npi"),
    ("First Name", "first_name"),
    ("Last Name", "last_name"),
    ("Address", "address_line_1"),
    ("City", "city"),
    ("State", "state"),
    ("Zip Code", "zip_code"),
    ("Phone", "phone"),
    ("Specialty", "specialty")
]

# =============================================================================
# DATA OPERATIONS
# =============================================================================
def load_data():
    """Load match review data from gold export or use healthcare mock data."""
    path = os.path.join(os.path.dirname(__file__), "../../../data/output/gold_export.csv")
    try:
        if os.path.exists(path):
            return pd.read_csv(path)
    except Exception:
        pass
        
    return pd.DataFrame({
        "unique_id": ["EMR001", "NPI001", "EMR002", "NPI002", "CLM002", "EMR003", "SNOW_01", "DL_01", "SAP_01"],
        "npi": ["3444587960", "3444587960", "3175087427", "3175087427", "3175087427", "4125896321", "4125896321", "4125896321", "4125890000"],
        "first_name": ["Jennifer", "Jennifer", "Sarah", "Sara", "Sarah", "Nirmal", "Nirmal", "Nirmal Kumar", "Nirmal"],
        "last_name": ["Martin", "Martin", "Wilson", "Wilson", "Willson", "Pukazhen", "Pukazhen", "Pukazhen", "Pukazhen"],
        "address_line_1": ["4484 Cedar Ct", "4484 Cedar Ct", "6940 Main Street", "6940 Main St", "6940 Main St", "123 iLink Way", "123 iLink Way", "123 iLink Way", "123 iLink Way"],
        "city": ["Los Angeles", "Los Angeles", "San Antonio", "San Antonio", "San Antonio", "Houston", "Houston", "Houston", "Houston"],
        "state": ["CA", "CA", "TX", "TX", "TX", "TX", "TX", "TX", "TX"],
        "zip_code": ["25354", "25354", "74966", "74966", "74966", "77001", "77001", "77001", "77001"],
        "phone": ["449-632-7536", "449-632-7536", "848-211-7653", "848-211-7653", None, "713-555-0101", "713-555-0101", "713-555-0101", "713-555-0000"],
        "specialty": ["Oncology", "Oncology", "Internal Medicine", "Internal Medicine", "Internal Med", "General Practice", "GP", "General Practice", "GP"],
        "_source_system": ["EMR", "NPI_Registry", "EMR", "NPI_Registry", "Claims_DB", "EMR", "Snowflake", "Delta Lake", "SAP"],
        "cluster_id": ["CL001", "CL001", "CL002", "CL002", "CL002", "CL003", "CL003", "CL003", "CL003"],
        "confidence_score": [0.95, 0.95, 0.87, 0.87, 0.87, 0.98, 0.70, 0.88, 0.90]
    })

def detect_discrepancies(records: pd.DataFrame, field_key: str) -> bool:
    values = []
    for _, rec in records.iterrows():
        val = rec.get(field_key)
        if pd.notna(val) and str(val).strip():
            values.append(str(val).lower().strip())
    if not values: return False
    return len(set(values)) > 1

def get_match_grade(score: int):
    if score >= 90:
        return {"label": "Auto-Match", "color": "#0369a1", "bg": "#f0f9ff", "border": "#bae6fd"}
    elif score >= 70:
        return {"label": "Review Requested", "color": "#b45309", "bg": "#fffbeb", "border": "#fef3c7"}
    else:
        return {"label": "Conflict Detected", "color": "#be123c", "bg": "#fff1f2", "border": "#ffe4e6"}

# =============================================================================
# CSS STYLING
# =============================================================================
def inject_premium_styles():
    css = f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap');
        
        .stApp {{
            font-family: 'Outfit', sans-serif !important;
            background-color: {ILINK_SLATE};
        }}
        
        /* Global Reset */
        .stMarkdownCode, .stCodeBlock {{ display: none !important; }}
        .block-container {{ 
            padding-top: 1.5rem !important; 
            padding-bottom: 2rem !important; 
            max-width: 1240px !important; 
        }}
        div[data-testid="stDecoration"] {{ display: none; }}
        
        /* PREMIUM HEADER - GLASSMORPHISM */
        .header-box {{
            background: linear-gradient(135deg, rgba(255, 255, 255, 0.9), rgba(255, 255, 255, 0.7));
            backdrop-filter: blur(8px);
            padding: 1.25rem 2rem;
            border-radius: 12px;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 10px 15px -3px rgba(0, 0, 0, 0.03);
            border: 1px solid rgba(255, 255, 255, 0.8);
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 2rem;
            position: relative;
            overflow: hidden;
        }}
        .header-box::before {{
            content: '';
            position: absolute;
            left: 0;
            top: 0;
            bottom: 0;
            width: 4px;
            background: {ILINK_RED};
            box-shadow: 2px 0 8px {ILINK_RED}33;
        }}
        .header-title-group {{ display: flex; align-items: center; gap: 1.5rem; }}
        .header-title {{ 
            font-size: 1.5rem; 
            font-weight: 700; 
            color: {ILINK_DARK}; 
            margin: 0;
            letter-spacing: -0.02em;
        }}
        .header-sub {{ 
            color: #64748b; 
            font-size: 0.85rem; 
            font-weight: 600;
            letter-spacing: 0.02em;
            text-transform: uppercase;
        }}
        .progress-indicator {{
            text-align: right;
            padding-left: 1.5rem;
            border-left: 1px solid {ILINK_BORDER};
        }}
        .current-count {{ color: #475569; font-weight: 800; font-size: 1.25rem; }}
        
        /* PREMIUM MATCH CARDS */
        .match-card {{
            background: white;
            border-radius: 12px;
            border: 1px solid {ILINK_BORDER};
            box-shadow: 0 1px 2px rgba(0,0,0,0.05);
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            overflow: hidden;
            display: flex;
            flex-direction: column;
            height: 100%;
        }}
        .match-card:hover {{
            box-shadow: 0 12px 24px -8px rgba(0,0,0,0.12);
            border-color: {ILINK_RED}33;
            transform: translateY(-2px);
        }}
        .card-header {{
            padding: 1rem 1.25rem;
            border-bottom: 1px solid #f1f5f9;
            background: #ffffff;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .card-title-group {{ display: flex; flex-direction: column; }}
        .card-title {{
            font-size: 1.1rem;
            font-weight: 700;
            color: {ILINK_DARK};
            margin: 0;
        }}
        .card-id {{
            font-size: 0.7rem;
            color: #94a3b8;
            font-weight: 500;
            margin-top: 2px;
        }}
        .card-body {{ 
            padding: 0.5rem 0; 
            background: #ffffff;
        }}
        
        /* INTERACTIVE DATA ROWS */
        .data-row {{
            display: grid;
            grid-template-columns: 120px 1fr;
            padding: 0.625rem 1.25rem;
            border-bottom: 1px solid #f8fafc;
            align-items: center;
            transition: background 0.2s ease;
        }}
        .data-row:last-child {{ border-bottom: none; }}
        .data-row:hover {{ background-color: {ILINK_SLATE}; }}
        
        .data-label {{
            font-size: 0.7rem;
            text-transform: uppercase;
            color: #94a3b8;
            font-weight: 600;
            letter-spacing: 0.06em;
        }}
        .data-value {{
            font-size: 0.95rem;
            color: {ILINK_DARK};
            font-weight: 500;
            text-align: right;
        }}
        .data-value.conflict {{
            color: white;
            background: {ILINK_RED};
            padding: 2px 8px;
            border-radius: 4px;
            font-weight: 600;
            box-shadow: 0 2px 8px {ILINK_RED}44;
        }}
        
        /* BRANDED BADGES */
        .badge {{
            font-size: 0.65rem;
            font-weight: 700;
            padding: 4px 10px;
            border-radius: 6px;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}
        .badge-emr {{ background: #fef2f2; color: {ILINK_RED}; border: 1px solid #fee2e2; }}
        .badge-npi {{ background: #f0f9ff; color: #0369a1; border: 1px solid #bae6fd; }}
        .badge-other {{ background: #f1f5f9; color: #475569; border: 1px solid #e2e8f0; }}

        /* PREMIUM CONFIDENCE BOX */
        .score-box {{
            background: white;
            border-radius: 12px;
            padding: 1.5rem;
            text-align: center;
            border: 1px solid {ILINK_BORDER};
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
            height: 100%;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
        }}
        .score-circle {{
            width: 100px;
            height: 100px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 2rem;
            font-weight: 800;
            margin-bottom: 1rem;
            position: relative;
            background: conic-gradient(var(--score-color, {ILINK_RED}) var(--percentage), #f1f5f9 0);
            box-shadow: 0 4px 15px rgba(0,0,0,0.05);
        }}
        .score-circle::after {{
            content: '';
            position: absolute;
            width: 84px;
            height: 84px;
            background: white;
            border-radius: 50%;
        }}
        .score-inner {{
            position: relative;
            z-index: 1;
            color: {ILINK_DARK};
        }}
        .status-value {{
            font-weight: 700;
            font-size: 0.85rem;
            padding: 4px 12px;
            border-radius: 6px;
            letter-spacing: 0.01em;
        }}
        
        /* MATRIX COMPONENT */
        .matrix-container {{
            background: white;
            border-radius: 12px;
            border: 1px solid {ILINK_BORDER};
            box-shadow: 0 4px 20px -5px rgba(0,0,0,0.05);
            overflow: hidden;
            margin-bottom: 2rem;
        }}
        .matrix-header {{
            padding: 1rem 1.5rem;
            border-bottom: 1px solid #f1f5f9;
            background: white;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .matrix-table {{
            width: 100%;
            border-collapse: collapse;
        }}
        .matrix-table th {{
            text-align: left;
            padding: 0.75rem 1.5rem;
            background: {ILINK_SLATE};
            border-bottom: 1px solid {ILINK_BORDER};
            font-size: 0.75rem;
            text-transform: uppercase;
            color: #64748b;
            font-weight: 700;
        }}
        .matrix-table td {{
            padding: 0.75rem 1.5rem;
            border-bottom: 1px solid #f8fafc;
            color: {ILINK_DARK};
            font-size: 0.9rem;
            transition: all 0.2s ease;
        }}
        .matrix-table tr:hover td {{
            background-color: {ILINK_SLATE};
        }}
        .row-conflict td {{ background-color: #fff9f9; }}
        .matrix-value-conflict {{
            color: {ILINK_RED};
            font-weight: 600;
        }}

        /* PREMIUM COMMAND BUTTONS - ROBUST TARGETING */
        div.stButton > button {{
            border-radius: 10px;
            padding: 0.75rem 2.5rem;
            font-weight: 600;
            font-size: 0.9rem;
            border: 1px solid {ILINK_BORDER};
            background-color: white !important;
            color: {ILINK_DARK} !important;
            transition: all 0.25s ease;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -1px rgba(0, 0, 0, 0.03);
            text-transform: none;
            letter-spacing: 0.01em;
            width: 100%;
        }}
        
        div.stButton > button:hover {{
            transform: translateY(-2px);
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.08);
            border-color: #cbd5e1 !important;
        }}

        /* Targeting via markers (simplified) */
        div:has(.btn-marker-approve) + div button:hover,
        div:has(.btn-marker-approve) + div button:active {{
            background-color: #D1FAE5 !important; /* Visible Mild Green */
            color: #065F46 !important;
            border-color: #A7F3D0 !important;
        }}

        div:has(.btn-marker-reject) + div button:hover,
        div:has(.btn-marker-reject) + div button:active {{
            background-color: #FEE2E2 !important; /* Visible Mild Red */
            color: #991B1B !important;
            border-color: #FECACA !important;
        }}

        div:has(.btn-marker-defer) + div button:hover,
        div:has(.btn-marker-defer) + div button:active {{
            background-color: #F1F5F9 !important; /* Visible Mild Grey */
            color: #334155 !important;
            border-color: #E2E8F0 !important;
        }}
        
        div:has(.btn-marker-restart) + div button:hover,
        div:has(.btn-marker-restart) + div button:active {{
            background-color: #F8FAFC !important;
            color: {ILINK_DARK} !important;
            border-color: #cbd5e1 !important;
        }}

        /* SOURCE SELECTOR TABS */
        .source-selector-container {{
            background: rgba(255, 255, 255, 0.5);
            padding: 8px;
            border-radius: 12px;
            border: 1px solid {ILINK_BORDER};
            margin-bottom: 1.5rem;
            display: flex;
            gap: 12px;
            align-items: center;
        }}
        .selector-label {{
            font-size: 0.75rem;
            font-weight: 700;
            color: #64748b;
            text-transform: uppercase;
            padding-left: 8px;
            letter-spacing: 0.05em;
        }}
        
        /* Streamlit Pill Overrides for Premium Look */
        div[data-testid="stRadio"] > label {{ display: none !important; }}
        
        div[data-testid="stHorizontalBlock"] div.stRadio {{ 
            background: transparent !important; 
        }}
        
        /* Modern Segmented Control look for radio */
        div[data-testid="stHorizontalBlock"] div.stRadio > div[role="radiogroup"] {{
            background: #f1f5f9;
            padding: 4px;
            border-radius: 10px;
            border: 1px solid {ILINK_BORDER};
            gap: 8px !important;
            display: flex;
            flex-direction: row;
        }}
        
        div[data-testid="stHorizontalBlock"] div.stRadio label[data-baseweb="radio"] {{
            background: transparent;
            border: 1px solid transparent;
            padding: 6px 16px !important;
            border-radius: 8px !important;
            transition: all 0.2s ease !important;
            margin: 0 !important;
            cursor: pointer;
        }}
        
        /* THE KEY FIX: Ensure the text is VISIBLE */
        div.stRadio label [data-testid="stMarkdownContainer"] {{
            opacity: 1 !important;
            visibility: visible !important;
            display: block !important;
        }}
        
        div.stRadio label [data-testid="stMarkdownContainer"] p {{
            font-size: 0.82rem !important;
            font-weight: 600 !important;
            color: #475569 !important; /* Darker grey for better contrast */
            margin: 0 !important;
        }}

        div[data-testid="stHorizontalBlock"] div.stRadio label:hover {{
            background: rgba(255,255,255,0.8) !important;
        }}

        div[data-testid="stHorizontalBlock"] div.stRadio label:has(input:checked) {{
            background: white !important;
            box-shadow: 0 2px 6px rgba(0,0,0,0.08) !important;
            border-color: {ILINK_BORDER} !important;
        }}
        
        div[data-testid="stHorizontalBlock"] div.stRadio label:has(input:checked) [data-testid="stMarkdownContainer"] p {{
            color: {ILINK_RED} !important;
        }}
        
        /* Hide the actual radio circle indicator */
        div[data-testid="stHorizontalBlock"] div.stRadio label[data-baseweb="radio"] > div:first-child {{
            display: none !important;
        }}

        /* PROGRESS BAR COLOR OVERRIDE */
        div[data-testid="stProgress"] > div > div > div > div {{
            background-color: {ILINK_RED} !important;
        }}
    </style>
    """
    st.markdown(clean_html(css), unsafe_allow_html=True)

# =============================================================================
# UI HELPERS
# =============================================================================
def clean_html(html_str: str) -> str:
    """Removes leading whitespace from each line to prevent Markdown code-block interpretation."""
    return "\n".join([line.strip() for line in html_str.split("\n")])

def render_safe_html(html_str: str):
    """Clean and render HTML safely."""
    st.markdown(clean_html(html_str), unsafe_allow_html=True)

# =============================================================================
# UI RENDERERS
# =============================================================================

def render_header(current, total, cluster_id):
    pct = (current / total)
    html = f"""
        <div class="header-box">
            <div class="header-title-group">
                <h1 class="header-title">Match Review Center</h1>
                <div class="header-sub" style="opacity:0.6;">|</div>
                <div class="header-sub">CLUSTER: <span style="color:#475569; font-weight:700;">{cluster_id}</span></div>
            </div>
            <div class="progress-indicator">
                <div class="header-sub" style="margin-bottom:4px;">RESOLUTION STATUS</div>
                <div><span class="current-count">{current}</span> <span style="color:#cbd5e1; font-weight:300;">/</span> <span style="color:#64748b;">{total}</span></div>
            </div>
        </div>
    """
    render_safe_html(html)
    st.progress(pct)

def render_card(rec, comparison_rec=None):
    src = rec.get('_source_system', 'SRC')
    badge_cls = "badge-emr" if "EMR" in src else "badge-npi" if "NPI" in src else "badge-other"
    first = rec.get('first_name', '')
    last = rec.get('last_name', '')
    unique_id = rec.get('unique_id', 'Unknown')
    
    html = f"""
        <div class="match-card">
            <div class="card-header">
                <div class="card-title-group">
                    <div class="card-title">{first} {last}</div>
                    <div class="card-id">UID: {unique_id}</div>
                </div>
                <div class="badge {badge_cls}">{src}</div>
            </div>
            <div class="card-body">
    """
    
    for label, key in HEALTHCARE_FIELDS:
        val = str(rec.get(key, '—'))
        if val.lower() == 'nan' or not val.strip(): val = '—'
        
        is_conflict = False
        if comparison_rec is not None:
             other = str(comparison_rec.get(key, ''))
             if val.lower().strip() != other.lower().strip() and val != '—' and other != '—':
                 is_conflict = True
        
        cls_extra = "conflict" if is_conflict else ""
        
        html += f"""
                <div class="data-row">
                    <div class="data-label">{label}</div>
                    <div class="data-value {cls_extra}">{val}</div>
                </div>
        """
        
    html += """
            </div>
        </div>
    """
    render_safe_html(html)

def render_score(score):
    grade = get_match_grade(score)
    # Conditional color: Green (>80%) or Red (Brand Red)
    score_color = "#22c55e" if score > 80 else ILINK_RED
    
    html = f"""
        <div class="score-box">
            <div class="score-circle" style="--percentage:{score}%; --score-color:{score_color}">
                <div class="score-inner">{score}%</div>
            </div>
            <div class="status-label" style="margin-bottom:8px;">MATCH CONFIDENCE</div>
            <div class="status-value" style="background:{grade['bg']}; color:{grade['color']}; border:1px solid {grade['border']};">
                {grade['label'].upper()}
            </div>
        </div>
    """
    render_safe_html(html)

def render_matrix(records):
    sources = records['_source_system'].tolist()
    
    html = f"""
        <div class="matrix-container">
            <div class="matrix-header">
                <div style="font-size:1.1rem; font-weight:700; color:{ILINK_DARK};">Multilateral Comparison</div>
                <div class="header-sub" style="color:{ILINK_RED}; font-weight:700;">{len(sources)} DATA VECTORS</div>
            </div>
            <div style="overflow-x:auto;">
                <table class="matrix-table">
                    <thead>
                        <tr>
                            <th style="width:160px;">Attribute</th>
    """
    
    for src in sources:
        badge = "badge-emr" if "EMR" in src else "badge-npi" if "NPI" in src else "badge-other"
        html += f'<th><span class="badge {badge}">{src}</span></th>'
    
    html += "</tr></thead><tbody>"
    
    for label, key in HEALTHCARE_FIELDS:
        has_issue = detect_discrepancies(records, key)
        row_cls = "row-conflict" if has_issue else ""
        
        html += f'<tr class="{row_cls}">'
        html += f'<td style="font-weight:600; color:#64748b; font-size:0.75rem; letter-spacing:0.04em;">{label.upper()}</td>'
        
        for _, rec in records.iterrows():
             val = str(rec.get(key, '—'))
             if val.lower() == 'nan' or not val.strip(): val = '—'
             
             style_extras = 'class="matrix-value-conflict"' if has_issue else ''
             html += f'<td {style_extras}>{val}</td>'
             
        html += "</tr>"
        
    html += "</tbody></table></div></div>"
    render_safe_html(html)

# =============================================================================
# MAIN RENDER LOOP
# =============================================================================
def render():
    inject_premium_styles()
    
    if 'match_index' not in st.session_state:
        st.session_state.match_index = 0
        
    df = load_data()
    # Filter for clusters that have more than 1 record (actual matches to review)
    clusters = df.groupby('cluster_id').filter(lambda x: len(x) > 1)
    unique_ids = clusters['cluster_id'].unique()
    total = len(unique_ids)
    
    if st.session_state.match_index >= total:
        st.markdown(f"""
            <div style='text-align:center; padding:8rem 2rem;'>
                <div style='font-size:1.75rem; font-weight:700; color:{ILINK_DARK}; margin-bottom:1rem;'>Resolution Cycle Complete</div>
                <p style='color:#64748b; font-size:1rem; max-width:500px; margin:0 auto 2.5rem;'>All identified clusters have been reconciled. The golden record master has been updated with your resolutions.</p>
            </div>
        """, unsafe_allow_html=True)
        
        c1, c2, c3 = st.columns([1,1.5,1])
        with c2:
            st.markdown('<span class="btn-marker-restart"></span>', unsafe_allow_html=True)
            if st.button("Restart Review Session", use_container_width=True):
                st.session_state.match_index = 0
                st.rerun()
        return

    curr_id = unique_ids[st.session_state.match_index]
    records = clusters[clusters['cluster_id'] == curr_id]
    
    # Calculate Score
    score = int(float(records.iloc[0].get('confidence_score', 0.90)) * 100)
    
    # 1. Render Header
    render_header(st.session_state.match_index + 1, total, curr_id)
    st.write("") 
    
    # 2. Render Body
    if len(records) == 2:
        c1, c2, c3 = st.columns([1, 0.35, 1])
        with c1: render_card(records.iloc[0], records.iloc[1])
        with c2: render_score(score)
        with c3: render_card(records.iloc[1], records.iloc[0])
    else:
        # Multi-Source Focus mode
        ref_rec = records.iloc[0]
        other_recs = records.iloc[1:]
        
        # UI for Source Selector
        st.write('<div class="source-selector-container"><span class="selector-label">Compare Reference Against:</span>', unsafe_allow_html=True)
        
        # Build options with logos and vendor names
        source_options = []
        source_map = {}
        
        for _, r in other_recs.iterrows():
            sys_id = r['_source_system']
            meta = SOURCE_METADATA.get(sys_id, {"name": sys_id})
            opt_label = f"{meta['name']} ({int(r['confidence_score']*100)}%)"
            source_options.append(opt_label)
            source_map[opt_label] = r
            
        # Use a pill-like selector with unique key for this cluster
        selected_option = st.radio(
            "Select Source",
            options=source_options,
            horizontal=True,
            key=f"source_sel_v2_{curr_id}",
            label_visibility="collapsed"
        )
        st.write('</div>', unsafe_allow_html=True)
        
        # Find selected record
        target_rec = source_map[selected_option]
        target_score = int(target_rec['confidence_score'] * 100)
        
        c1, c2, c3 = st.columns([1, 0.35, 1])
        with c1: render_card(ref_rec, target_rec)
        with c2: render_score(target_score)
        with c3: render_card(target_rec, ref_rec)
        
        st.write("<br>", unsafe_allow_html=True)
        render_matrix(records)
        
    # 3. Actions - Professional Command Bar
    st.write("")
    st.write("")
    
    ac1, ac2, ac3, ac4, ac5 = st.columns([0.5, 1, 1, 1, 0.5])
    
    def advance():
        st.session_state.match_index += 1
        time.sleep(0.1)
        st.rerun()

    with ac2:
        st.markdown('<span class="btn-marker-approve"></span>', unsafe_allow_html=True)
        if st.button("Approve Match", use_container_width=True):
            st.toast("Match Resolved: Approved")
            advance()
    with ac3:
        st.markdown('<span class="btn-marker-reject"></span>', unsafe_allow_html=True)
        if st.button("Reject Match", use_container_width=True):
            st.toast("Match Resolved: Rejected")
            advance()
    with ac4:
        st.markdown('<span class="btn-marker-defer"></span>', unsafe_allow_html=True)
        if st.button("Defer Resolution", use_container_width=True):
            advance()
    
    st.write("<br><br>", unsafe_allow_html=True)
