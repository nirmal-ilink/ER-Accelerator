import streamlit as st
import altair as alt
import pandas as pd
from datetime import datetime

# --- HARDCODED CURATED DATA ---
def get_enterprise_data():
    """Returns perfectly curated, realistic enterprise-scale data for live demonstrations."""
    # 1. Trend Data (Last 12 Months)
    # Using specific dates to ensure chart looks identical and perfect every time
    dates = pd.date_range(end=datetime.today(), periods=12, freq='ME')
    
    # A beautiful upward trending trust curve with a slight realistic dip and strong recovery
    trust_scores = [78.5, 79.2, 78.8, 81.5, 83.2, 82.9, 85.8, 88.4, 91.2, 92.8, 93.5, 96.4]
    trust_df = pd.DataFrame({'Date': dates, 'TrustScore': trust_scores})
    
    # Auto-Resolution vs Manual - clean divergence showing platform improvement over time
    auto_res = [65000, 72000, 76000, 84000, 91000, 98000, 105000, 112000, 119000, 128000, 136000, 142000]
    manual_res = [25000, 24000, 22000, 19000, 16000, 14000, 12000, 10500, 9000, 7500, 6000, 4200]
    resolution_df = pd.DataFrame({
        'Date': dates,
        'Auto': auto_res,
        'Manual': manual_res
    }).melt('Date', var_name='Type', value_name='Records')
    
    # 2. Risk Data (Hardcoded high-stakes vectors)
    risk_data = pd.DataFrame({
        'Category': ['Unresolved High Value', 'SLA Breach Risk', 'Policy Violations', 'Low Confidence Links'],
        'Count': [124, 45, 892, 3400],
        'Severity': ['High', 'High', 'Medium', 'Low']
    })
    
    # 3. Source System Coverage (Realistic distribution)
    source_data = pd.DataFrame({
        'System': ['Epic EMR', 'Cerner', 'Salesforce CRM', 'Legacy Billing', 'Ext. Lab Feeds'],
        'Records': [4500000, 3200000, 1500000, 2100000, 800000],
        'TrustLevel': ['High', 'High', 'Medium', 'Low', 'Medium']
    })

    return {
        'trust_trend': trust_df,
        'resolution_trend': resolution_df,
        'risk_data': risk_data,
        'source_data': source_data
    }

def render():
    # --- 1. SESSION STATE & LIVE DATA ---
    if 'dash_v2_filter' not in st.session_state:
        st.session_state['dash_v2_filter'] = "Operational"
    if 'dash_v2_horizon' not in st.session_state:
        st.session_state['dash_v2_horizon'] = "30 Days"

    data = get_enterprise_data()
    
    # --- 2. PREMIUM DESIGN SYSTEM (Glassmorphism v2) ---
    # Using HSL for more precise control over luminance and alpha
    DESIGN = {
        "bg_main": "hsl(215, 25%, 98%)",
        "bg_glass": "#ffffff",
        "accent": "hsl(348, 83%, 47%)",      
        "emerald": "hsl(158, 64%, 52%)",     
        "slate_900": "hsl(222, 47%, 11%)",
        "slate_700": "hsl(222, 10%, 30%)",
        "slate_500": "hsl(215, 16%, 47%)",
        "slate_100": "hsla(215, 20%, 90%, 0.4)",
        "border_subtle": "rgba(15, 23, 42, 0.08)",
        "shadow_card": "0 1px 3px rgba(0,0,0,0.02), 0 10px 15px -3px rgba(0,0,0,0.03)",
        "glow_accent": "0 0 20px rgba(209, 31, 65, 0.15)"
    }

    # Atomic CSS Payload
    st.markdown(f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&display=swap');
        
        /* Layout & Surface Texture */
        .stApp {{ 
            background: {DESIGN['bg_main']} !important; 
            background-image: radial-gradient({DESIGN['slate_100']} 1px, transparent 1px) !important;
            background-size: 24px 24px !important;
        }}
        .block-container {{ padding-top: 2rem !important; padding-bottom: 2rem !important; }}
        
        /* Typography */
        [data-testid="stMain"] h1, [data-testid="stMain"] h2, [data-testid="stMain"] h3, 
        [data-testid="stMain"] .stMarkdown {{ 
            font-family: 'Outfit', sans-serif !important; 
            letter-spacing: -0.02em; 
        }}
        
        /* Card System - MATHEMATICAL SYMMETRY */
        .v2-card {{
            background: {DESIGN['bg_glass']};
            border: 1px solid {DESIGN['border_subtle']};
            border-radius: 12px;
            padding: 24px;
            box-shadow: {DESIGN['shadow_card']};
            margin-bottom: 0px;
            height: 100%;
            display: flex;
            flex-direction: column;
            transition: all 0.3s ease;
        }}
        
        /* --- COMMAND STRIP UNIFICATION --- */
        
        /* --- COMMAND STRIP UNIFICATION --- */
        /* Unified height and foundation for all controls in the top row */
        div[data-testid="stHorizontalBlock"] div.stSelectbox [data-baseweb="select"] > div,
        div[data-testid="stHorizontalBlock"] div.stButton button {{
            height: 42px !important;
            min-height: 42px !important;
            border-radius: 12px !important;
            font-family: 'Outfit', sans-serif !important;
            font-size: 14px !important;
            font-weight: 500 !important; /* Reduced from 600 */
            display: flex !important;
            align-items: center !important;
            transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1) !important;
            padding-top: 0 !important;
            padding-bottom: 0 !important;
        }}

        /* Secondary/Default Button Styling */
        div[data-testid="stHorizontalBlock"] div.stButton button {{
            background: white !important;
            border: 1px solid rgba(15, 23, 42, 0.2) !important;
            color: {DESIGN['slate_900']} !important;
            justify-content: center !important;
            width: 100% !important;
            box-shadow: 0 1px 2px rgba(0,0,0,0.05) !important;
        }}

        /* Primary Button Styling (Execute Global Linkage) - DEFAULT WHITE */
        div[data-testid="stHorizontalBlock"] div.stButton button[kind="primary"] {{
            background: white !important;
            border: 1px solid rgba(15, 23, 42, 0.2) !important;
            color: {DESIGN['slate_900']} !important;
            box-shadow: 0 1px 2px rgba(0,0,0,0.05) !important;
        }}

        /* Selectbox specific foundation */
        div[data-testid="stHorizontalBlock"] div.stSelectbox [data-baseweb="select"] > div {{
            background: white !important;
            border: 1px solid rgba(15, 23, 42, 0.2) !important; /* Slightly darker border for visibility */
        }}

        /* Unified Hover State for ALL Buttons */
        div[data-testid="stHorizontalBlock"] div.stButton button:hover,
        div[data-testid="stHorizontalBlock"] div.stButton button[kind="primary"]:hover {{
            background: {DESIGN['accent']} !important;
            border-color: {DESIGN['accent']} !important;
            color: white !important;
            transform: translateY(-2px) !important;
            box-shadow: 0 10px 20px -5px rgba(209, 31, 65, 0.4), {DESIGN['glow_accent']} !important;
        }}

        div[data-testid="stHorizontalBlock"] div.stSelectbox [data-baseweb="select"]:hover > div {{
            border-color: {DESIGN['slate_900']} !important;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1) !important;
        }}

        /* Tactile Click (Active) State for ALL Buttons */
        div[data-testid="stHorizontalBlock"] div.stButton button:active {{
            transform: scale(0.98) translateY(0) !important;
            transition: transform 0.1s !important;
            background: {DESIGN['accent']} !important; /* Ensure it stays red on click */
            filter: brightness(0.9);
        }}

        /* Selectbox value text visibility - FORCE ELEGANT TEXT */
        div[data-testid="stHorizontalBlock"] div.stSelectbox [data-baseweb="select"] div,
        div[data-testid="stHorizontalBlock"] div.stSelectbox [data-baseweb="select"] span {{
            color: {DESIGN['slate_900']} !important;
            font-weight: 500 !important; /* Reduced from 600 */
        }}

        /* KPI System */
        .stat-group {{ display: flex; flex-direction: column; gap: 4px; }}
        .stat-label {{ font-size: 11px; font-weight: 500; color: {DESIGN['slate_500']}; text-transform: uppercase; letter-spacing: 0.05em; }}
        .stat-value {{ font-size: 28px; font-weight: 500; color: {DESIGN['slate_900']}; line-height: 1; }}
        .stat-trend {{ font-size: 11px; font-weight: 500; display: flex; align-items: center; gap: 4px; }}
        
        /* Live Signal */
        .live-signal {{
            display: flex;
            align-items: center;
            gap: 10px;
            background: hsla(215, 25%, 90%, 0.5);
            padding: 6px 16px;
            border-radius: 50px;
            font-size: 11px;
            font-weight: 500;
            color: {DESIGN['slate_900']};
        }}
        .signal-dot {{
            width: 8px;
            height: 8px;
            background: {DESIGN['emerald']};
            border-radius: 50%;
            box-shadow: 0 0 10px {DESIGN['emerald']};
            animation: pulse 2s infinite;
        }}
        @keyframes pulse {{
            0% {{ transform: scale(0.9); opacity: 0.5; box-shadow: 0 0 0 0 hsla(158, 64%, 52%, 0.4); }}
            50% {{ transform: scale(1); opacity: 1; box-shadow: 0 0 0 12px hsla(158, 64%, 52%, 0); }}
            100% {{ transform: scale(0.9); opacity: 0.5; box-shadow: 0 0 0 0 hsla(158, 64%, 52%, 0); }}
        }}

        /* --- DROPDOWN & POPOVER MENU STYLING (MATCH REFERENCE) --- */
        [data-baseweb="popover"],
        [data-baseweb="popover"] > div {{
            background: white !important;
            background-color: white !important;
            border: none !important;
            box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.1), 0 8px 10px -6px rgba(0, 0, 0, 0.1) !important;
            border-radius: 12px !important;
            overflow: hidden !important;
        }}
        
        ul[data-testid="stSelectboxVirtualDropdown"],
        [data-baseweb="menu"],
        ul[role="listbox"] {{
            background: white !important;
            padding: 6px !important;
            border: none !important;
        }}
        
        li[role="option"] {{
            background: white !important;
            color: {DESIGN['slate_900']} !important;
            border-radius: 8px !important;
            margin: 2px 4px !important;
            padding: 10px 14px !important;
            font-size: 14px !important;
            font-weight: 500 !important;
            transition: all 0.2s ease !important;
        }}
        
        /* Selection Highlight (Light Red) */
        li[role="option"][aria-selected="true"] {{
            background: #fef2f2 !important;
            color: {DESIGN['accent']} !important;
            font-weight: 500 !important;
        }}
        
        li[role="option"]:hover {{
            background: #f8fafc !important;
        }}

        /* Progress Bar */
        div[data-testid="stProgress"] > div > div > div > div {{
            background: linear-gradient(90deg, {DESIGN['accent']}, #f43f5e) !important;
            border-radius: 10px !important;
        }}
        
    </style>
    """, unsafe_allow_html=True)
    
    # --- 3. COMMAND CENTER HEADER ---
    st.markdown(f"""
    <div style="display: flex; align-items: flex-end; justify-content: space-between; margin-bottom: 32px;">
        <div style="flex: 1;">
            <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 4px;">
                <div style="width: 32px; height: 4px; background: {DESIGN['accent']}; border-radius: 2px;"></div>
                <span style="font-size: 11px; font-weight: 500; color: {DESIGN['accent']}; text-transform: uppercase; letter-spacing: 0.1em;">Enterprise Intelligence</span>
            </div>
            <h1 style="font-size: 38px; font-weight: 500; color: {DESIGN['slate_900']}; margin: 0; line-height: 1;">Operational Command</h1>
        </div>
        <div class="live-signal">
            <div class="signal-dot"></div>
            SIGNAL: LIVE ENTERPRISE FEED
        </div>
    </div>
    """, unsafe_allow_html=True)

    # --- 4. COMMAND STRIP ---
    c1, c2, c3, c4 = st.columns([1.5, 1.5, 1.2, 1.2])
    with c1:
        st.session_state['dash_v2_horizon'] = st.selectbox("Time Window", ["Last 7 Days", "Last 30 Days", "Fiscal Q1", "Full Year"], label_visibility="collapsed")
    with c2:
        st.session_state['dash_v2_filter'] = st.selectbox("Operational Focus", ["All Platforms", "Clinical Core", "Financial Access", "Customer CRM"], label_visibility="collapsed")
    with c3:
        if st.button("Execute Global Linkage", use_container_width=True, type="primary"):
            st.toast("Syncing distributed vectors...")
    with c4:
        st.button("Advanced Pipeline", use_container_width=True)
    st.markdown('<div style="height: 32px;"></div>', unsafe_allow_html=True)

    # --- 5. KPI HUB ---
    k1, k2, k3, k4 = st.columns(4)
    
    def render_stat(col, label, value, trend, is_good=True):
        t_color = DESIGN['emerald'] if is_good else DESIGN['accent']
        col.markdown(f"""
        <div class="v2-card" style="padding: 20px;">
            <div style="font-size: 11px; font-weight: 500; color: {DESIGN['slate_500']}; text-transform: uppercase; margin-bottom: 12px; letter-spacing: 0.05em;">{label}</div>
            <div style="display: flex; align-items: flex-end; justify-content: space-between;">
                <div style="font-size: 28px; font-weight: 600; color: {DESIGN['slate_900']}; line-height: 1;">{value}</div>
                <div style="font-size: 12px; font-weight: 600; color: {t_color}; display: flex; align-items: center; gap: 4px;">
                    {'↑' if is_good else '↓'} {trend}
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    render_stat(k1, "Records Governed", "14.2M", "4.2%", True)
    render_stat(k2, "Golden Consistency", "96.4%", "1.8%", True)
    render_stat(k3, "Duplicate Risk", "0.42%", "12%", False)
    render_stat(k4, "Throughput / HR", "84.2K", "9.1%", True)

    st.markdown('<div style="height: 24px;"></div>', unsafe_allow_html=True)

    # --- 6. CORE INTELLIGENCE GRID ---
    g1, g2 = st.columns([2.2, 1])

    with g1:
        # 1. Base Layer & Scales
        y_scale = alt.Scale(domain=[75, 100])
        y_axis = alt.Axis(grid=True, gridColor=DESIGN['slate_100'], domain=False, labelColor=DESIGN['slate_500'], labelFontSize=11, labelFont='Outfit', title=None, tickCount=5)
        
        base = alt.Chart(data['trust_trend']).transform_calculate(
            baseline="75"
        ).encode(
            x=alt.X('Date:T', axis=alt.Axis(format='%b', grid=False, domain=False, labelColor=DESIGN['slate_500'], labelFontSize=11, labelFont='Outfit', title=None, tickCount=12))
        )

        # 2. Area Chart (Anchored to 75 to avoid 0-baseline drop)
        area = base.mark_area(
            color=alt.Gradient(
                gradient='linear',
                stops=[alt.GradientStop(color=DESIGN['accent'], offset=0),
                       alt.GradientStop(color='white', offset=1)],
            ),
            opacity=0.15
        ).encode(
            y=alt.Y('TrustScore:Q', scale=y_scale, axis=y_axis),
            y2=alt.Y2('baseline:Q')
        )
        
        # 3. Line Chart
        line = base.mark_line(color=DESIGN['accent'], strokeWidth=2).encode(
            y=alt.Y('TrustScore:Q', scale=y_scale)
        )
        
        # 4. Interactive Points
        hover = alt.selection_point(on='mouseover', nearest=True, empty=False) if hasattr(alt, 'selection_point') else alt.selection_single(on='mouseover', nearest=True, empty=False)
        points = base.mark_point(
            size=90, color=DESIGN['accent'], fill='white', strokeWidth=2
        ).encode(
            y=alt.Y('TrustScore:Q', scale=y_scale),
            opacity=alt.condition(hover, alt.value(1), alt.value(0)),
            tooltip=[alt.Tooltip('Date:T', title='Date', format='%b %Y'), alt.Tooltip('TrustScore:Q', title='Trust Score', format='.1f')]
        )
        try:
            points = points.add_params(hover)
        except AttributeError:
            points = points.add_selection(hover)

        final_chart = (area + line + points).properties(height=300).configure_view(stroke="transparent").configure(background='white')
        
        # 3. Render Card Wrapper
        st.markdown(f"""
        <div class="v2-card">
            <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 20px;">
                <div>
                    <h3 style="font-size: 16px; font-weight: 500; color: {DESIGN['slate_900']}; margin: 0;">Enterprise Trust Trajectory</h3>
                    <p style="font-size: 12px; color: {DESIGN['slate_500']}; margin: 2px 0 0 0;">Synthesized data health index over 12 months</p>
                </div>
                <div style="font-size: 9px; font-weight: 600; color: {DESIGN['slate_500']}; background: {DESIGN['bg_main']}; padding: 4px 8px; border-radius: 4px; letter-spacing: 0.05em;">VECTOR: GLOBAL RECONCILIATION</div>
            </div>
        """, unsafe_allow_html=True)
        
        # 4. Render Chart inside the open div
        st.altair_chart(final_chart, use_container_width=True)
        
        # 5. Close Card Wrapper
        st.markdown('</div>', unsafe_allow_html=True)
        
    with g2:
        risks_data = [
            ("SLA BREACH: EPIC-01", "High Severity", DESIGN['accent'], "LATE"),
            ("DUPLICATE CLUSTER: PHX", "Review Req", "#F59E0B", "PEND"),
            ("SYSTEM SYNC DELAY", "Delta Lake", "#3B82F6", "SYNC"),
            ("INCONSISTENT PROVIDER ID", "Manual Resolve", DESIGN['accent'], "ERR")
        ]
        
        risks_html = "".join([f"""<div style="display: flex; align-items: center; gap: 12px; padding: 12px 0; border-bottom: {('1px solid ' + DESIGN['slate_100']) if i < len(risks_data)-1 else 'none'};"><div style="width: 32px; height: 32px; border-radius: 8px; background: {color}15; display: flex; align-items: center; justify-content: center; font-size: 10px; font-weight: 700; color: {color};">{status}</div><div style="flex: 1;"><div style="font-size: 13px; font-weight: 500; color: {DESIGN['slate_900']};">{title}</div><div style="font-size: 11px; color: {DESIGN['slate_500']};">{subtitle}</div></div><div style="font-size: 14px; color: {DESIGN['slate_500']}; opacity: 0.5;">→</div></div>""" for i, (title, subtitle, color, status) in enumerate(risks_data)])

        st.markdown(f"""<div class="v2-card"><h3 style="font-size: 16px; font-weight: 500; color: {DESIGN['slate_900']}; margin: 0;">High Risk Vectors</h3><p style="font-size: 12px; color: {DESIGN['slate_500']}; margin: 2px 0 16px 0;">Urgent items requiring intervention</p>{risks_html}<div style="margin-top: 20px;"></div></div>""", unsafe_allow_html=True)
        
        if st.button("Open Operational Review", use_container_width=True, type="primary"):
            st.session_state['current_page'] = "Match Review"
            st.rerun()

    st.markdown('<div style="height: 32px;"></div>', unsafe_allow_html=True)

    # --- 7. PLATFORM EFFICIENCY ---
    c5, c6 = st.columns([1, 1.2])
    
    with c5:
        # Wrap in a native Streamlit container
        with st.container(border=True):
            st.markdown(f"""
                <h3 style="font-size: 16px; font-weight: 500; color: {DESIGN["slate_900"]}; margin: 0 0 4px 0;">Resolution Effectiveness</h3>
                <p style="font-size: 12px; color: {DESIGN['slate_500']}; margin-bottom: 24px;">Automatic vs Manual performance</p>
            """, unsafe_allow_html=True)
            
            chart_res = alt.Chart(data['resolution_trend']).mark_bar(size=14, cornerRadius=2).encode(
                x=alt.X('Date:T', axis=alt.Axis(format='%b', title=None, grid=False, domain=False, labelFont='Outfit', labelColor=DESIGN['slate_500'], labelFontSize=11, tickCount=12)),
                y=alt.Y('Records:Q', axis=alt.Axis(title=None, format='~s', grid=True, gridColor=DESIGN['slate_100'], domain=False, labelFont='Outfit', labelColor=DESIGN['slate_500'], labelFontSize=11)),
                color=alt.Color('Type:N', scale=alt.Scale(domain=['Auto', 'Manual'], range=[DESIGN['slate_900'], DESIGN['accent']]), legend=alt.Legend(orient='top-right', title=None, symbolType='circle', labelFont='Outfit', labelColor=DESIGN['slate_500'])),
            ).properties(height=180).configure_view(stroke="transparent").configure(background='white')
            
            st.altair_chart(chart_res, use_container_width=True)
            
    with c6:
        st.markdown(f"""
        <div class="v2-card">
            <h3 style="font-size: 16px; font-weight: 500; color: {DESIGN["slate_900"]}; margin: 0 0 16px 0;">Platform Health Distribution</h3>
            <div style="display: flex; align-items: center; justify-content: center; flex: 1; padding: 10px 0;">
        """, unsafe_allow_html=True)
            
        chart_source = alt.Chart(data['source_data']).mark_arc(innerRadius=60, cornerRadius=4).encode(
            theta=alt.Theta(field="Records", type="quantitative"),
            color=alt.Color(field="System", scale=alt.Scale(range=[DESIGN['slate_900'], DESIGN['accent'], DESIGN['emerald'], '#3B82F6', '#F59E0B']), legend=alt.Legend(orient='right', labelFont='Outfit', labelColor=DESIGN['slate_500'], labelFontSize=11)),
            tooltip=['System', 'Records']
        ).properties(height=180).configure_view(stroke="transparent").configure(background='white')
        
        st.altair_chart(chart_source, use_container_width=True)
        
        st.markdown(f"""
            </div>
            <div style="display: flex; justify-content: space-between; align-items: center; border-top: 1px solid {DESIGN['slate_100']}; padding-top: 16px; margin-top: 10px;">
                <div style="font-size: 11px; font-weight: 500; color: {DESIGN['slate_500']}; text-transform: uppercase;">Confidence Index</div>
                <div style="font-size: 24px; font-weight: 600; color: {DESIGN['emerald']}; line-height: 1;">94<span style="font-size: 14px;">%</span></div>
            </div>
        </div>
        """, unsafe_allow_html=True)
