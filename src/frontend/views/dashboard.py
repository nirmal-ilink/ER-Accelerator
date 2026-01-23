import streamlit as st
import altair as alt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# --- MOCK DATA GENERATOR ---
def generate_enterprise_data():
    """Generates realistic enterprise-scale mock data for the dashboard."""
    np.random.seed(42)
    
    # 1. Trend Data (Last 12 Months)
    dates = pd.date_range(end=datetime.today(), periods=12, freq='ME')
    
    # Trust Index (Calculated composite score)
    trust_base = np.linspace(72, 88, 12) + np.random.normal(0, 1, 12)
    trust_df = pd.DataFrame({'Date': dates, 'TrustScore': trust_base})
    
    # Auto-Resolution vs Manual
    auto_res = np.linspace(80000, 150000, 12).astype(int)
    manual_res = np.linspace(15000, 5000, 12).astype(int)
    resolution_df = pd.DataFrame({
        'Date': dates,
        'Auto': auto_res,
        'Manual': manual_res
    }).melt('Date', var_name='Type', value_name='Records')
    
    # 2. Risk Data
    risk_data = pd.DataFrame({
        'Category': ['Unresolved High Value', 'SLA Breach Risk', 'Policy Violations', 'Low Confidence Links'],
        'Count': [124, 45, 892, 3400],
        'Severity': ['High', 'High', 'Medium', 'Low']
    })
    
    # 3. Source System Coverage
    source_data = pd.DataFrame({
        'System': ['Epic EMR', 'Cerner', 'Salesforce CRM', 'Legacy Billing', 'Ext. Lab Feeds'],
        'Records': [4500000, 3200000, 1500000, 2100000, 800000],
        'TrustLevel': ['High', 'High', 'Medium', 'Low', 'Medium']
    })

    return {
        'trust_trend': trust_df,
        'resolution_trend': resolution_df,
        'risk_data': risk_data,
        'source_data': source_data,
        'kpis': {
            'records_gov': "12.4M",
            'golden_records': "8.1M",
            'dup_exposure': "1.2%",
            'auto_res_rate': "94.8%",
            'intervention': "2.3%",
            'trust_index': "88/100"
        }
    }

def render():
    data = generate_enterprise_data()
    
    # --- BRAND CONSTANTS ---
    # Enterprise Colors - Refined
    COLORS = {
        'red': "#D11F41",      # Core Brand Red
        'slate': "#0F172A",    # Dark Slate (Text/Headers)
        'slate_light': "#64748B", # Muted Text
        'bg_card': "#FFFFFF",  # Pure White Cards
        'bg_main': "#F8FAFC",  # Light Slate Background (for App)
        'green': "#059669",    # Success Green
        'green_light': "#ECFDF5", # Light Green bg
        'red_light': "#FFF1F2",   # Light Red bg
        'border': "#E2E8F0"    # Subtle Borders
    }
    
    # --- CSS: RESPONSIVE BOARDROOM STYLE ---
    st.markdown(f"""
    <style>
        .stApp {{ background-color: {COLORS['bg_main']} !important; }}
        
        /* Typography */
        h1, h2, h3, h4, .stMarkdown {{ font-family: 'Inter', sans-serif !important; letter-spacing: -0.2px; }}
        
        /* RESPONSIVE GRID SYSTEM FOR KPIS */
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
            gap: 16px;
            margin-bottom: 32px;
            width: 100%;
        }}
        
        /* CARD SYSTEM */
        .board-card {{
            background: {COLORS['bg_card']};
            border: 1px solid {COLORS['border']};
            border-radius: 12px;
            padding: 20px 24px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.04);
            transition: all 0.2s ease;
            height: 100%;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        }}
        .board-card:hover {{
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.05); /* Softer shadow */
            transform: translateY(-2px);
            border-color: #CBD5E1;
        }}
        
        /* KPI STYLING */
        .kpi-label {{
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.8px;
            color: {COLORS['slate_light']};
            font-weight: 600;
            margin-bottom: 8px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }}
        
        .kpi-value {{
            font-size: clamp(24px, 2.5vw, 32px); /* Responsive font */
            font-weight: 700;
            color: {COLORS['slate']};
            line-height: 1.1;
            margin-bottom: 8px;
        }}
        
        .kpi-trend {{
            font-size: 11px;
            font-weight: 600;
            display: inline-flex;
            align-items: center;
            padding: 4px 8px;
            border-radius: 99px;
            width: fit-content;
        }}
        
        .trend-up {{ color: {COLORS['green']}; background: {COLORS['green_light']}; }}
        .trend-down {{ color: {COLORS['red']}; background: {COLORS['red_light']}; }}
        .trend-neutral {{ color: {COLORS['slate_light']}; background: #F1F5F9; }}
        
        /* SECTIONS */
        .section-header {{
            font-size: 18px;
            font-weight: 700;
            color: {COLORS['slate']};
            margin-top: 32px;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 12px;
        }}
        .section-header::before {{
            content: "";
            display: block;
            width: 4px;
            height: 20px;
            background: {COLORS['red']};
            border-radius: 2px;
        }}
        
        /* IMPACT TYPOGRAPHY */
        .impact-number {{
            font-size: clamp(32px, 3vw, 42px);
            font-weight: 800;
            color: {COLORS['red']};
            letter-spacing: -1px;
            line-height: 1.2;
        }}
        .impact-desc {{
            font-size: 13px;
            color: {COLORS['slate_light']};
            line-height: 1.5;
            margin-top: 4px;
        }}
        
        /* UTILS */
        .flex-center {{ display: flex; align-items: center; justify-content: center; height: 100%; }}
        .text-center {{ text-align: center; }}
        
    </style>
    """, unsafe_allow_html=True)
    
    # --- HEADER ---
    st.markdown("""
<div class="main-content">
<div style="display: flex; flex-wrap: wrap; justify-content: space-between; align-items: flex-end; margin-bottom: 32px; gap: 16px;">
    <div style="min-width: 280px;">
        <h1 style="font-size: 32px; font-weight: 800; color: #0F172A; margin: 0;">Operational Command</h1>
        <p style="color: #64748B; font-size: 14px; margin-top: 6px;">Executive Enterprise Visibility & Governance Console</p>
    </div>
    <div style="text-align: right; flex-grow: 1;">
            <span style="background: #E2E8F0; color: #475569; padding: 6px 12px; border-radius: 20px; font-size: 12px; font-weight: 600; white-space: nowrap;">Live Feed: Connected</span>
    </div>
</div>
    """, unsafe_allow_html=True)
    
    # --- 1. KPI COMMAND STRIP (CSS Grid implementation) ---
    kpis = data['kpis']
    
    metrics = [
        ("Records Governed", kpis['records_gov'], "+2.4% MoM", "trend-up"),
        ("Golden Records", kpis['golden_records'], "+5.1% MoM", "trend-up"),
        ("Duplicate Exposure", kpis['dup_exposure'], "-0.8% MoM", "trend-up"), 
        ("Auto-Res Rate", kpis['auto_res_rate'], "+1.2% MoM", "trend-up"),
        ("Intervention Rate", kpis['intervention'], "-0.5% MoM", "trend-up"),
        ("Trust Index", kpis['trust_index'], "Static", "trend-neutral")
    ]
    
    # Construct Grid HTML WITHOUT INDENTATION to avoid raw code blocks
    kpi_html = '<div class="kpi-grid">'
    for label, value, trend, trend_class in metrics:
        kpi_html += f"""
<div class="board-card">
    <div>
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
    </div>
    <div class="kpi-trend {trend_class}">
        {trend}
    </div>
</div>"""
    kpi_html += '</div>'
    
    st.markdown(kpi_html, unsafe_allow_html=True)
            
    # --- 2. DATA HEALTH & TRUST ---
    st.markdown('<div class="section-header">Enterprise Data Health & Trust</div>', unsafe_allow_html=True)
    c1, c2 = st.columns([2, 1])
    
    with c1:
        st.markdown('<div class="board-card">', unsafe_allow_html=True)
        st.markdown("**Enterprise Trust Index Trend**")
        st.markdown("<p style='font-size: 12px; color: #64748B; margin-bottom: 16px;'>Composite score of accuracy, completeness, and consistency over time.</p>", unsafe_allow_html=True)
        
        chart_trust = alt.Chart(data['trust_trend']).mark_area(
            line={'color': COLORS['red']},
            color=alt.Gradient(
                gradient='linear',
                stops=[alt.GradientStop(color=COLORS['red'], offset=0),
                       alt.GradientStop(color='white', offset=1)],
                x1=1, x2=1, y1=1, y2=0
            )
        ).encode(
            x=alt.X('Date', axis=alt.Axis(format='%b %Y', grid=False, tickCount=6, domain=False, labelColor=COLORS['slate_light'])),
            y=alt.Y('TrustScore', scale=alt.Scale(domain=[60, 100]), axis=alt.Axis(grid=True, gridColor='#F1F5F9', domain=False, labelColor=COLORS['slate_light'])),
            tooltip=['Date', 'TrustScore']
        ).properties(height=240, background='transparent').configure_view(strokeWidth=0).configure_axis(
            labelFont='Inter',
            titleFont='Inter'
        )
        
        st.altair_chart(chart_trust, width="stretch")
        st.markdown('</div>', unsafe_allow_html=True)
        
    with c2:
        st.markdown('<div class="board-card">', unsafe_allow_html=True)
        st.markdown("**Confidence Distribution**")
        st.markdown("<p style='font-size: 12px; color: #64748B; margin-bottom: 16px;'>Match score distribution.</p>", unsafe_allow_html=True)
        
        conf_data = pd.DataFrame({'Bucket': ['90-100%', '80-90%', '70-80%', '<70%'], 'Count': [65, 20, 10, 5]})
        
        chart_conf = alt.Chart(conf_data).mark_bar(cornerRadius=4).encode(
            x=alt.X('Count', axis=None),
            y=alt.Y('Bucket', sort=None, axis=alt.Axis(domain=False, tickSize=0, labelFontWeight='bold', labelColor=COLORS['slate_light'])),
            color=alt.Color('Bucket', scale=alt.Scale(range=[COLORS['green'], '#34D399', '#FBBF24', COLORS['red']]), legend=None),
            tooltip=['Bucket', 'Count']
        ).properties(height=240, background='transparent').configure_view(strokeWidth=0)
        
        st.altair_chart(chart_conf, width="stretch")
        st.markdown('</div>', unsafe_allow_html=True)

    # --- 3. RESOLUTION EFFECTIVENESS ---
    st.markdown('<div class="section-header">Resolution Effectiveness</div>', unsafe_allow_html=True)
    c3, c4 = st.columns([2, 1])
    
    with c3:
        st.markdown('<div class="board-card">', unsafe_allow_html=True)
        st.markdown("**Auto-Resolution vs Manual Intervention**")
        st.markdown("<p style='font-size: 12px; color: #64748B; margin-bottom: 16px;'>Monthly resolution volume by type.</p>", unsafe_allow_html=True)
        
        chart_res = alt.Chart(data['resolution_trend']).mark_bar().encode(
            x=alt.X('Date', axis=alt.Axis(format='%b', title=None, domain=False, labelColor=COLORS['slate_light'])),
            y=alt.Y('Records', axis=alt.Axis(title=None, grid=True, gridColor='#F1F5F9', format='~s', domain=False, labelColor=COLORS['slate_light'])),
            color=alt.Color('Type', scale=alt.Scale(domain=['Auto', 'Manual'], range=[COLORS['slate'], COLORS['red']])),
            tooltip=['Date', 'Type', 'Records']
        ).properties(height=240, background='transparent').configure_view(strokeWidth=0)
        
        st.altair_chart(chart_res, width="stretch")
        st.markdown('</div>', unsafe_allow_html=True)
        
    with c4:
        st.markdown(f"""
        <div class="board-card text-center">
            <div style="font-weight: 600; color: {COLORS['slate']}; margin-bottom: 24px;">Throughput Velocity</div>
            <div style="font-size: clamp(32px, 3vw, 42px); font-weight: 800; color: {COLORS['slate']};">12,450</div>
            <div style="font-size: 12px; color: {COLORS['slate_light']}; font-weight: 600; text-transform: uppercase; letter-spacing: 1px;">Records / Hour</div>
             <div style="margin-top: 24px; width: 100%; height: 8px; background: #E2E8F0; border-radius: 4px; overflow: hidden;">
                <div style="width: 85%; height: 100%; background: {COLORS['green']};"></div>
            </div>
            <div style="margin-top: 8px; font-size: 11px; color: {COLORS['slate_light']};">85% of Peak Capacity (15k/hr)</div>
        </div>
        """, unsafe_allow_html=True)

    # --- 4. DATA IMPACT & RISK ---
    c5, c6 = st.columns([1, 1])
    
    with c5:
        st.markdown('<div class="section-header">Business Impact</div>', unsafe_allow_html=True)
        st.markdown(f"""
        <div class="board-card">
            <div style="display: flex; align-items: center; justify-content: space-between; gap: 16px; flex-wrap: wrap;">
                <div style="flex: 1; min-width: 140px;">
                    <div class="kpi-label">Operational Cost Avoidance</div>
                    <div class="impact-number">$4.2M</div>
                    <div class="impact-desc">YTD savings from automation.</div>
                </div>
                <div style="width: 1px; height: 60px; background: #E2E8F0; display: block;"></div>
                <div style="flex: 1; min-width: 140px; padding-left: 12px;">
                     <div class="kpi-label">Reliability Uplift</div>
                     <div class="impact-number" style="color: {COLORS['green']};">+18%</div>
                     <div class="impact-desc">Downstream accuracy.</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
    with c6:
        st.markdown('<div class="section-header">Risk & Governance</div>', unsafe_allow_html=True)
        
        # Risk chart
        chart_risk = alt.Chart(data['risk_data']).mark_bar().encode(
            x=alt.X('Count', axis=alt.Axis(grid=True, gridColor='#F1F5F9', domain=False, labelColor=COLORS['slate_light'])),
            y=alt.Y('Category', sort='-x', axis=alt.Axis(title=None, labelLimit=120, domain=False, labelColor=COLORS['slate_light'])),
            color=alt.Color('Severity', scale=alt.Scale(domain=['High', 'Medium', 'Low'], range=[COLORS['red'], '#F59E0B', '#10B981']), legend=None),
            tooltip=['Category', 'Count', 'Severity']
        ).properties(height=140, background='transparent').configure_view(strokeWidth=0)
        
        st.markdown('<div class="board-card" style="padding: 16px;">', unsafe_allow_html=True)
        st.altair_chart(chart_risk, width="stretch")
        st.markdown('</div>', unsafe_allow_html=True)

    # --- 5. ENTERPRISE COVERAGE ---
    st.markdown('<div class="section-header">Enterprise Coverage</div>', unsafe_allow_html=True)
    st.markdown('<div class="board-card">', unsafe_allow_html=True)
    
    chart_source = alt.Chart(data['source_data']).mark_arc(innerRadius=70).encode(
        theta=alt.Theta(field="Records", type="quantitative"),
        color=alt.Color(field="System", legend=alt.Legend(orient="bottom", title=None, columns=5, labelLimit=200, labelColor=COLORS['slate_light'])),
        order=alt.Order("Records", sort="descending"),
        tooltip=['System', 'Records', 'TrustLevel']
    ).properties(height=300, background='transparent').configure_view(strokeWidth=0)
    
    st.altair_chart(chart_source, width="stretch")
    st.markdown('</div></div>', unsafe_allow_html=True)
