import streamlit as st

def render():
    # --- DASHBOARD SPECIFIC CSS ---
    st.markdown("""
    <style>
        /* Card Container */
        .metric-card {
            background-color: #ffffff;
            border-radius: 16px;
            padding: 24px;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.02), 0 2px 4px -1px rgba(0, 0, 0, 0.02);
            border: 1px solid #f1f5f9;
            transition: transform 0.2s ease, box-shadow 0.2s ease;
            height: 100%;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        }
        
        .metric-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.05), 0 4px 6px -2px rgba(0, 0, 0, 0.025);
            border-color: #e2e8f0;
        }

        .metric-label {
            color: #64748b;
            font-size: 11px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.8px;
            margin-bottom: 8px;
        }
        
        .metric-value {
            color: #0f172a;
            font-size: 32px;
            font-weight: 800;
            letter-spacing: -1px;
            line-height: 1;
        }
        
        .metric-delta {
            display: inline-flex;
            align-items: center;
            padding: 4px 10px;
            border-radius: 9999px;
            font-size: 11px;
            font-weight: 600;
            margin-top: 12px;
            width: fit-content;
        }
        
        .delta-positive {
            background-color: #ecfdf5;
            color: #047857;
        }
        
        .delta-negative {
            background-color: #fef2f2;
            color: #b91c1c;
        }
        
        /* Chart Container */
        .chart-container {
            background-color: #ffffff;
            border-radius: 20px;
            padding: 24px;
            margin-top: 24px;
            border: 1px solid #f1f5f9;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.02);
        }
        
        .chart-header {
            color: #1e293b;
            font-size: 16px;
            font-weight: 700;
            margin-bottom: 20px;
            letter-spacing: -0.3px;
        }
    </style>
    """, unsafe_allow_html=True)

    # --- HEADER ---
    st.markdown("""
    <div style="margin-bottom: 32px;">
        <h1 style="font-size: 28px; font-weight: 800; text-align: left; color: #0f172a; letter-spacing: -1px; margin-bottom: 8px;">Executive Dashboard</h1>
        <p style="text-align: left; color: #64748b; font-size: 14px;">Real-time insights on Data Quality and Entity Resolution performance.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # --- METRICS ROW ---
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="metric-card">
            <div>
                <div class="metric-label">Total Records</div>
                <div class="metric-value">1,024</div>
            </div>
            <div class="metric-delta delta-positive">
                ↑ 12.5% vs last week
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown("""
        <div class="metric-card">
            <div>
                <div class="metric-label">Duplicates Found</div>
                <div class="metric-value">342</div>
            </div>
            <div class="metric-delta delta-negative">
                ↓ 2% efficiency gain
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown("""
        <div class="metric-card">
            <div>
                <div class="metric-label">Auto-Merge Rate</div>
                <div class="metric-value">88%</div>
            </div>
            <div class="metric-delta delta-positive">
                ↑ 5% accuracy boost
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # --- CHARTS ROW ---
    c1, c2 = st.columns(2)
    
    with c1:
        st.markdown('<div class="chart-container"><div class="chart-header">Data Quality Score</div>', unsafe_allow_html=True)
        st.bar_chart({"Accuracy": 90, "Completeness": 85, "Consistency": 92}, color="#3b82f6")
        st.markdown('</div>', unsafe_allow_html=True)
        
    with c2:
        st.markdown('<div class="chart-container"><div class="chart-header">Records by Source</div>', unsafe_allow_html=True)
        st.bar_chart({"EMR": 500, "Registry": 524}, color="#0ea5e9")
        st.markdown('</div>', unsafe_allow_html=True)
