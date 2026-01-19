import streamlit as st
import pandas as pd
import os

# Mock Data Loading (Replace with reading gold_export in real scenario if updated)
# unique_id,first_name,last_name,npi,address_line_1,city,state,zip_code,source_system,cluster_id,match_score
def load_data():
    path = os.path.join(os.path.dirname(__file__), "../../../data/output/gold_export.csv")
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        # Fallback for UI Dev
        return pd.DataFrame({
            "first_name": ["John", "Jon"],
            "last_name": ["Doe", "Doe"],
            "source_system": ["Oracle DB", "Snowflake"],
            "cluster_id": ["1", "1"],
            "confidence_score": [0.95, 0.95]
        })

def render():
    st.title("Match Review Center")
    
    # Initialize Session State using more specific keys to avoid conflicts
    if 'match_index' not in st.session_state:
        st.session_state.match_index = 0
    if 'matches_processed' not in st.session_state:
        st.session_state.matches_processed = 0

    # Load Data
    df = load_data()
    
    # Filter for Ambiguous Clusters (Mock logic: > 1 record)
    # In reality, you'd group by cluster_id and find clusters size > 1
    clusters = df.groupby('cluster_id').filter(lambda x: len(x) > 1)
    unique_clusters = clusters['cluster_id'].unique()
    
    total_matches = len(unique_clusters)
    
    if st.session_state.match_index >= total_matches:
        st.success("All matches reviewed! Great job.")
        if st.button("Reset Review Queue"):
            st.session_state.match_index = 0
            st.rerun()
        return

    # Get Current Cluster
    current_cluster_id = unique_clusters[st.session_state.match_index]
    records = clusters[clusters['cluster_id'] == current_cluster_id]
    
    # Progress Bar
    progress = (st.session_state.match_index + 1) / total_matches
    st.markdown(f"**Reviewing match {st.session_state.match_index + 1} of {total_matches}**")
    st.progress(progress)
    
    st.markdown("---")
    
    # --- CARD UI ---
    # Assuming standard pair (2 records) for demo. 
    # If > 2, logic allows scrolling or grid. Here we optimize for the "Tinder Pair" view.
    
    rec1 = records.iloc[0]
    rec2 = records.iloc[1] if len(records) > 1 else records.iloc[0] # Fallback
    
    # Layout
    c1, c_mid, c2 = st.columns([4, 2, 4])
    
    with c1:
        st.subheader(rec1.get('source_system', 'Source A'))
        st.caption("Source System")
        
        st.markdown(f"""
        <div style='background-color: #e3f2fd; padding: 15px; border-radius: 8px; border-left: 5px solid #2196f3;'>
            <h4 style='margin:0; color: #0d47a1'>{rec1.get('first_name', '')} {rec1.get('last_name', '')}</h4>
            <p><strong>NPI:</strong> {rec1.get('npi', 'N/A')}</p>
            <p><strong>Address:</strong> {rec1.get('address_line_1', '')}, {rec1.get('city', '')} {rec1.get('state', '')}</p>
            <p><strong>Tax ID:</strong> {rec1.get('tax_id', 'N/A')}</p>
        </div>
        """, unsafe_allow_html=True)
        st.caption(f"ID: {rec1.get('unique_id', 'N/A')}")

    with c_mid:
        st.markdown("<br><br>", unsafe_allow_html=True)
        score = int(float(rec1.get('confidence_score', 0.90)) * 100)
        
        # Color coding score
        color = "#ff9800" # Orange for medium
        if score > 90: color = "#4caf50" # Green
        if score < 70: color = "#f44336" # Red
        
        st.markdown(f"""
        <div style='text-align: center;'>
            <div style='
                width: 80px; height: 80px; 
                border-radius: 50%; 
                border: 5px solid {color};
                display: flex; align-items: center; justify-content: center;
                margin: auto;
                font-size: 24px; font-weight: bold; color: {color};
            '>
                {score}%
            </div>
            <p style='margin-top: 5px; font-weight: bold; color: {color}'>MATCH SCORE</p>
        </div>
        """, unsafe_allow_html=True)

    with c2:
        st.subheader(rec2.get('source_system', 'Source B'))
        st.caption("Source System") # In real app, name from config
        
        st.markdown(f"""
        <div style='background-color: #e3f2fd; padding: 15px; border-radius: 8px; border-left: 5px solid #2196f3;'>
            <h4 style='margin:0; color: #0d47a1'>{rec2.get('first_name', '')} {rec2.get('last_name', '')}</h4>
            <p><strong>NPI:</strong> {rec2.get('npi', 'N/A')}</p>
            <p><strong>Address:</strong> {rec2.get('address_line_1', '')}, {rec2.get('city', '')} {rec2.get('state', '')}</p>
            <p><strong>Tax ID:</strong> {rec1.get('tax_id', 'N/A')}</p>
        </div>
        """, unsafe_allow_html=True)
        st.caption(f"ID: {rec2.get('unique_id', 'N/A')}")

    st.markdown("<br><br>", unsafe_allow_html=True)
    
    # --- ACTION BUTTONS ---
    b1, b2, b3 = st.columns([1, 2, 1])
    
    # Logic: Just move index for now. In real app, write to DB.
    def next_match():
        st.session_state.match_index += 1
        # st.rerun() # Automated by button callback
        
    with b1:
        if st.button("Approve", use_container_width=True):
            st.toast("Match Approved!", icon="✅")
            time.sleep(0.5)
            next_match()
            st.rerun()
            
    # Centering the Reject button to match design slightly or keep side by side
    with b2:
        if st.button("Reject", type="primary", use_container_width=True):
             st.toast("Match Rejected", icon="🚫")
             time.sleep(0.5)
             next_match()
             st.rerun()
             
    with b3:
        if st.button("Skip", use_container_width=True):
            next_match()
            st.rerun()
            
import time
