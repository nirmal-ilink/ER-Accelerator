import streamlit as st
import pandas as pd
import os

def render():
    st.title("Stewardship Workbench")
    st.markdown("Review and resolve ambiguous entity matches.")

    # Load Data (Air-Gap Simulation)
    # Ideally this path is config-driven
    DATA_PATH = "./data/output/gold_export.csv"
    
    if not os.path.exists(DATA_PATH):
        st.warning(f"No Gold Export found at {DATA_PATH}. Please run the Backend Pipeline first.")
        # Create dummy data for visualization if file missing
        data = {
            'cluster_id': ['C101', 'C101', 'C102', 'C102'],
            'first_name': ['John', 'Jon', 'Sarah', 'Sara'],
            'last_name': ['Doe', 'Doe', 'Smith', 'Smit'],
            'trust_score': [0.9, 0.5, 0.8, 0.6],
            '_source_system': ['NPI_Registry', 'EMR', 'NPI_Registry', 'EMR']
        }
        df = pd.DataFrame(data)
    else:
        df = pd.read_csv(DATA_PATH)

    # Filter Logic
    st.subheader("Ambiguous Clusters Queue")
    
    # Simulate identifying clusters with variance
    # In real app, we'd query the 'manual_review' table
    unique_clusters = df['cluster_id'].unique()
    selected_cluster = st.selectbox("Select Cluster to Review", unique_clusters)
    
    cluster_df = df[df['cluster_id'] == selected_cluster]
    
    st.write("### Cluster Details")
    st.dataframe(cluster_df)
    
    # Diff View
    st.write("---")
    st.subheader("Compare & Merge")
    
    col_left, col_right = st.columns(2)
    
    if len(cluster_df) >= 2:
        rec_a = cluster_df.iloc[0]
        rec_b = cluster_df.iloc[1]
        
        with col_left:
            st.info(f"Record A ({rec_a['_source_system']})")
            st.json(rec_a.to_dict())
            
        with col_right:
            st.warning(f"Record B ({rec_b['_source_system']})")
            st.json(rec_b.to_dict())
            
        st.button("Merge Records", type="primary")
        st.button("Reject Match", type="secondary")
    else:
        st.success("This cluster contains a single Golden Record. No Action Needed.")
