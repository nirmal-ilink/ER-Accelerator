import streamlit as st
import pandas as pd
import os
import sys

# Add project root to path to allow imports if needed, though this runs standalone usually
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.frontend.views import steward_workbench

st.set_page_config(layout="wide", page_title="iLink ER Accelerator", page_icon="favicon.ico")

# --- Theme & Style ---
st.markdown("""
<style>
    .reportview-container { background: #f0f2f6; }
    h1 { color: #002b5c; } /* Navy Blue */
    .stButton>button { background-color: #002b5c; color: white; }
</style>
""", unsafe_allow_html=True)

# --- Session State (Mock Auth) ---
if 'user_role' not in st.session_state:
    st.session_state['user_role'] = 'Guest'

# --- Sidebar ---
st.sidebar.image("https://ilink-systems.com/wp-content/uploads/2021/05/ilink-logo-blue.png", width=150)
st.sidebar.title("Navigation")

menu_options = ["Home", "Executive Dashboard", "Steward Workbench", "Developer Console"]
selection = st.sidebar.radio("Go to", menu_options)

# --- Login Logic ---
with st.sidebar.expander("User Profile"):
    role = st.selectbox("Switch Role", ["Admin", "Steward", "Executive"])
    if st.button("Login"):
        st.session_state['user_role'] = role
        st.success(f"Logged in as {role}")

# --- Routing ---
if selection == "Home":
    st.title("Enterprise Entity Resolution Accelerator")
    st.markdown("""
    ### Welcome to the Platform
    
    This accelerator demonstrates the power of **PySpark**, **Delta Lake**, and **Streamlit** to solve complex MDM challenges.
    
    **Current Status:**
    - Backend: Running on Databricks 14.3 LTS
    - Frontend: Air-Gapped Mode (Reading `gold_export.csv`)
    """)
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Records Ingested", "10,000")
    col2.metric("Duplicates Resolved", "2,340")
    col3.metric("Data Quality Score", "94%")

elif selection == "Steward Workbench":
    steward_workbench.render()

elif selection == "Executive Dashboard":
    st.title("Executive ROI Dashboard")
    st.info("Coming soon in Phase 2...")
    
elif selection == "Developer Console":
    st.title("System Observability")
    st.code("tail -f sys_event_log", language="bash")
