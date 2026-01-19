import streamlit as st
import pandas as pd
import sys
import os
import time

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# Placeholder imports for views (we will create these next)
# from src.frontend.views import match_review, dashboard, connectors, inspector, audit, users

# --- CONFIGURATION ---
st.set_page_config(
    page_title="ER Accelerator | iLink Digital",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- AUTHENTICATION & PERMISSIONS ---
USERS = {
    "admin": {"pass": "admin123", "role": "Admin", "name": "Administrator"},
    "exec": {"pass": "exec123", "role": "Executive", "name": "C-Suite User"},
    "steward": {"pass": "steward123", "role": "Steward", "name": "Data Steward"},
    "dev": {"pass": "dev123", "role": "Developer", "name": "Tech Lead"}
}

# Permission Matrix
PERMISSIONS = {
    "Admin": ["Dashboard", "Connectors", "Pipeline Inspector", "Match Review", "Audit Logs", "User Management"],
    "Executive": ["Dashboard", "Audit Logs"],
    "Steward": ["Dashboard", "Match Review", "Audit Logs"],
    "Developer": ["Dashboard", "Connectors", "Pipeline Inspector", "Audit Logs"]
}

# --- SESSION STATE INITIALIZATION ---
if 'authenticated' not in st.session_state:
    st.session_state['authenticated'] = False
if 'user_role' not in st.session_state:
    st.session_state['user_role'] = None
if 'user_name' not in st.session_state:
    st.session_state['user_name'] = None
if 'current_page' not in st.session_state:
    st.session_state['current_page'] = "Dashboard"

# --- CUSTOM CSS (NAVY THEME & ANIMATIONS) ---
st.markdown("""
<style>
    /* 1. ANIMATED BACKGROUND (SUBTLE & PROFESSIONAL) */
    @keyframes gradientBG {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    
    .stApp {
        /* Very subtle, translucent-like professional gradient */
        background: linear-gradient(-45deg, #f0f2f6, #e6eaef, #dbe4f0, #f0f2f6);
        background-size: 400% 400%;
        animation: gradientBG 30s ease infinite;
        font-family: 'Inter', sans-serif;
    }
    
    /* 2. LOGIN CARD (Targeting the Middle Column) */
    /* This selects the wrapper of the second column to style it as a card */
    [data-testid="stColumn"]:nth-of-type(2) > div > div > div {
        background-color: rgba(255, 255, 255, 0.95);
        padding: 40px;
        border-radius: 16px;
        box-shadow: 0 10px 40px rgba(0,0,0,0.08); /* Softer shadow */
        border: 1px solid rgba(255,255,255,0.5);
        backdrop-filter: blur(10px); /* Translucent glass effect */
    }

    /* 3. INPUT FIELDS */
    .stTextInput > div > div > input {
        border: 1px solid #e0e0e0;
        background-color: #f8f9fa; /* Slight off-white bg for inputs */
        border-radius: 8px;
        padding: 12px;
        font-size: 15px;
        color: #333;
        transition: all 0.3s ease;
    }
    .stTextInput > div > div > input:focus {
        border-color: #d11f41;
        background-color: #fff;
        box-shadow: 0 0 0 3px rgba(209, 31, 65, 0.1);
    }
    
    /* Label Styling */
    .stTextInput > label, .stTextInput label p {
        font-size: 14px;
        color: #555;
        font-weight: 500;
        margin-bottom: 4px;
    }
    
    /* 4. BUTTON STYLING */
    .stButton > button {
        background: #0b1c32; /* Navy Base */
        border: none;
        color: white;
        padding: 14px 24px;
        text-align: center;
        text-decoration: none;
        display: inline-block;
        font-size: 16px;
        margin-top: 10px;
        cursor: pointer;
        border-radius: 8px;
        width: 100%;
        font-weight: 600;
        box-shadow: 0 4px 6px rgba(11, 28, 50, 0.2);
        transition: all 0.2s;
    }
    .stButton > button:hover {
        background: #d11f41; /* iLink Red on Hover */
        transform: translateY(-2px);
        box-shadow: 0 6px 12px rgba(209, 31, 65, 0.3);
        color: white !important;
    }
    .stButton > button:active {
        transform: translateY(0px);
    }

    /* 5. SIDEBAR OVERRIDE */
    [data-testid="stSidebar"] {
        background-color: #0b1c32;
        border-right: 1px solid #1a2b42;
    }
    
    /* Hide the top header decoration */
    header[data-testid="stHeader"] {
        background: transparent;
    }
</style>
""", unsafe_allow_html=True)

import base64

def get_img_as_base64(file_path):
    """Reads an image file and converts it to a base64 string for HTML embedding."""
    try:
        with open(file_path, "rb") as f:
            data = f.read()
        return base64.b64encode(data).decode()
    except Exception:
        return ""

# --- LOGIN SCREEN ---
def login_page():
    # Centering Layout: Use a wider central column ratio to fit the wide card comfortably
    col1, col2, col3 = st.columns([1, 1.2, 1])
    
    # Load Logical Assets
    logo_path = os.path.join(os.path.dirname(__file__), "assets/app_logo.png")
    powered_by_path = os.path.join(os.path.dirname(__file__), "assets/ilink_logo.png")
    
    logo_b64 = get_img_as_base64(logo_path)
    powered_by_b64 = get_img_as_base64(powered_by_path)

    with col2:
        # Spacer
        st.markdown("<br><br><br>", unsafe_allow_html=True)
        
        # LOGO & HEADER (Inside the column, which is now styled as the card)
        st.markdown(f"""
        <div style='text-align: center; margin-bottom: 30px;'>
            <div style='display: flex; justify-content: center; margin-bottom: 20px;'>
                 <img src="data:image/png;base64,{logo_b64}" width="180"> 
            </div>
            <h2 style='color: #0b1c32; font-family: "Inter", sans-serif; margin-bottom: 5px; font-weight: 700;'>ER Accelerator</h2>
            <p style='color: #666; font-size: 14px; letter-spacing: 0.5px;'>Enterprise Master Data Management</p>
            <hr style='width: 40px; height: 3px; background-color: #d11f41; border: none; margin: 20px auto 30px auto; border-radius: 2px;'>
        </div>
        """, unsafe_allow_html=True)
        
        # INPUTS (Now naturally inside the card due to CSS targeting)
        username = st.text_input("Username", 
                               placeholder="Enter your username", 
                               label_visibility="collapsed") # Hiding label for cleaner look if desired, or "visible"
        
        st.markdown("<div style='margin-bottom: 15px;'></div>", unsafe_allow_html=True) # Spacer
        
        password = st.text_input("Password", 
                               type="password", 
                               placeholder="Enter your password", 
                               label_visibility="collapsed")
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        login_submitted = st.button("Sign In")
        
        if login_submitted:
            if username in USERS and USERS[username]["pass"] == password:
                st.session_state['authenticated'] = True
                st.session_state['user_role'] = USERS[username]["role"]
                st.session_state['user_name'] = USERS[username]["name"]
                st.session_state['current_page'] = "Dashboard"
                st.rerun()
            else:
                st.error("Invalid credentials.")
                
        # FOOTER
        st.markdown(f"""
        <div style='text-align: center; margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee;'>
             <p style='font-size: 12px; color: #999; margin: 0; margin-bottom: 5px;'>Powered by</p>
             <img src="data:image/png;base64,{powered_by_b64}" width="100">
        </div>
        """, unsafe_allow_html=True)


# --- SIDEBAR NAVIGATION ---
def sidebar_nav():
    with st.sidebar:
        # User Profile Section
        st.markdown(f"""
        <div style='padding-top: 20px; padding-bottom: 20px;'>
            <h3 style='margin:0'>ER Accelerator</h3>
            <p style='font-size: 0.8em; opacity: 0.7;'>Enterprise MDM</p>
            <div style='margin-top: 20px; padding-left: 10px; border-left: 3px solid #d11f41;'>
                <p style='font-size: 0.7em; margin:0; opacity: 0.7;'>SIGNED IN AS</p>
                <p style='font-weight: bold; margin:0; font-size: 1.1em;'>{st.session_state['user_name']}</p>
                <p style='font-size: 0.8em; margin:0; opacity: 0.9;'>{st.session_state['user_role'].upper()}</p>
            </div>
        </div>
        <hr style='border-color: #2c3e50;'>
        """, unsafe_allow_html=True)

        # Dynamic Menu Generation
        role = st.session_state['user_role']
        allowed_pages = PERMISSIONS.get(role, [])
        
        for page in allowed_pages:
            # Simple Highlight Logic
            is_active = st.session_state['current_page'] == page
            button_style = "background-color: #1a2b42;" if is_active else "background-color: transparent;"
            
            if st.button(page, key=f"nav_{page}", use_container_width=True):
                st.session_state['current_page'] = page
                st.rerun()

        st.markdown("<br><br><br>", unsafe_allow_html=True)
        if st.button("Sign Out"):
            st.session_state['authenticated'] = False
            st.session_state['user_role'] = None
            st.rerun()

# --- MAIN ROUTER ---
if not st.session_state['authenticated']:
    login_page()
else:
    sidebar_nav()
    page = st.session_state['current_page']
    
    # Import logic inline to avoid circular imports and load only what's needed
    if page == "Match Review":
        from src.frontend.views import match_review
        match_review.render()
    elif page == "Dashboard":
        from src.frontend.views import dashboard
        dashboard.render()
    else:
        st.title(page)
        st.info(f"{page} module is currently under development or maintenance.")
