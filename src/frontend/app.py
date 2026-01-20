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
logo_path = os.path.join(os.path.dirname(__file__), "assets/app_logo.png")
st.set_page_config(
    page_title="iCORE | iLink Digital",
    page_icon=logo_path,
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

# --- CUSTOM CSS (SUBTLE PREMIUM MESH) ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', system-ui, -apple-system, sans-serif;
    }

    /* 1. BACKGROUND: Enterprise Cool Slate Gradient */
    .stApp {
        /* Professional, Calm, Secure */
        background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 50%, #cbd5e1 100%);
        background-attachment: fixed;
    }

    /* GLOBAL COMPACTION */
    .block-container {
        padding-top: 2rem !important;
        padding-bottom: 1rem !important;
        max-width: 100% !important;
    }

    /* 2. GLASS CARD: Frosted & Refined */
    /* Target the content block inside the center column */
    div[data-testid="column"]:nth-of-type(2) [data-testid="stVerticalBlock"] {
        background: rgba(255, 255, 255, 0.70) !important;
        backdrop-filter: blur(24px);
        -webkit-backdrop-filter: blur(24px);
        
        border: 1px solid rgba(255, 255, 255, 0.5);
        border-top: 1px solid rgba(255, 255, 255, 0.8); /* Highlight top */
        
        /* Soft, Diffuse Shadow for Depth */
        box-shadow: 0 24px 48px -12px rgba(15, 23, 42, 0.15) !important;
        
        border-radius: 24px;
        padding: 24px !important; /* AGGRESSIVE COMPRACTION from 32px */
        gap: 12px; /* AGGRESSIVE COMPRACTION from 16px */
        
        /* RESPONSIVE CAPS */
        max-width: 420px; /* Reduced width slightly for better vertical ratio */
        width: 90%;
        margin: 0 auto;
    }

    /* SCROLLBAR REMOVAL ON LARGE SCREENS */
    @media (min-width: 1024px) {
        ::-webkit-scrollbar {
            display: none !important;
        }
        .stApp {
            overflow: hidden !important; /* Prevent scroll entirely if fitting */
        }
        /* Ensure specific elements don't force scroll */
        section.main {
            overflow: hidden !important;
        }
    }

    @media screen and (max-width: 600px) {
        div[data-testid="column"]:nth-of-type(2) [data-testid="stVerticalBlock"] {
            padding: 24px !important;
            width: 95%;
        }
    }

    /* 3. INPUTS: Enterprise Grade Labels */
    .stTextInput label {
        color: #1e293b !important;
        font-size: 11px !important;
        font-weight: 700 !important;
        text-transform: uppercase;
        letter-spacing: 1.2px;
        margin-bottom: 12px !important;
        display: block !important;
    }

    /* 3. INPUTS: Premium Text Boxes - Professional & Clean */
    .stTextInput > div > div[data-baseweb="base-input"],
    div[data-baseweb="base-input"] {
        background-color: #ffffff !important;
        border: 1px solid #e2e8f0 !important; /* Thin light border */
        border-radius: 8px !important;
        transition: border-color 0.15s ease-in-out, box-shadow 0.15s ease-in-out !important;
        overflow: hidden;
        box-shadow: none !important; /* Clean flat look by default */
    }
    
    /* Hover State */
    .stTextInput > div > div[data-baseweb="base-input"]:hover,
    div[data-baseweb="base-input"]:hover {
        border-color: #cbd5e1 !important; /* Slightly darker grey on hover */
    }
    
    /* Focus State - Strong Dark Highlight */
    .stTextInput > div > div[data-baseweb="base-input"]:focus-within,
    div[data-baseweb="base-input"]:focus-within {
        border-color: #0f172a !important; /* Dark Slate / Black */
        box-shadow: 0 0 0 1px #0f172a !important; /* Sharp outline */
        background-color: #ffffff !important;
    }

    /* Force transparency on internal input elements */
    div[data-baseweb="base-input"] > div,
    div[data-baseweb="base-input"] * {
        background-color: transparent !important;
        background: transparent !important;
        border: none !important;
        outline: none !important;
    }

    /* Input Text Styling - Clean & Crisp */
    .stTextInput input {
        color: #111827 !important;
        caret-color: #111827 !important;
        font-weight: 500 !important;
        font-size: 15px !important;
        padding: 12px 14px !important; /* Reduced padding to ensure space */
        padding-right: 40px !important; /* Specific right padding for eye icon */
        letter-spacing: 0.3px;
        background: transparent !important;
    }
    
    /* Placeholder Styling - Elegant & Subtle */
    .stTextInput input::placeholder {
        color: #9ca3af !important;
        font-weight: 400 !important;
        font-style: normal !important;
        opacity: 1 !important;
    }

    /* Hide "Press Enter" instruction */
    div[data-testid="InputInstructions"] { display: none !important; }
    
    /* Input Container Spacing - Compacted */
    .stTextInput {
        margin-bottom: 2px !important;
    }

    /* Eye Icon Polish - STACK OF WHITES FIX */
    /* Target the container that holds the eye icon - Force WHITE background to cover any leaks */
    /* Target the container that holds the eye icon */
    div[data-baseweb="base-input"] > div:last-child {
        background-color: transparent !important;
        border: none !important;
        box-shadow: none !important;
        margin-right: 0px !important;
        right: 0px !important;
        height: 100% !important;
        display: flex !important;
        align-items: center !important;
    }

    /* Target the button AND ALL ITS CHILDREN recursively */
    div[data-baseweb="base-input"] > div:last-child *,
    div[data-baseweb="base-input"] button,
    div[data-baseweb="base-input"] button * {
        background-color: transparent !important; /* Children stay transparent */
        border: none !important;
        box-shadow: none !important;
    }
    
    /* Ensure the button itself has no internal padding ruining the layout */
    div[data-baseweb="base-input"] button {
        padding-right: 12px !important; 
    }

    /* Force the SVG fill */
    div[data-baseweb="base-input"] button svg {
        fill: #64748b !important;
    }
    
    /* Hover effects for the button (Icon only) */
    div[data-baseweb="base-input"] button:hover {
        background-color: transparent !important;
        transform: scale(1.1);
        cursor: pointer;
    }
    
    div[data-baseweb="base-input"] button:hover svg {
        fill: #0f172a !important;
    }

    /* 4. BUTTON: PERFECT CENTER - Compact Margin */
    div.stButton {
        width: 100%; /* container takes full width */
        margin-top: 12px; /* REDUCED from 16px */
        display: flex;
        justify-content: center;
    }
    
    div.stButton > button {
        background: linear-gradient(180deg, #d11f41 0%, #be123c 100%);
        color: white;
        border: none;
        height: 48px;
        border-radius: 8px;
        font-weight: 600;
        font-size: 16px;
        box-shadow: 0 4px 6px -1px rgba(209, 31, 65, 0.25);
    }

    div.stButton > button:hover {
        background: linear-gradient(180deg, #be123c 0%, #9f1239 100%) !important;
        transform: translateY(-1px);
        box-shadow: 0 10px 15px -3px rgba(209, 31, 65, 0.35);
        color: white !important;
    }

    div.stButton > button:active {
        transform: translateY(1px);
        box-shadow: 0 2px 4px -1px rgba(209, 31, 65, 0.2);
    }

    /* Sidebar & header overrides */
    [data-testid="stSidebar"] {
        background-color: #0f172a;
        border-right: 1px solid #1e293b;
    }
    header[data-testid="stHeader"] { background: transparent; }

    /* Fix for placeholder padding issue as requested */
    .st-bz {
        padding-right: 0px !important;
    }

    /* Remove Streamlit Header Anchors (Link Icons) for cleaner UI */
    [data-testid="stHeaderActionElements"] {
        display: none !important;
    }
    
    /* Ensure no other anchor artifacts affect centering */
    h1 a, h2 a, h3 a {
        display: none !important;
    }
</style>

""", unsafe_allow_html=True)

import base64

def get_img_as_base64(file_path):
    try:
        with open(file_path, "rb") as f:
            data = f.read()
        return base64.b64encode(data).decode()
    except Exception:
        return ""

# --- LOGIN SCREEN ---
def login_page():
    logo_path = os.path.join(os.path.dirname(__file__), "assets/app_logo.png")
    ilink_logo_path = os.path.join(os.path.dirname(__file__), "assets/ilink_logo.png")
    
    logo_b64 = get_img_as_base64(logo_path)
    ilink_logo_b64 = get_img_as_base64(ilink_logo_path)

    # Columns: Card occupies middle. 
    # [1, 0.6, 1] works well for narrower card
    col1, col2, col3 = st.columns([1, 0.6, 1])

    with col2:
        # HEADER - STRICT FLEX ALIGNMENT - COMPACT
        st.markdown(f"""
        <div style='display: flex; flex-direction: column; align-items: center; justify-content: center; width: 100%; margin-bottom: 8px;'>
             <div style='margin-bottom: 4px; display: flex; justify-content: center; width: 100%;'>
                  <img src="data:image/png;base64,{logo_b64}" width="110" style="filter: drop-shadow(0 2px 4px rgba(0,0,0,0.1)); display: block; max-width: 100%; height: auto;"> 
             </div>
             <h1 style='color: #0f172a; font-size: clamp(20px, 5vw, 26px); font-weight: 800; margin: 0; padding: 0; width: 100%; letter-spacing: -0.5px; text-align: center;'>iCORE</h1>
             <p style='color: #475569; font-size: clamp(8px, 3vw, 10px); font-weight: 700; margin-top: 2px; letter-spacing: 1.2px; text-transform: uppercase; text-align: center; width: 100%;'>Central Operational Resolution Engine</p>
             <div style='width: 30px; height: 2px; background: #d11f41; margin: 4px auto; border-radius: 1px;'></div>
             <p style='color: #64748b; font-size: clamp(10px, 4vw, 12px); font-weight: 500; margin-bottom: 0; text-align: center; width: 100%;'>The Core of Business Truth</p>
        </div>
        """, unsafe_allow_html=True)
        
        # INPUTS
        username = st.text_input("Username", placeholder="Enter username")
        password = st.text_input("Password", type="password", placeholder="Enter password")
        
        # BUTTON
        # Custom CSS forces full width and layout
        if st.button("Sign In", use_container_width=True):
            if username in USERS and USERS[username]["pass"] == password:
                st.session_state['authenticated'] = True
                st.session_state['user_role'] = USERS[username]["role"]
                st.session_state['user_name'] = USERS[username]["name"]
                st.session_state['current_page'] = "Dashboard"
                st.rerun()
            else:
                st.error("Invalid credentials.")
        
        # FOOTER (Inside Card)
        st.markdown(f"""
        <div class="footer-text" style="display: flex; flex-direction: column; align-items: center; justify-content: center; width: 100%; margin-top: 12px;">
             <p style="margin-bottom: 4px; color: #475569; font-weight: 600; font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; text-align: center;">Enterprise Entity Resolution Platform</p>
             <div style="display: flex; align-items: center; justify-content: center; gap: 6px;">
                 <span style="font-size: 11px; color: #334155; font-weight: 700;">Powered by</span>
                 <img src="data:image/png;base64,{ilink_logo_b64}" height="18" style="filter: drop-shadow(0 1px 2px rgba(0,0,0,0.1));">
             </div>
        </div>
        """, unsafe_allow_html=True)


# --- SIDEBAR NAVIGATION ---
def sidebar_nav():
    with st.sidebar:
        # User Profile Section
        st.markdown(f"""
        <div style='padding-top: 20px; padding-bottom: 20px;'>
            <h3 style='margin:0'>iCORE</h3>
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
