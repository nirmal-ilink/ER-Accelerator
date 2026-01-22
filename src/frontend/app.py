import streamlit as st
import pandas as pd
import sys
import os
import time

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# Placeholder imports for views (we will create these next)
# from src.frontend.views import match_review, dashboard, connectors, inspector, audit, users
from src.backend.audit.logger import AuditLogger

# Initialize Logger
audit_log = AuditLogger()

# --- CONFIGURATION ---
logo_path = os.path.join(os.path.dirname(__file__), "assets/app_logo.png")
# Force Reload Fix for CSS
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
# Determine Theme Mode based on Authentication
is_auth = st.session_state.get('authenticated', False)

# Dynamic CSS Variables
if not is_auth:
    # LOGIN MODE: Pearlescent Gradient Background
    main_bg_css = """
    .stApp {
        background: linear-gradient(-45deg, #ffffff, #f1f5f9, #fee2e2, #e2e8f0, #dbeafe, #ffffff);
        background-size: 400% 400%;
        animation: gradientBG 12s ease infinite;
        background-attachment: fixed;
    }
    
    /* LOGIN BUTTON STYLES (SCOPED) */
    .stApp > header + div > div > div > div:nth-child(2) div.stButton {
        width: 100%;
        margin-top: 24px;
        display: flex;
        justify-content: center;
    }
    
    div[data-testid="stColumn"]:nth-of-type(2) div[data-testid="stButton"] button {
        background-color: #d11f41 !important;
        color: white !important;
        border: none !important;
        height: 48px !important;
        border-radius: 24px !important;
        font-weight: 600 !important;
        font-size: 16px !important;
        box-shadow: 0 4px 6px -1px rgba(209, 31, 65, 0.25) !important;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
        width: 200px !important;
        margin-left: auto !important;
        margin-right: auto !important;
        display: block !important;
    }

    div[data-testid="stColumn"]:nth-of-type(2) div[data-testid="stButton"] button:hover {
        background-color: #9f1239 !important;
        box-shadow: 0 10px 15px -3px rgba(159, 18, 57, 0.4) !important;
        transform: translateY(-2px) scale(1.02);
        color: white !important;
    }
    """
    sidebar_bg = "#0f172a"  # Fallback (Login usually has no sidebar)
else:
    # DASHBOARD MODE: Pure White Background & Light Elegant Sidebar
    main_bg_css = """
    .stApp {
        background-color: #F8FAFC !important;
        background-image: none !important;
        color: #0f172a !important; /* Force Dark Text */
    }
    /* Force Streamlit classes to use dark text if they rely on theme */
    .stMarkdown, .stText, h1, h2, h3, h4, h5, h6, .stDataFrame {
        color: #0f172a !important;
    }
    """
    sidebar_bg = "#f3f4f6" # Gray 100 for better contrast against white content

st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

    html, body, [class*="css"] {{
        font-family: 'Inter', system-ui, -apple-system, sans-serif;
    }}

    /* 1. BACKGROUND: Dynamic or White based on State */
    {main_bg_css}
    
    @keyframes gradientBG {{
        0% {{ background-position: 0% 50%; }}
        50% {{ background-position: 100% 50%; }}
        100% {{ background-position: 0% 50%; }}
    }}

    /* GLOBAL COMPACTION */
    .block-container {{
        padding-top: 5vh !important;
        padding-bottom: 2rem !important;
        max-width: 100% !important;
    }}

    /* SCROLLBAR REMOVAL ON LARGE SCREENS */
    @media (min-width: 1024px) {{
        ::-webkit-scrollbar {{
            display: none !important;
        }}
        .stApp {{
            overflow: hidden !important;
        }}
        section.main {{
            overflow: hidden !important;
        }}
    }}

    /* 3. INPUTS: Enterprise Grade Labels */
    .stTextInput label {{
        color: #475569 !important;
        font-size: 12px !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        letter-spacing: 1.0px;
        margin-bottom: 8px !important;
        display: block !important;
    }}

    /* 3. INPUTS: Premium Text Boxes */
    .stTextInput > div > div[data-baseweb="base-input"],
    div[data-baseweb="base-input"] {{
        background-color: #ffffff !important;
        border: 1px solid #e2e8f0 !important;
        border-radius: 8px !important;
        transition: border-color 0.15s ease-in-out, box-shadow 0.15s ease-in-out !important;
        overflow: hidden;
        box-shadow: none !important;
    }}
    
    .stTextInput > div > div[data-baseweb="base-input"]:hover,
    div[data-baseweb="base-input"]:hover {{
        border-color: #cbd5e1 !important;
    }}
    
    .stTextInput > div > div[data-baseweb="base-input"]:focus-within,
    div[data-baseweb="base-input"]:focus-within {{
        border-color: #0f172a !important;
        box-shadow: 0 0 0 1px #0f172a !important;
        background-color: #ffffff !important;
    }}

    div[data-baseweb="base-input"] > div,
    div[data-baseweb="base-input"] * {{
        background-color: transparent !important;
        background: transparent !important;
        border: none !important;
        outline: none !important;
    }}

    .stTextInput input {{
        color: #111827 !important;
        caret-color: #111827 !important;
        font-weight: 500 !important;
        font-size: 15px !important;
        padding: 12px 14px !important;
        padding-right: 40px !important;
        letter-spacing: 0.3px;
        background: transparent !important;
    }}
    
    .stTextInput input::placeholder {{
        color: #9ca3af !important;
        font-weight: 400 !important;
        font-style: normal !important;
        opacity: 1 !important;
    }}

    div[data-testid="InputInstructions"] {{ display: none !important; }}
    
    .stTextInput {{
        margin-bottom: 16px !important;
    }}

    /* Eye Icon Polish */
    div[data-baseweb="base-input"] > div:last-child {{
        background-color: transparent !important;
        border: none !important;
        box-shadow: none !important;
        margin-right: 0px !important;
        right: 0px !important;
        height: 100% !important;
        display: flex !important;
        align-items: center !important;
    }}

    div[data-baseweb="base-input"] > div:last-child *,
    div[data-baseweb="base-input"] button,
    div[data-baseweb="base-input"] button * {{
        background-color: transparent !important;
        border: none !important;
        box-shadow: none !important;
    }}
    
    div[data-baseweb="base-input"] button {{
        padding-right: 12px !important; 
    }}

    div[data-baseweb="base-input"] button svg {{
        fill: #64748b !important;
    }}
    
    div[data-baseweb="base-input"] button:hover {{
        background-color: transparent !important;
        transform: scale(1.1);
        cursor: pointer;
    }}
    
    div[data-baseweb="base-input"] button:hover svg {{
        fill: #0f172a !important;
    }}
    {main_bg_css}
    
    @keyframes gradientBG {{
        0% {{ background-position: 0% 50%; }}
        50% {{ background-position: 100% 50%; }}
        100% {{ background-position: 0% 50%; }}
    }}

    /* GLOBAL COMPACTION */
    .block-container {{
        padding-top: 5vh !important;
        padding-bottom: 2rem !important;
        max-width: 100% !important;
    }}


    /* 5. INNER CONTENT: Constrained Width (Moved to Login Page) */

    /* 5. INNER CONTENT: Constrained Width */
    /* Width constraints removed from global scope */

    /* SCROLLBAR REMOVAL ON LARGE SCREENS */
    @media (min-width: 1024px) {{
        ::-webkit-scrollbar {{
            display: none !important;
        }}
        .stApp {{
            overflow: hidden !important;
        }}
        section.main {{
            overflow: hidden !important;
        }}
    }}



    /* 3. INPUTS: Enterprise Grade Labels */
    .stTextInput label {{
        color: #475569 !important;
        font-size: 12px !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        letter-spacing: 1.0px;
        margin-bottom: 8px !important;
        display: block !important;
    }}

    /* 3. INPUTS: Premium Text Boxes */
    .stTextInput > div > div[data-baseweb="base-input"],
    div[data-baseweb="base-input"] {{
        background-color: #ffffff !important;
        border: 1px solid #e2e8f0 !important;
        border-radius: 8px !important;
        transition: border-color 0.15s ease-in-out, box-shadow 0.15s ease-in-out !important;
        overflow: hidden;
        box-shadow: none !important;
    }}
    
    .stTextInput > div > div[data-baseweb="base-input"]:hover,
    div[data-baseweb="base-input"]:hover {{
        border-color: #cbd5e1 !important;
    }}
    
    .stTextInput > div > div[data-baseweb="base-input"]:focus-within,
    div[data-baseweb="base-input"]:focus-within {{
        border-color: #0f172a !important;
        box-shadow: 0 0 0 1px #0f172a !important;
        background-color: #ffffff !important;
    }}

    div[data-baseweb="base-input"] > div,
    div[data-baseweb="base-input"] * {{
        background-color: transparent !important;
        background: transparent !important;
        border: none !important;
        outline: none !important;
    }}

    .stTextInput input {{
        color: #111827 !important;
        caret-color: #111827 !important;
        font-weight: 500 !important;
        font-size: 15px !important;
        padding: 12px 14px !important;
        padding-right: 40px !important;
        letter-spacing: 0.3px;
        background: transparent !important;
    }}
    
    .stTextInput input::placeholder {{
        color: #9ca3af !important;
        font-weight: 400 !important;
        font-style: normal !important;
        opacity: 1 !important;
    }}

    div[data-testid="InputInstructions"] {{ display: none !important; }}
    
    .stTextInput {{
        margin-bottom: 16px !important;
    }}

    /* Eye Icon Polish */
    div[data-baseweb="base-input"] > div:last-child {{
        background-color: transparent !important;
        border: none !important;
        box-shadow: none !important;
        margin-right: 0px !important;
        right: 0px !important;
        height: 100% !important;
        display: flex !important;
        align-items: center !important;
    }}

    div[data-baseweb="base-input"] > div:last-child *,
    div[data-baseweb="base-input"] button,
    div[data-baseweb="base-input"] button * {{
        background-color: transparent !important;
        border: none !important;
        box-shadow: none !important;
    }}
    
    div[data-baseweb="base-input"] button {{
        padding-right: 12px !important; 
    }}

    div[data-baseweb="base-input"] button svg {{
        fill: #64748b !important;
    }}
    
    div[data-baseweb="base-input"] button:hover {{
        background-color: transparent !important;
        transform: scale(1.1);
        cursor: pointer;
    }}
    
    div[data-baseweb="base-input"] button:hover svg {{
        fill: #0f172a !important;
    }}



    /* Fix specific streamlit class padding */
    .st-bz {{
        padding-right: 0px !important;
    }}

    /* Global Header Overrides */
    header[data-testid="stHeader"] {{ background: transparent; }}
    [data-testid="stHeaderActionElements"] {{ display: none !important; }}
    h1 a, h2 a, h3 a {{ display: none !important; }}
    
    /* Main Content Area - Dynamic stretch when sidebar toggles */
    [data-testid="stMain"],
    [data-testid="stAppViewContainer"],
    .main,
    .block-container {{
        transition: margin-left 0.3s cubic-bezier(0.4, 0, 0.2, 1), 
                    padding-left 0.3s cubic-bezier(0.4, 0, 0.2, 1), 
                    width 0.3s cubic-bezier(0.4, 0, 0.2, 1),
                    max-width 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
    }}
    
    .stApp > div {{
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
    }}
    
    /* Sidebar itself should also animate */
    section[data-testid="stSidebar"] {{
        transition: width 0.3s cubic-bezier(0.4, 0, 0.2, 1),
                    min-width 0.3s cubic-bezier(0.4, 0, 0.2, 1),
                    transform 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
    }}
    
    /* DYNAMIC MAIN CONTENT EXPANSION WHEN SIDEBAR IS COLLAPSED */
    /* When sidebar is collapsed (aria-expanded="false"), collapse sidebar width and expand main content */
    
    /* FORCE SIDEBAR TO 0 WIDTH WHEN COLLAPSED */
    section[data-testid="stSidebar"][aria-expanded="false"] {{
        width: 0px !important;
        min-width: 0px !important;
        max-width: 0px !important;
        padding: 0 !important;
        margin: 0 !important;
        overflow: hidden !important;
        flex-shrink: 1 !important;
        transform: translateX(-100%) !important;
    }}
    
    section[data-testid="stSidebar"][aria-expanded="false"] > div {{
        width: 0px !important;
        min-width: 0px !important;
        padding: 0 !important;
        overflow: hidden !important;
    }}
    
    /* Target using parent container with :has() - for main content expansion */
    [data-testid="stAppViewContainer"]:has(section[data-testid="stSidebar"][aria-expanded="false"]) [data-testid="stMain"] {{
        margin-left: 0 !important;
        width: 100% !important;
        max-width: 100% !important;
    }}
    
    [data-testid="stAppViewContainer"]:has(section[data-testid="stSidebar"][aria-expanded="false"]) .stMainBlockContainer {{
        margin-left: auto !important;
        margin-right: auto !important;
        max-width: 100% !important;
        padding-left: 4rem !important;
        padding-right: 2rem !important;
    }}
    
    /* Also target the .stApp level for broader compatibility */
    .stApp:has(section[data-testid="stSidebar"][aria-expanded="false"]) [data-testid="stMain"] {{
        margin-left: 0 !important;
        width: 100% !important;
        max-width: 100% !important;
    }}
    
    .stApp:has(section[data-testid="stSidebar"][aria-expanded="false"]) .block-container {{
        margin-left: auto !important;
        margin-right: auto !important;
        max-width: 100% !important;
        padding-left: 4rem !important;
        padding-right: 2rem !important;
    }}
    
    /* Additional targeting for the main content wrapper */
    .stApp:has(section[data-testid="stSidebar"][aria-expanded="false"]) .st-emotion-cache-6px8kg {{
        width: 100% !important;
        max-width: 100% !important;
        margin-left: 0 !important;
    }}
    
    /* ---------------------------------------------------------
       SIDEBAR REVAMP - PREMIUM & ELEGANT
       --------------------------------------------------------- */
       
    /* Sidebar Container */
    section[data-testid="stSidebar"] {{
        background-color: {sidebar_bg} !important; /* Light Grey */
        border-right: 1px solid #e2e8f0 !important;
        width: 240px !important;
        min-width: 240px !important;
        max-width: 240px !important;
    }}

    section[data-testid="stSidebar"] > div {{
        padding-top: 0.5rem !important;
        padding-left: 1rem !important;
    }}
    
    section[data-testid="stSidebar"] .block-container {{
        padding-top: 1rem !important;
        padding-bottom: 0rem !important;
        display: flex;
        flex-direction: column;
        height: 100%; 
    }}
    
    /* Ensure the main scrollable area takes full height for the flex column to work */
    div[data-testid="stSidebarUserContent"] {{
        height: 100%;
        display: flex;
        flex-direction: column;
    }}

    /* Ensures NO scrollbar in sidebar but allows scrolling */
    section[data-testid="stSidebar"] * {{
        -ms-overflow-style: none !important;  /* IE and Edge */
        scrollbar-width: none !important;  /* Firefox */
    }}
    
    section[data-testid="stSidebar"] ::-webkit-scrollbar {{
        display: none !important;
    }}
    
    /* Strict Compaction for Sidebar Items to fit without scrolling */
    section[data-testid="stSidebar"] .block-container {{
        padding-top: 1rem !important;
        padding-bottom: 0rem !important;
        gap: 0px !important; /* Remove gap between main blocks */
    }}

    /* Buttons inside Sidebar (Navigation Items) - SUBTLE & ELEGANT */
    section[data-testid="stSidebar"] .stButton > button {{
        width: 100% !important;
        margin: 0 auto !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        text-align: center !important;
        background-color: transparent !important;
        color: #475569 !important; /* Slate 600 */
        border: none !important;
        border-radius: 10px !important;
        font-size: 14px !important;
        font-weight: 500 !important;
        padding: 0.65rem 1rem !important;
        margin-bottom: 6px !important;
        box-shadow: none !important;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
        line-height: 1.4 !important;
        position: relative !important;
    }}

    /* Hover State for Sidebar Buttons - SUBTLE GLOW */
    section[data-testid="stSidebar"] .stButton > button:hover {{
        background-color: rgba(209, 31, 65, 0.06) !important; /* Very light red tint */
        color: #1e293b !important; /* Darker slate */
        box-shadow: 0 0 0 1px rgba(209, 31, 65, 0.1) !important; /* Subtle outline */
        transform: scale(1.02) !important;
    }}

    /* Active State (Primary Buttons in Sidebar) - ELEGANT HIGHLIGHT */
    section[data-testid="stSidebar"] .stButton > button[kind="primary"] {{
        background-color: #ffffff !important;
        color: #d11f41 !important; /* Brand Red */
        font-weight: 600 !important;
        box-shadow: 0 2px 8px rgba(209, 31, 65, 0.12), 
                    0 0 0 1px rgba(209, 31, 65, 0.15) !important;
        border: none !important;
    }}
    
    section[data-testid="stSidebar"] .stButton > button[kind="primary"]:hover {{
        box-shadow: 0 4px 12px rgba(209, 31, 65, 0.18), 
                    0 0 0 1px rgba(209, 31, 65, 0.2) !important;
        transform: scale(1.02) !important;
    }}

    /* Sidebar Toggle Button (Collapsed Control - The arrow to OPEN sidebar) */
    /* FORCE VISIBILITY AT ALL TIMES - NO HOVER NEEDED */
    [data-testid="stSidebarCollapsedControl"],
    [data-testid="stExpandSidebarButton"],
    button[data-testid="stExpandSidebarButton"] {{
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        position: fixed !important; /* Pin to viewport */
        top: 60px !important; /* Below standard header height if any */
        left: 12px !important; /* Bit more left as requested */
        z-index: 9999999 !important; /* Max z-index */
        background-color: #ffffff !important;
        color: #0f172a !important; /* Dark Icon */
        border: 2px solid #cbd5e1 !important; /* Thicker Border */
        border-radius: 8px !important;
        height: 40px !important;
        width: 40px !important;
        box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1), 0 2px 4px -1px rgba(0,0,0,0.06) !important;
        transition: all 0.2s ease !important;
        opacity: 1 !important; /* Force opacity */
        visibility: visible !important;
    }}
    
    /* Ensure any animation containers don't hide it */
    [data-testid="stSidebarCollapsedControl"] > div,
    [data-testid="stExpandSidebarButton"] > div {{
        display: flex !important;
        visibility: visible !important;
    }}

    /* Override Streamlit's default behavior that might hide it */
    section[data-testid="stSidebar"] > [data-testid="stSidebarCollapsedControl"] {{
        opacity: 1 !important;
        display: flex !important;
        visibility: visible !important;
    }}
    
    /* When Sidebar is closed, ensuring the button remains visible */
    [data-testid="stSidebarCollapsedControl"] > *,
    [data-testid="stExpandSidebarButton"] > * {{
        opacity: 1 !important;
        visibility: visible !important;
        color: #0f172a !important;
        fill: #0f172a !important;
    }}

    [data-testid="stSidebarCollapsedControl"] svg,
    [data-testid="stExpandSidebarButton"] svg,
    [data-testid="stSidebarCollapsedControl"] img,
    [data-testid="stExpandSidebarButton"] img,
    [data-testid="stSidebarCollapsedControl"] span,
    [data-testid="stExpandSidebarButton"] span {{
        opacity: 1 !important;
        visibility: visible !important;
        fill: #0f172a !important;
        color: #0f172a !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
    }}
    
    /* Target internal paths/icons specifically */
    [data-testid="stSidebarCollapsedControl"] svg path,
    [data-testid="stExpandSidebarButton"] svg path,
    [data-testid="stIconMaterial"] {{
        fill: #0f172a !important;
        color: #0f172a !important;
        opacity: 1 !important;
    }}
    
    /* Ensure the icon has a size */
    [data-testid="stSidebarCollapsedControl"] svg,
    [data-testid="stExpandSidebarButton"] svg {{
        width: 20px !important;
        height: 20px !important;
    }}
    
    /* Specific Material Icon Sizing */
    span[data-testid="stIconMaterial"] {{
        font-size: 20px !important;
        line-height: 1 !important;
    }}

    /* HOVER EFFECTS - All Toggle Buttons */
    [data-testid="stSidebarCollapsedControl"]:hover,
    [data-testid="stExpandSidebarButton"]:hover,
    div[data-testid="stSidebarCollapseButton"] > button:hover {{
        background-color: #f8fafc !important;
        border-color: #d11f41 !important; /* Brand Red Border */
        color: #d11f41 !important; /* Brand Red Icon */
        transform: scale(1.05) !important;
        box-shadow: 0 10px 15px -3px rgba(0,0,0,0.1) !important;
    }}
    
    [data-testid="stSidebarCollapsedControl"]:hover span,
    [data-testid="stExpandSidebarButton"]:hover span,
    div[data-testid="stSidebarCollapseButton"] > button:hover span,
    [data-testid="stSidebarCollapsedControl"]:hover svg,
    [data-testid="stExpandSidebarButton"]:hover svg,
    div[data-testid="stSidebarCollapseButton"] > button:hover svg {{
        fill: #d11f41 !important;
        color: #d11f41 !important;
    }}
    
    [data-testid="stSidebarCollapsedControl"]:hover svg path,
    [data-testid="stExpandSidebarButton"]:hover svg path,
    div[data-testid="stSidebarCollapseButton"] > button:hover svg path {{
        fill: #d11f41 !important;
    }}
    
    /* ACTIVE/CLICK EFFECTS - All Toggle Buttons */
    [data-testid="stSidebarCollapsedControl"]:active,
    [data-testid="stExpandSidebarButton"]:active,
    div[data-testid="stSidebarCollapseButton"] > button:active {{
        transform: scale(0.95) !important;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05) !important;
    }}
    
    /* Force Icon Visibility - All Toggle Buttons */
    [data-testid="stSidebarCollapsedControl"] svg,
    [data-testid="stExpandSidebarButton"] svg,
    div[data-testid="stSidebarCollapseButton"] > button svg {{
        fill: #0f172a !important;
        color: #0f172a !important;
        width: 20px !important;
        height: 20px !important;
        opacity: 1 !important;
        display: block !important;
        visibility: visible !important;
    }}

    /* Sidebar Collapse Button (Inside Header - Double Arrow <<) */
    /* Same styling as expand button for consistency */
    div[data-testid="stSidebarCollapseButton"] > button {{
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        color: #0f172a !important; /* Dark Icon */
        background-color: #ffffff !important; /* White BG */
        border-radius: 8px !important;
        border: 2px solid #cbd5e1 !important; /* Same border as expand */
        height: 40px !important; /* Same size as expand */
        width: 40px !important;
        padding: 0 !important;
        box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1), 0 2px 4px -1px rgba(0,0,0,0.06) !important;
        transition: all 0.2s ease !important;
        opacity: 1 !important;
        visibility: visible !important;
    }}
    
    /* Ensure icon colors in collapse button */
    div[data-testid="stSidebarCollapseButton"] > button span {{
        color: #0f172a !important;
        opacity: 1 !important;
        visibility: visible !important;
    }}
    
    /* Active state already defined above */
    
    /* Specific Icon Styling for the Close Button */
    div[data-testid="stSidebarCollapseButton"] button span,
    div[data-testid="stSidebarCollapseButton"] button svg,
    [data-testid="stSidebarUserContent"] .stButton > button[kind="header"] svg {{
        fill: currentColor !important;
        color: currentColor !important;
        width: 18px !important;
        height: 18px !important;
        opacity: 1 !important;
    }}
    
    /* Restore Header Space (Compact) */
    div[data-testid="stSidebarHeader"] {{
        padding-top: 16px !important;
        padding-bottom: 0px !important; /* Collapsed */
        margin-bottom: 0px !important;
        height: auto !important;
    }}
    
    div[data-testid="stSidebarHeader"] > div {{
       display: flex !important;
       align-items: center !important;
    }}
    
    .st-ed {{
        padding-right: 0px !important;
    }}



    /* ---------------------------------------------------------
       SIGN OUT BUTTON - ISOLATED & RED
       --------------------------------------------------------- */
       
    /* Target the LAST button in the sidebar (which is Sign Out) */
    /* We use specificity to override the general sidebar button styles */
    section[data-testid="stSidebar"] div[data-testid="stVerticalBlock"] > div:last-child div.stButton > button,
    section[data-testid="stSidebar"] div[data-testid="stVerticalBlock"] > div:last-of-type div.stButton > button {{
        background-color: #d11f41 !important; /* Brand Red */
        background: #d11f41 !important;
        color: #ffffff !important;
        border: none !important;
        border-radius: 24px !important; /* Rounded pill shape from image */
        font-weight: 600 !important;
        text-align: center !important;
        justify-content: center !important;
        margin-top: 10px !important;
        box-shadow: 0 4px 6px -1px rgba(209, 31, 65, 0.3) !important;
        width: 100% !important; /* Full width relative to container */
        padding-left: 0 !important; /* Reset nav padding */
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
    }}
    
    section[data-testid="stSidebar"] div[data-testid="stVerticalBlock"] > div:last-child div.stButton > button:hover,
    section[data-testid="stSidebar"] div[data-testid="stVerticalBlock"] > div:last-of-type div.stButton > button:hover {{
        background-color: #9f1239 !important; /* Darker Red */
        background: #9f1239 !important;
        color: #ffffff !important;
        box-shadow: 0 10px 15px -3px rgba(159, 18, 57, 0.4) !important;
        transform: translateY(-2px);
    }}
    
    section[data-testid="stSidebar"] div[data-testid="stVerticalBlock"] > div:last-child div.stButton > button:active {{
        background-color: #881337 !important;
        transform: translateY(1px);
        box-shadow: 0 2px 4px -1px rgba(159, 18, 57, 0.4) !important;
    }}

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
    # INJECT LOGIN SPECIFIC CSS HERE
    st.markdown("""
    <style>
    /* 2. LOGIN CARD: Premium Glassmorphism */
    div[data-testid="stColumn"]:nth-of-type(2) > div[data-testid="stVerticalBlock"] {
        background: rgba(255, 255, 255, 0.65) !important;
        backdrop-filter: blur(25px);
        -webkit-backdrop-filter: blur(25px);
        border: 1px solid rgba(255, 255, 255, 0.4);
        box-shadow: 0 8px 32px 0 rgba(31, 38, 135, 0.15) !important;
        border-radius: 32px;
        padding: 60px !important;
        gap: 24px;
        width: 100% !important;
        margin-left: auto !important;
        margin-right: auto !important;
        display: block !important;
    }

    div[data-testid="stColumn"]:nth-of-type(2) .stTextInput,
    div[data-testid="stColumn"]:nth-of-type(2) .stButton {
        max-width: 400px !important;
        margin-left: auto !important;
        margin-right: auto !important;
    }

    
    @media screen and (max-width: 600px) {
        div[data-testid="stColumn"]:nth-of-type(2) > div[data-testid="stVerticalBlock"] {
            padding: 24px !important;
            width: 95%;
        }
    }
    
    /* 4. BUTTON: PERFECT CENTER (LOGIN ONLY) */
    .stApp > header + div > div > div > div:nth-child(2) div.stButton {
        width: 100%;
        margin-top: 24px;
        display: flex;
        justify-content: center;
    }
    
    /* More robust selector for Login Button */
    div[data-testid="stColumn"]:nth-of-type(2) div[data-testid="stButton"] button {
        background-color: #d11f41 !important;
        color: white !important;
        border: none !important;
        height: 48px !important;
        border-radius: 24px !important;
        font-weight: 600 !important;
        font-size: 16px !important;
        box-shadow: 0 4px 6px -1px rgba(209, 31, 65, 0.25) !important;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
        width: 200px !important;
        margin-left: auto !important;
        margin-right: auto !important;
        display: block !important;
    }

    div[data-testid="stColumn"]:nth-of-type(2) div[data-testid="stButton"] button:hover {
        background-color: #9f1239 !important;
        box-shadow: 0 10px 15px -3px rgba(159, 18, 57, 0.4) !important;
        transform: translateY(-2px) scale(1.02);
        color: white !important;
    }

    div[data-testid="stColumn"]:nth-of-type(2) div[data-testid="stButton"] button:active {
        background-color: #881337 !important;
        transform: translateY(1px);
        box-shadow: 0 2px 4px -1px rgba(159, 18, 57, 0.4) !important;
    }
    </style>
    """, unsafe_allow_html=True)

    logo_path = os.path.join(os.path.dirname(__file__), "assets/app_logo.png")
    ilink_logo_path = os.path.join(os.path.dirname(__file__), "assets/ilink_logo.png")
    
    logo_b64 = get_img_as_base64(logo_path)
    ilink_logo_b64 = get_img_as_base64(ilink_logo_path)

    # Columns: Card occupies middle. 
    # [1, 1.5, 1] results in approx 43% width for the middle card
    col1, col2, col3 = st.columns([1, 1.5, 1])

    with col2:
        # HEADER - STRICT FLEX ALIGNMENT - COMPACT
        st.markdown(f"""
        <div style='display: flex; flex-direction: column; align-items: center; justify-content: center; width: 100%; margin-bottom: 24px;'>
             <div style='margin-bottom: 4px; display: flex; justify-content: center; width: 100%;'>
                  <img src="data:image/png;base64,{logo_b64}" width="110" style="filter: drop-shadow(0 2px 4px rgba(0,0,0,0.1)); display: block; max-width: 100%; height: auto;"> 
             </div>
             <h1 style='color: #0f172a; font-size: clamp(20px, 5vw, 26px); font-weight: 800; margin: 0; padding: 0; width: 100%; letter-spacing: -0.5px; text-align: center;'>iCORE</h1>
             <p style='color: #475569; font-size: clamp(8px, 3vw, 10px); font-weight: 700; margin-top: 2px; letter-spacing: 1.2px; text-transform: uppercase; text-align: center; width: 100%;'>Central Operational Resolution Engine</p>
             <div style='width: 30px; height: 2px; background: #d11f41; margin: 12px auto; border-radius: 1px;'></div>
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
                audit_log.log_event(
                    module="Auth", 
                    action="User Login", 
                    status="Success", 
                    details=f"Session started for {username}",
                    user=username
                )
                st.session_state['authenticated'] = True
                st.session_state['user_role'] = USERS[username]["role"]
                st.session_state['user_name'] = USERS[username]["name"]
                st.session_state['current_page'] = "Dashboard"
                st.rerun()
            else:
                st.error("Invalid credentials.")
        
        # FOOTER (Inside Card)
        st.markdown(f"""
        <div class="footer-text" style="display: flex; flex-direction: column; align-items: center; justify-content: center; width: 100%; margin-top: 24px;">
             <p style="margin-bottom: 4px; color: #475569; font-weight: 600; font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; text-align: center;">Enterprise Entity Resolution Platform</p>
             <div style="display: flex; align-items: center; justify-content: center; gap: 8px;">
                 <span style="font-size: 12px; color: #334155; font-weight: 700;">Powered by</span>
                 <img src="data:image/png;base64,{ilink_logo_b64}" height="28" style="filter: drop-shadow(0 1px 2px rgba(0,0,0,0.1));">
             </div>
        </div>
        """, unsafe_allow_html=True)


# --- SIDEBAR NAVIGATION ---
def sidebar_nav():
    with st.sidebar:
        # 1. Logo Section
        try:
            logo_path = os.path.join(os.path.dirname(__file__), "assets/app_logo.png")
            logo_b64 = get_img_as_base64(logo_path)
        except:
            logo_b64 = ""
            
        st.markdown(f"""
        <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 10px; padding-left: 8px; padding-top: 10px;">
            <img src="data:image/png;base64,{logo_b64}" width="85" style="filter: drop-shadow(0 4px 6px rgba(0,0,0,0.1));">
            <div style="display: flex; flex-direction: column;">
                <h3 style="margin: 0; font-size: 30px; color: #0f172a; font-weight: 800; letter-spacing: -0.5px; line-height: 1.0;">iCORE</h3>
                <p style="margin: 0; font-size: 13px; color: #64748b; font-weight: 700; letter-spacing: 0.5px; margin-top: 2px;">iLINK DIGITAL</p>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # 2. Navigation
        role = st.session_state.get('user_role', 'Guest')
        allowed_pages = PERMISSIONS.get(role, [])
        current_page = st.session_state.get('current_page', 'Dashboard')
        


        for page in allowed_pages:
            # Determine if this is the active page
            is_active = (page == current_page)
            # Use 'primary' type for active to trigger our specific CSS
            btn_type = "primary" if is_active else "secondary"
            
            # Using columns to create a "full width" feel or just standard button
            if st.button(page, key=f"nav_{page}", type=btn_type, use_container_width=True):
                st.session_state['current_page'] = page
                st.rerun()

        # Spacer to push user info to bottom - utilizing flex grow
        st.markdown("<div style='flex-grow: 1;'></div>", unsafe_allow_html=True)
         
        
        # 3. User Profile Card
        user_name = st.session_state.get('user_name', 'User')
        initial = user_name[0] if user_name else "U"
        
        st.markdown(f"""
        <div style="background-color: #ffffff; border-radius: 8px; padding: 8px; margin-bottom: 8px; border: 1px solid #e2e8f0; box-shadow: 0 1px 2px rgba(0,0,0,0.02) !important;">
            <div style="display: flex; align-items: center; gap: 10px;">
                <div style="width: 32px; height: 32px; background: #f1f5f9; border-radius: 50%; display: flex; align-items: center; justify-content: center; color: #64748b; font-weight: 700; font-size: 13px; border: 1px solid #e2e8f0;">
                    {initial}
                </div>
                <div style="overflow: hidden;">
                    <p style="margin: 0; color: #334155; font-size: 12px; font-weight: 600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">{user_name}</p>
                    <p style="margin: 0; color: #94a3b8; font-size: 10px; text-transform: capitalize;">{role}</p>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Sign Out
        if st.button("Sign Out", type="secondary", use_container_width=True):
             # Log Logout
             user = st.session_state.get('user_name', 'User')
             role = st.session_state.get('user_role', 'Guest')
             audit_log.log_event(
                module="Auth",
                action="User Logout",
                status="Success",
                details="User signed out manually",
                user=user
             )
             
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
        import importlib
        importlib.reload(dashboard)
        dashboard.render()
    elif page == "Audit Logs":
        from src.frontend.views import audit
        audit.render()
    elif page == "Connectors":
        from src.frontend.views import connectors
        connectors.render()
    else:
        st.title(page)
        st.info(f"{page} module is currently under development or maintenance.")
