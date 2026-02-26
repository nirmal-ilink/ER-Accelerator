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
from src.backend.auth.user_manager import UserManager

# Initialize Logger
audit_log = AuditLogger()

# --- CONFIGURATION ---
ASSETS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "assets")
logo_path = os.path.join(ASSETS_DIR, "app_logo.png")
# Force Reload Fix for CSS
st.set_page_config(
    page_title="iCORE | iLink Digital",
    page_icon=logo_path,
    layout="wide",
    initial_sidebar_state="expanded"
)

# Force Reload Trigger (Internal)

# --- AUTHENTICATION & PERMISSIONS ---
if 'user_manager' not in st.session_state:
    st.session_state['user_manager'] = UserManager()

if 'PLATFORM_USERS' not in st.session_state:
    st.session_state['PLATFORM_USERS'] = st.session_state['user_manager'].get_users()

USERS = st.session_state['PLATFORM_USERS']

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
    # LOGIN MODE: 2D Canvas Particle Network Background
    main_bg_css = """
    /* Make app background transparent so Canvas can be seen */
    .stApp {
        background-color: transparent !important;
        position: relative;
    }

    /* Entrance Animation for Login Card */
    div[data-testid="stColumn"]:nth-of-type(2) > div[data-testid="stVerticalBlock"] {
        animation: cardEntrance 0.8s cubic-bezier(0.2, 0.8, 0.2, 1) forwards !important;
        opacity: 0;
    }

    @keyframes cardEntrance {
        from { opacity: 0; transform: translateY(20px) scale(0.98); }
        to { opacity: 1; transform: translateY(0) scale(1); }
    }

    /* Pulsing Sign In Button to guide user */
    div[data-testid="stColumn"]:nth-of-type(2) div[data-testid="stButton"] button {
        animation: buttonPulse 3s ease-in-out infinite !important;
    }

    @keyframes buttonPulse {
        0% { box-shadow: 0 4px 6px -1px rgba(209, 31, 65, 0.25); }
        50% { box-shadow: 0 0 0 4px rgba(209, 31, 65, 0.15), 0 10px 15px -3px rgba(209, 31, 65, 0.3); }
        100% { box-shadow: 0 4px 6px -1px rgba(209, 31, 65, 0.25); }
    }
    
    /* Ensure main content stays above background */
    .stApp > header,
    .stApp > header + div,
    .main .block-container,
    [data-testid="stAppViewContainer"],
    [data-testid="stMain"] {
        position: relative;
        z-index: 20;
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
    
    import streamlit.components.v1 as components
    components.html("""
    <script>
    (function() {
        const parentDoc = window.parent.document;
        const parentWin = window.parent;

        // --- Cleanup helper ---
        if (!parentWin._destroyParticleBg) {
            parentWin._destroyParticleBg = function() {
                if (parentWin._particleAnimId) {
                    cancelAnimationFrame(parentWin._particleAnimId);
                    parentWin._particleAnimId = null;
                }
                const el = parentDoc.getElementById('particle-bg-container');
                if (el) el.remove();
            };
        }

        // Avoid duplicates on Streamlit re-renders
        if (parentDoc.getElementById('particle-bg-container')) return;

        // --- Container + Canvas ---
        const container = parentDoc.createElement('div');
        container.id = 'particle-bg-container';
        Object.assign(container.style, {
            position: 'fixed', top: '0', left: '0',
            width: '100vw', height: '100vh',
            zIndex: '0', pointerEvents: 'none',
            overflow: 'hidden'
        });

        const canvas = parentDoc.createElement('canvas');
        canvas.style.display = 'block';
        container.appendChild(canvas);

        const appEl = parentDoc.querySelector('.stApp');
        if (appEl) {
            appEl.parentNode.insertBefore(container, appEl);
            appEl.style.background = 'transparent';
            appEl.style.backgroundColor = 'transparent';
        } else {
            parentDoc.body.prepend(container);
        }
        const vc = parentDoc.querySelector('[data-testid="stAppViewContainer"]');
        if (vc) { vc.style.background = 'transparent'; vc.style.backgroundColor = 'transparent'; }
        const hdr = parentDoc.querySelector('header');
        if (hdr) { hdr.style.background = 'transparent'; hdr.style.backgroundColor = 'transparent'; }

        // --- Setup ---
        const ctx = canvas.getContext('2d');
        let W, H;

        function resize() {
            W = parentWin.innerWidth;
            H = parentWin.innerHeight;
            canvas.width = W;
            canvas.height = H;
        }
        resize();
        parentWin.addEventListener('resize', resize);

        const NODE_COUNT     = 140;
        const LINE_DIST      = 150;
        const MOUSE_RADIUS   = 160;
        const MOUSE_FORCE    = 3;
        const BG_COLOR       = '#e8ecf1';

        // Balanced palette: brand presence + elegant neutrals
        const NODE_PRESETS = [
            { color: '#d11f41', glow: 'rgba(209,31,65,',   weight: 3 },  // Brand red
            { color: '#e8707e', glow: 'rgba(232,112,126,', weight: 3 },  // Soft red
            { color: '#94a3b8', glow: 'rgba(148,163,184,', weight: 4 },  // Silver slate
            { color: '#7b8ca1', glow: 'rgba(123,140,161,', weight: 3 },  // Medium slate
            { color: '#cbd5e1', glow: 'rgba(203,213,225,', weight: 3 },  // Pale slate
            { color: '#c9956a', glow: 'rgba(201,149,106,', weight: 1 },  // Warm gold
        ];
        const palette = [];
        NODE_PRESETS.forEach(function(p) { for (let i = 0; i < p.weight; i++) palette.push(p); });

        // --- Mouse ---
        const mouse = { x: W * 0.5, y: H * 0.5, tx: W * 0.5, ty: H * 0.5 };
        parentDoc.addEventListener('mousemove', function(e) {
            mouse.tx = e.clientX;
            mouse.ty = e.clientY;
        });

        // --- Nodes with Z-depth for 3D parallax ---
        const nodes = [];
        const cols = Math.ceil(Math.sqrt(NODE_COUNT * (W / H)));
        const rows = Math.ceil(NODE_COUNT / cols);
        const cellW = W / cols;
        const cellH = H / rows;

        for (let i = 0; i < NODE_COUNT; i++) {
            const col = i % cols;
            const row = Math.floor(i / cols);
            const z = Math.random();  // 0 = far background, 1 = near foreground
            const preset = palette[Math.floor(Math.random() * palette.length)];
            nodes.push({
                x: col * cellW + Math.random() * cellW,
                y: row * cellH + Math.random() * cellH,
                z: z,
                vx: (Math.random() - 0.5) * (0.12 + z * 0.3),
                vy: (Math.random() - 0.5) * (0.12 + z * 0.3),
                baseR: 1.5 + z * 3.0,
                color: preset.color,
                glow: preset.glow,
                phase: Math.random() * Math.PI * 2,
                // Projected positions (updated each frame)
                px: 0, py: 0, pr: 0
            });
        }

        // Sort by z for back-to-front rendering
        nodes.sort(function(a, b) { return a.z - b.z; });

        // --- Pre-create off-screen glow textures for performance ---
        function makeGlow(color, glowStr, radius) {
            const size = Math.ceil(radius * 2 + 4);
            const off = parentDoc.createElement('canvas');
            off.width = size; off.height = size;
            const oc = off.getContext('2d');
            const cx = size / 2, cy = size / 2;
            const grad = oc.createRadialGradient(cx, cy, 0, cx, cy, radius);
            grad.addColorStop(0, glowStr + '0.9)');
            grad.addColorStop(0.3, glowStr + '0.4)');
            grad.addColorStop(0.6, glowStr + '0.1)');
            grad.addColorStop(1, glowStr + '0)');
            oc.fillStyle = grad;
            oc.fillRect(0, 0, size, size);
            return off;
        }

        // --- Animation ---
        let time = 0;
        const centerX = W * 0.5;
        const centerY = H * 0.5;

        function animate() {
            parentWin._particleAnimId = requestAnimationFrame(animate);
            time += 0.008;

            // Smooth mouse easing
            mouse.x += (mouse.tx - mouse.x) * 0.05;
            mouse.y += (mouse.ty - mouse.y) * 0.05;

            // Mouse offset from center (for parallax)
            const mx = (mouse.x - W * 0.5) / (W * 0.5);  // -1..1
            const my = (mouse.y - H * 0.5) / (H * 0.5);

            // Clear
            ctx.fillStyle = BG_COLOR;
            ctx.fillRect(0, 0, W, H);

            // Update & project positions
            for (let i = 0; i < NODE_COUNT; i++) {
                const n = nodes[i];

                // Gentle drift (speed scales with z)
                const speed = 0.5 + n.z * 0.5;
                n.x += n.vx + Math.sin(time * speed + n.phase) * 0.15 * speed;
                n.y += n.vy + Math.cos(time * 0.7 * speed + n.phase) * 0.15 * speed;

                // Mouse repulsion (stronger on near nodes)
                const dx = n.x - mouse.tx;
                const dy = n.y - mouse.ty;
                const dist = Math.sqrt(dx * dx + dy * dy);
                if (dist < MOUSE_RADIUS && dist > 0) {
                    const force = (MOUSE_RADIUS - dist) / MOUSE_RADIUS * MOUSE_FORCE * (0.3 + n.z * 0.7);
                    n.x += (dx / dist) * force;
                    n.y += (dy / dist) * force;
                }

                // Wrap at edges
                const margin = LINE_DIST;
                if (n.x < -margin) n.x = W + margin;
                if (n.x > W + margin) n.x = -margin;
                if (n.y < -margin) n.y = H + margin;
                if (n.y > H + margin) n.y = -margin;

                // 3D Parallax projection: deeper nodes shift less with mouse
                const parallax = 0.2 + n.z * 0.8;
                n.px = n.x + mx * 30 * parallax;
                n.py = n.y + my * 22 * parallax;
                n.pr = n.baseR + Math.sin(time * 1.8 + n.phase) * 0.4;
            }

            // --- Draw lines (back layer) ---
            ctx.lineWidth = 0.8;
            for (let i = 0; i < NODE_COUNT; i++) {
                const a = nodes[i];
                for (let j = i + 1; j < NODE_COUNT; j++) {
                    const b = nodes[j];
                    const dx = a.px - b.px;
                    const dy = a.py - b.py;
                    const d2 = dx * dx + dy * dy;
                    if (d2 < LINE_DIST * LINE_DIST) {
                        const d = Math.sqrt(d2);
                        const avgZ = (a.z + b.z) * 0.5;
                        const alpha = (1 - d / LINE_DIST) * (0.06 + avgZ * 0.18);
                        ctx.strokeStyle = 'rgba(148,163,184,' + alpha.toFixed(3) + ')';
                        ctx.beginPath();
                        ctx.moveTo(a.px, a.py);
                        ctx.lineTo(b.px, b.py);
                        ctx.stroke();
                    }
                }
            }

            // --- Draw nodes back-to-front ---
            for (let i = 0; i < NODE_COUNT; i++) {
                const n = nodes[i];
                const nodeAlpha = 0.25 + n.z * 0.5;

                // 1. Very subtle ambient halo (small, not blobby)
                const haloR = n.pr * 2.2;
                ctx.beginPath();
                ctx.arc(n.px, n.py, haloR, 0, Math.PI * 2);
                ctx.fillStyle = n.glow + (nodeAlpha * 0.07).toFixed(3) + ')';
                ctx.fill();

                // 2. Crisp core circle (the main shape)
                ctx.beginPath();
                ctx.arc(n.px, n.py, n.pr, 0, Math.PI * 2);
                ctx.fillStyle = n.color;
                ctx.globalAlpha = nodeAlpha;
                ctx.fill();
                ctx.globalAlpha = 1;

                // 3. Thin ring outline on near nodes for definition
                if (n.z > 0.4) {
                    ctx.beginPath();
                    ctx.arc(n.px, n.py, n.pr + 0.8, 0, Math.PI * 2);
                    ctx.strokeStyle = n.glow + (nodeAlpha * 0.25).toFixed(2) + ')';
                    ctx.lineWidth = 0.5;
                    ctx.stroke();
                }
            }
        }

        animate();

        // Cleanup on unmount (login -> dashboard)
        window.addEventListener('beforeunload', function() {
            parentWin._destroyParticleBg();
        });
    })();
    </script>
    """, height=0, width=0)
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
    
    /* DYNAMIC MAIN CONTENT EXPANSION WITH MINI-SIDEBAR */
    section[data-testid="stSidebar"][aria-expanded="false"] {{
        width: 105px !important;
        min-width: 105px !important;
        max-width: 105px !important;
        transform: translateX(0) !important;
        margin-left: 0px !important;
        position: relative !important;
        overflow-x: hidden !important;
    }}
    
    section[data-testid="stSidebar"][aria-expanded="false"] > div {{
        width: 105px !important;
        min-width: 105px !important;
        overflow-x: hidden !important;
    }}
    
    /* Target using sibling combinators for robust main content expansion */
    [data-testid="stAppViewContainer"]:has(section[data-testid="stSidebar"][aria-expanded="false"]) [data-testid="stMain"],
    .stApp:has(section[data-testid="stSidebar"][aria-expanded="false"]) [data-testid="stMain"],
    .stApp:has(section[data-testid="stSidebar"][aria-expanded="false"]) .st-emotion-cache-6px8kg,
    section[data-testid="stSidebar"][aria-expanded="false"] + [data-testid="stMain"],
    section[data-testid="stSidebar"][aria-expanded="false"] ~ [data-testid="stMain"] {{
        margin-left: 0px !important;
        width: 100% !important;
        max-width: 100% !important;
    }}
    
    [data-testid="stAppViewContainer"]:has(section[data-testid="stSidebar"][aria-expanded="false"]) .stMainBlockContainer,
    .stApp:has(section[data-testid="stSidebar"][aria-expanded="false"]) .block-container,
    section[data-testid="stSidebar"][aria-expanded="false"] + [data-testid="stMain"] .block-container,
    section[data-testid="stSidebar"][aria-expanded="false"] ~ [data-testid="stMain"] .block-container {{
        margin-left: auto !important;
        margin-right: auto !important;
        max-width: 100% !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
    }}

    /* --- MINI-SIDEBAR ICON VISIBILITY LOGIC --- */
    /* Hide Text for Navigation Buttons */
    section[data-testid="stSidebar"][aria-expanded="false"] .stButton button p {{
        font-size: 0px !important;
        color: transparent !important;
    }}
    
    /* Adjust Button Dimensions to Rounded Squares for Icons */
    section[data-testid="stSidebar"][aria-expanded="false"] div.stButton {{
        display: flex !important;
        justify-content: center !important;
        margin-bottom: 8px !important;
        width: 100% !important;
    }}
    
    section[data-testid="stSidebar"][aria-expanded="false"] div.stButton button,
    section[data-testid="stSidebar"][aria-expanded="false"] div.element-container:has(span[data-page]) + div.element-container .stButton button {{
        padding: 0 !important;
        background-position: center center !important; /* Force exact center */
        background-size: 24px 24px !important; /* Perfect fit */
        width: 44px !important; /* Compact button */
        min-width: 44px !important;
        max-width: 44px !important;
        height: 44px !important; /* Compact button */
        min-height: 44px !important;
        max-height: 44px !important;
        margin: 0 !important;
        border-radius: 14px !important; /* Rounded square */
        background-color: transparent !important; /* Let active/hover handle background colors */
        border: none !important; /* Remove harsh default borders if any */
        box-shadow: none !important; /* Flatten out default shadows */
        transition: all 0.2s ease !important;
    }}
    
    /* Ensure hover/active states don't break the new compact shape */
    section[data-testid="stSidebar"][aria-expanded="false"] div.stButton button:hover,
    section[data-testid="stSidebar"][aria-expanded="false"] div.element-container:has(span[data-page]) + div.element-container .stButton button:hover {{
        background-color: rgba(209, 31, 65, 0.06) !important; /* Very light red tint */
        transform: scale(1.02) !important;
        box-shadow: 0 0 0 1px rgba(209, 31, 65, 0.1) !important; /* Subtle outline */
    }}
    
    section[data-testid="stSidebar"][aria-expanded="false"] div.stButton button[kind="primary"],
    section[data-testid="stSidebar"][aria-expanded="false"] div.stButton button[data-testid*="primary"],
    section[data-testid="stSidebar"][aria-expanded="false"] div.element-container:has(span[data-page]) + div.element-container .stButton button[kind="primary"],
    section[data-testid="stSidebar"][aria-expanded="false"] div.element-container:has(span[data-page]) + div.element-container .stButton button[data-testid*="primary"] {{
        background-color: #ffffff !important;
        border: none !important;
        box-shadow: 0 2px 8px rgba(209, 31, 65, 0.12), 
                    0 0 0 1px rgba(209, 31, 65, 0.15) !important;
    }}

    section[data-testid="stSidebar"][aria-expanded="false"] div.stButton button[kind="primary"]:hover,
    section[data-testid="stSidebar"][aria-expanded="false"] div.stButton button[data-testid*="primary"]:hover,
    section[data-testid="stSidebar"][aria-expanded="false"] div.element-container:has(span[data-page]) + div.element-container .stButton button[kind="primary"]:hover,
    section[data-testid="stSidebar"][aria-expanded="false"] div.element-container:has(span[data-page]) + div.element-container .stButton button[data-testid*="primary"]:hover {{
        box-shadow: 0 4px 12px rgba(209, 31, 65, 0.18), 
                    0 0 0 1px rgba(209, 31, 65, 0.2) !important;
        transform: scale(1.02) !important;
    }}

    /* Fix the Sign Out Button (which lacks an icon by default) */
    section[data-testid="stSidebar"][aria-expanded="false"] div[data-testid="stVerticalBlock"] > div:last-child div.stButton > button {{
        background-image: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="white"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1" /></svg>') !important;
        background-position: center !important;
        background-repeat: no-repeat !important;
        background-size: 26px 26px !important;
    }}

    /* --- LOGO & USER PROFILE COLLAPSED STATE --- */
    
    /* Hide text from Logo */
    section[data-testid="stSidebar"][aria-expanded="false"] .sidebar-logo-text {{
        display: none !important;
    }}

    /* Hide text from User Profile */
    section[data-testid="stSidebar"][aria-expanded="false"] .user-profile-text {{
        display: none !important;
    }}

    /* Re-center the Logo Image */
    section[data-testid="stSidebar"][aria-expanded="false"] .sidebar-logo-img {{
        width: 76px !important; /* Enlarge the logo */
        margin: 0 auto !important;
        display: block !important;
    }}

    /* Adjust Logo Container */
    section[data-testid="stSidebar"][aria-expanded="false"] .sidebar-logo-container {{
        justify-content: center !important;
        padding-left: 0px !important;
        gap: 0px !important;
    }}

    /* Adjust User Profile Card for Compactness */
    section[data-testid="stSidebar"][aria-expanded="false"] .user-profile-card {{
        background-color: transparent !important;
        border: none !important;
        box-shadow: none !important;
        padding: 0px !important;
        margin-bottom: 8px !important;
        display: flex !important;
        justify-content: center !important;
        align-items: center !important;
    }}

    section[data-testid="stSidebar"][aria-expanded="false"] .user-profile-content {{
        gap: 0px !important;
        justify-content: center !important;
        align-items: center !important;
        width: 100% !important;
    }}

    /* Re-center User Profile Initial */
    section[data-testid="stSidebar"][aria-expanded="false"] .user-profile-initial {{
        margin: 0 auto !important;
        width: 42px !important;
        height: 42px !important;
        font-size: 18px !important;
        transition: all 0.2s ease !important;
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
    section[data-testid="stSidebar"] .stButton > button[kind="primary"],
    section[data-testid="stSidebar"] .stButton > button[data-testid*="primary"] {{
        background-color: #ffffff !important;
        color: #d11f41 !important; /* Brand Red */
        font-weight: 600 !important;
        box-shadow: 0 2px 8px rgba(209, 31, 65, 0.12), 
                    0 0 0 1px rgba(209, 31, 65, 0.15) !important;
        border: none !important;
    }}
    
    section[data-testid="stSidebar"] .stButton > button[kind="primary"]:hover,
    section[data-testid="stSidebar"] .stButton > button[data-testid*="primary"]:hover {{
        box-shadow: 0 4px 12px rgba(209, 31, 65, 0.18), 
                    0 0 0 1px rgba(209, 31, 65, 0.2) !important;
        transform: scale(1.02) !important;
    }}

    /* HIDE STREAMLIT'S DEFAULT EXTERNAL COLLAPSED CONTROL (We will use the internal one!) */
    [data-testid="stSidebarCollapsedControl"],
    [data-testid="collapsedControl"],
    [data-testid="stExpandSidebarButton"] {{
        display: none !important;
    }}

    /* FORCE THE INTERNAL MENU BUTTON TO BE VISIBLE AT ALL TIMES & CENTERED IN MINI-SIDEBAR */
    section[data-testid="stSidebar"][aria-expanded="false"] div[data-testid="stSidebarHeader"] {{
        display: flex !important;
        justify-content: center !important;
        align-items: center !important;
        padding-left: 0 !important;
        padding-right: 0 !important;
        width: 100% !important;
        padding-top: 18px !important;
    }}

    section[data-testid="stSidebar"][aria-expanded="false"] div[data-testid="stSidebarCollapseButton"],
    section[data-testid="stSidebar"][aria-expanded="false"] div[data-testid="stSidebarHeader"] > div {{
        display: flex !important;
        justify-content: center !important;
        margin: 0 auto !important;
    }}

    /* --- SWAP THE << ICON TO >> WHEN COLLAPSED --- */
    /* Hide the original literal text content "keyboard_double_arrow_left" */
    section[data-testid="stSidebar"][aria-expanded="false"] [data-testid="stIconMaterial"] {{
        color: transparent !important; /* Hide original text */
        position: relative;
    }}
    
    /* Inject the new literal text content "keyboard_double_arrow_right" */
    section[data-testid="stSidebar"][aria-expanded="false"] [data-testid="stIconMaterial"]::after {{
        content: "keyboard_double_arrow_right" !important;
        color: #0f172a !important; /* Same as hover/active icon color */
        position: absolute;
        top: 0;
        left: 0;
        font-family: inherit;
        font-size: inherit;
        display: flex;
        align-items: center;
        justify-content: center;
        width: 100%;
        height: 100%;
        transition: color 0.2s ease !important;
    }}
    
    /* Hover state for the injected expand icon */
    div[data-testid="stSidebarCollapseButton"] button:hover span::after,
    section[data-testid="stSidebar"][aria-expanded="false"] div[data-testid="stSidebarCollapseButton"] button:hover [data-testid="stIconMaterial"]::after {{
        color: #d11f41 !important; /* Brand Red Icon */
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
        cursor: pointer !important;
        pointer-events: auto !important;
    }}
    
    section[data-testid="stSidebar"] div[data-testid="stVerticalBlock"] > div:last-child div.stButton > button:hover,
    section[data-testid="stSidebar"] div[data-testid="stVerticalBlock"] > div:last-of-type div.stButton > button:hover {{
        background-color: #9f1239 !important; /* Darker Red */
        color: #ffffff !important;
        box-shadow: 0 10px 15px -3px rgba(159, 18, 57, 0.4) !important;
        transform: translateY(-2px);
    }}
    
    section[data-testid="stSidebar"] div[data-testid="stVerticalBlock"] > div:last-child div.stButton > button:active {{
        background-color: #881337 !important;
        transform: translateY(1px);
        box-shadow: 0 2px 4px -1px rgba(159, 18, 57, 0.4) !important;
    }}

    /* ---------------------------------------------------------
       STATUS (st.status) POLISH - AGGRESSIVE LIGHT THEME
       --------------------------------------------------------- */
    /* Target the main container and all its states */
    [data-testid="stStatusReport"],
    [data-testid="stStatusReport"] > details,
    [data-testid="stStatusReport"] summary,
    [data-testid="stStatusReport"] [data-testid="stExpanderDetails"],
    [data-testid="stStatusReport"] .st-emotion-cache-11ofl8m,
    [data-testid="stStatusReport"] .st-emotion-cache-nwb5ao,
    [data-testid="stStatusReport"] .st-emotion-cache-11fa8fd {{
        background-color: #ffffff !important;
        background: #ffffff !important;
        color: #1e293b !important;
        border-color: #e2e8f0 !important;
    }}

    /* Force background for the entire widget block */
    [data-testid="stStatusReport"] {{
        border: 1px solid #e2e8f0 !important;
        border-radius: 8px !important;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08) !important;
        padding: 0 !important;
        overflow: hidden !important;
    }}

    /* Target the summary (the header part with the spinner) */
    [data-testid="stStatusReport"] summary {{
        padding: 12px 16px !important;
        list-style: none !important;
        display: flex !important;
        align-items: center !important;
        border-bottom: none !important; /* Keep it clean when collapsed */
    }}

    /* Force text visibility inside header and content */
    [data-testid="stStatusReport"] summary span,
    [data-testid="stStatusReport"] summary div p,
    [data-testid="stStatusReport"] [data-testid="stMarkdownContainer"] p {{
        color: #1e293b !important;
        font-weight: 600 !important;
        font-size: 14px !important;
        margin: 0 !important;
    }}

    /* Fix the spinner and icons */
    [data-testid="stStatusReport"] svg,
    [data-testid="stStatusReport"] [data-testid="stExpanderIconSpinner"],
    [data-testid="stStatusReport"] [data-testid="stExpanderIcon"] {{
        fill: #d11f41 !important;
        color: #d11f41 !important;
    }}

    /* Hover state refinement */
    [data-testid="stStatusReport"] summary:hover {{
        background-color: #f8fafc !important;
    }}

    /* ---------------------------------------------------------
       GLOBAL SELECTBOX / DROPDOWN STYLING - CLEAN WHITE THEME
       --------------------------------------------------------- */
    
    /* DROPDOWN TRIGGER - CLEAN WHITE */
    .stSelectbox [data-baseweb="select"] > div:first-child,
    div[data-baseweb="select"] > div:first-child {{
        background: #ffffff !important;
        background-color: #ffffff !important;
        border: 1px solid #e2e8f0 !important;
        border-radius: 10px !important;
        color: #1f2937 !important;
        box-shadow: 0 1px 2px rgba(0,0,0,0.04) !important;
        transition: all 0.2s ease !important;
    }}
    
    .stSelectbox [data-baseweb="select"] > div:first-child:hover,
    div[data-baseweb="select"] > div:first-child:hover {{
        border-color: #cbd5e1 !important;
        box-shadow: 0 2px 4px rgba(0,0,0,0.06) !important;
    }}
    
    .stSelectbox [data-baseweb="select"] > div:first-child:focus-within,
    div[data-baseweb="select"] > div:first-child:focus-within {{
        border-color: #d11f41 !important;
        box-shadow: 0 0 0 1px rgba(209, 31, 65, 0.2) !important;
    }}
    
    /* DROPDOWN TEXT */
    .stSelectbox span, div[data-baseweb="select"] span {{
        color: #1f2937 !important;
        font-weight: 500 !important;
        font-size: 14px !important;
    }}
    
    /* DROPDOWN ARROW */
    .stSelectbox svg, div[data-baseweb="select"] svg {{
        fill: #64748b !important;
    }}
    
    /* DROPDOWN MENU CONTAINER - CLEAN WHITE */
    [data-baseweb="popover"],
    [data-baseweb="popover"] > div {{
        background: #ffffff !important;
        background-color: #ffffff !important;
        border: none !important;
        box-shadow: 0 10px 40px -10px rgba(0,0,0,0.2), 0 4px 16px rgba(0,0,0,0.08) !important;
        border-radius: 12px !important;
        overflow: hidden !important;
    }}
    
    /* DROPDOWN LIST */
    ul[data-testid="stSelectboxVirtualDropdown"],
    [data-baseweb="menu"],
    ul[role="listbox"] {{
        background: #ffffff !important;
        background-color: #ffffff !important;
        padding: 6px !important;
        border: none !important;
        border-radius: 12px !important;
    }}
    
    /* DROPDOWN ITEMS */
    li[role="option"],
    li.st-emotion-cache-xcuh4j,
    li.st-emotion-cache-5djvkm,
    .eg1z3xh0 {{
        background: #ffffff !important;
        background-color: #ffffff !important;
        color: #374151 !important;
        border: none !important;
        border-radius: 8px !important;
        margin: 2px 6px !important;
        padding: 10px 14px !important;
        font-size: 14px !important;
        font-weight: 500 !important;
        transition: all 0.15s ease !important;
    }}
    
    /* HOVER STATE */
    li[role="option"]:hover,
    li.st-emotion-cache-xcuh4j:hover,
    li.st-emotion-cache-5djvkm:hover {{
        background: #fef2f2 !important;
        background-color: #fef2f2 !important;
        color: #111827 !important;
    }}
    
    /* SELECTED STATE - Brand Red */
    li[role="option"][aria-selected="true"],
    li.st-emotion-cache-5djvkm {{
        background: #fef2f2 !important;
        background-color: #fef2f2 !important;
        color: #d11f41 !important;
        font-weight: 600 !important;
    }}
    
    /* TEXT INSIDE DROPDOWN ITEMS */
    .st-emotion-cache-qiev7j,
    .st-emotion-cache-11loom0,
    li[role="option"] div {{
        color: inherit !important;
        font-weight: inherit !important;
    }}
    
    /* REMOVE ANY DARK BORDERS/OUTLINES */
    li[role="option"]::before,
    li[role="option"]::after,
    .stSelectbox *::before,
    .stSelectbox *::after {{
        display: none !important;
    }}
    
    /* SCROLLBAR - CLEAN */
    ul[data-testid="stSelectboxVirtualDropdown"],
    [data-baseweb="menu"],
    ul[role="listbox"] {{
        overflow-x: hidden !important;
        overflow-y: auto !important;
        scrollbar-width: thin !important;
        scrollbar-color: #e5e7eb transparent !important;
    }}
    
    ul[data-testid="stSelectboxVirtualDropdown"]::-webkit-scrollbar {{
        width: 4px !important;
        height: 0px !important;
    }}
    ul[data-testid="stSelectboxVirtualDropdown"]::-webkit-scrollbar-thumb {{
        background: #d1d5db !important;
        border-radius: 4px !important;
    }}
    ul[data-testid="stSelectboxVirtualDropdown"]::-webkit-scrollbar-track {{
        background: transparent !important;
    }}
    
    /* Hide horizontal scrollbar everywhere in dropdown */
    [data-baseweb="popover"] *,
    .stSelectbox * {{
        overflow-x: hidden !important;
    }}
    
    /* SELECTBOX LABEL STYLING */
    .stSelectbox label,
    div[data-testid="stSelectbox"] label {{
        color: #475569 !important;
        font-size: 12px !important;
        font-weight: 600 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.5px !important;
        margin-bottom: 6px !important;
    }}
    
    .stSelectbox label p,
    div[data-testid="stSelectbox"] label p {{
        color: #475569 !important;
        font-weight: 600 !important;
    }}

    /* ---------------------------------------------------------
       GLOBAL EXPANDER STYLING - CLEAN WHITE THEME
       --------------------------------------------------------- */
    
    /* Expander Container */
    [data-testid="stExpander"],
    details[data-testid="stExpander"] {{
        background: #ffffff !important;
        background-color: #ffffff !important;
        border: 1px solid #e2e8f0 !important;
        border-radius: 12px !important;
        overflow: hidden !important;
        margin-bottom: 12px !important;
    }}
    
    /* Expander Header/Summary */
    [data-testid="stExpander"] summary,
    [data-testid="stExpander"] [data-testid="stExpanderHeader"],
    details[data-testid="stExpander"] summary {{
        background: #f8fafc !important;
        background-color: #f8fafc !important;
        color: #1e293b !important;
        padding: 14px 18px !important;
        font-weight: 600 !important;
        font-size: 14px !important;
        border: none !important;
        list-style: none !important;
    }}
    
    /* Expander Header Text */
    [data-testid="stExpander"] summary p,
    [data-testid="stExpander"] summary span,
    [data-testid="stExpanderHeader"] p,
    [data-testid="stExpanderHeader"] span {{
        color: #1e293b !important;
        background: transparent !important;
        font-weight: 600 !important;
    }}
    
    /* Expander Header Hover */
    [data-testid="stExpander"] summary:hover,
    details[data-testid="stExpander"] summary:hover {{
        background: #f1f5f9 !important;
        background-color: #f1f5f9 !important;
    }}
    
    /* Expander Content Area */
    [data-testid="stExpander"] [data-testid="stExpanderDetails"],
    details[data-testid="stExpander"] > div {{
        background: #ffffff !important;
        background-color: #ffffff !important;
        padding: 16px !important;
        border-top: 1px solid #e2e8f0 !important;
    }}
    
    /* Expander Icon/Arrow */
    [data-testid="stExpander"] svg,
    [data-testid="stExpanderHeader"] svg {{
        fill: #64748b !important;
        color: #64748b !important;
    }}

    /* ---------------------------------------------------------
       GLOBAL CHECKBOX STYLING - VISIBLE LABELS
       --------------------------------------------------------- */
    
    /* Checkbox Container */
    [data-testid="stCheckbox"],
    .stCheckbox {{
        background: transparent !important;
    }}
    
    /* Checkbox Label - Force Visibility */
    [data-testid="stCheckbox"] label,
    .stCheckbox label {{
        color: #374151 !important;
        font-size: 14px !important;
        font-weight: 500 !important;
        display: flex !important;
        align-items: center !important;
        gap: 10px !important;
        cursor: pointer !important;
    }}
    
    [data-testid="stCheckbox"] label p,
    [data-testid="stCheckbox"] label span,
    .stCheckbox label p,
    .stCheckbox label span {{
        color: #374151 !important;
        font-weight: 500 !important;
    }}
    
    /* Checkbox Box Styling */
    [data-testid="stCheckbox"] input[type="checkbox"],
    .stCheckbox input[type="checkbox"] {{
        accent-color: #d11f41 !important;
        width: 18px !important;
        height: 18px !important;
    }}
    
    /* Checkbox Hover */
    [data-testid="stCheckbox"] label:hover p,
    .stCheckbox label:hover p {{
        color: #1e293b !important;
    }}
    
    /* Checked Checkbox Label */
    [data-testid="stCheckbox"]:has(input:checked) label p,
    .stCheckbox:has(input:checked) label p {{
        color: #1e293b !important;
        font-weight: 600 !important;
    }}

    /* ---------------------------------------------------------
       GLOBAL RADIO BUTTON STYLING - VISIBLE LABELS
       --------------------------------------------------------- */
    
    /* Radio Group Container */
    div[role="radiogroup"] {{
        background: transparent !important;
    }}
    
    /* Radio Label - Force Visibility */
    div[role="radiogroup"] label,
    div[role="radiogroup"] > label {{
        color: #374151 !important;
        font-size: 14px !important;
        font-weight: 500 !important;
        display: flex !important;
        align-items: center !important;
        gap: 10px !important;
        cursor: pointer !important;
        padding: 8px 0 !important;
    }}
    
    div[role="radiogroup"] label p,
    div[role="radiogroup"] label span {{
        color: #374151 !important;
        font-weight: 500 !important;
    }}
    
    /* Radio Button Circle */
    div[role="radiogroup"] input[type="radio"] {{
        accent-color: #d11f41 !important;
        width: 18px !important;
        height: 18px !important;
    }}
    
    /* Radio Hover */
    div[role="radiogroup"] label:hover p {{
        color: #1e293b !important;
    }}
    
    /* Selected Radio Label */
    div[role="radiogroup"] label:has(input:checked) p {{
        color: #d11f41 !important;
        font-weight: 600 !important;
    }}
    
    /* Radio Section Label (the group title) */
    .stRadio label {{
        color: #475569 !important;
        font-size: 12px !important;
        font-weight: 600 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.5px !important;
    }}

    /* ---------------------------------------------------------
       GLOBAL TOOLTIP STYLING - CLEAN WHITE THEME
       --------------------------------------------------------- */
       
    /* Tooltip Inner Container */
    [data-baseweb="tooltip"] > div {{
        background-color: #ffffff !important;
        color: #0f172a !important;
        font-weight: 600 !important;
        font-size: 13px !important;
        border: 1px solid #e2e8f0 !important;
        border-radius: 8px !important;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08) !important;
        padding: 8px 12px !important;
    }}
    
    /* Tooltip Arrow */
    [data-baseweb="tooltip"] > div > div {{
        background-color: #ffffff !important;
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
    /* 2. LOGIN CARD: Ultra-Premium Glassmorphism & Depth */
    div[data-testid="stColumn"]:nth-of-type(2) > div[data-testid="stVerticalBlock"] {
        background: rgba(255, 255, 255, 0.7) !important;
        backdrop-filter: blur(40px) saturate(180%);
        -webkit-backdrop-filter: blur(40px) saturate(180%);
        border: 1px solid rgba(255, 255, 255, 0.5);
        box-shadow: 
            0 25px 50px -12px rgba(0, 0, 0, 0.15),
            0 0 0 1px rgba(255, 255, 255, 0.3) inset,
            0 4px 24px -1px rgba(209, 31, 65, 0.05) !important;
        border-radius: 40px;
        padding: 60px !important;
        gap: 24px;
        width: 100% !important;
        margin-left: auto !important;
        margin-right: auto !important;
        display: block !important;
        position: relative;
        z-index: 10;
        animation: cardEntrance 0.8s cubic-bezier(0.2, 0.8, 0.2, 1) forwards !important;
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

    ASSETS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "assets")
    logo_path = os.path.join(ASSETS_DIR, "app_logo.png")
    ilink_logo_path = os.path.join(ASSETS_DIR, "ilink_logo.png")
    
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
            ASSETS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "assets")
            logo_path = os.path.join(ASSETS_DIR, "app_logo.png")
            logo_b64 = get_img_as_base64(logo_path)
        except:
            logo_b64 = ""
            
        st.markdown(f"""
        <div class="sidebar-logo-container" style="display: flex; align-items: center; gap: 12px; margin-bottom: 10px; padding-left: 8px; padding-top: 10px;">
            <img class="sidebar-logo-img" src="data:image/png;base64,{logo_b64}" width="100" style="filter: drop-shadow(0 4px 6px rgba(0,0,0,0.1));">
            <div class="sidebar-logo-text" style="display: flex; flex-direction: column;">
                <h3 style="margin: 0; font-size: 28px; color: #0f172a; font-weight: 800; letter-spacing: -0.5px; line-height: 1.0;">iCORE</h3>
                <p style="margin: 0; font-size: 12px; color: #64748b; font-weight: 700; letter-spacing: 0.5px; margin-top: 2px;">iLINK DIGITAL</p>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # 2. Navigation
        role = st.session_state.get('user_role', 'Guest')
        allowed_pages = PERMISSIONS.get(role, [])
        current_page = st.session_state.get('current_page', 'Dashboard')
        page_icons = {
            "Dashboard": "layout-dashboard.png",
            "Connectors": "plug.png",
            "Pipeline Inspector": "workflow.png",
            "Match Review": "compare.png",
            "Audit Logs": "file-text.png",
            "User Management": "shield-user.png"
        }

        for page in allowed_pages:
            # Determine if this is the active page
            is_active = (page == current_page)
            # Use 'primary' type for active to trigger our specific CSS
            btn_type = "primary" if is_active else "secondary"
            
            icon_filename = page_icons.get(page)
            if icon_filename:
                ASSETS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "assets")
                icon_path = os.path.join(ASSETS_DIR, f"{icon_filename}")
                icon_b64 = get_img_as_base64(icon_path)
                if icon_b64:
                    page_id = page.replace(' ', '-')
                    st.markdown(f'''
                    <span data-page="{page_id}"></span>
                    <style>
                        div.element-container:has(span[data-page="{page_id}"]) {{
                            display: none !important;
                        }}
                        div.element-container:has(span[data-page="{page_id}"]) + div.element-container .stButton button {{
                            background-image: url('data:image/png;base64,{icon_b64}') !important;
                            background-position: 16px center !important;
                            background-repeat: no-repeat !important;
                            background-size: 18px 18px !important;
                            padding-left: 44px !important;
                            text-align: left !important;
                            justify-content: flex-start !important;
                        }}
                        /* Override background position specifically when sidebar is collapsed */
                        section[data-testid="stSidebar"][aria-expanded="false"] div.element-container:has(span[data-page="{page_id}"]) + div.element-container .stButton button {{
                            background-position: center center !important;
                        }}
                    </style>
                    ''', unsafe_allow_html=True)

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
        <div class="user-profile-card" style="background-color: #ffffff; border-radius: 8px; padding: 8px; margin-bottom: 8px; border: 1px solid #e2e8f0; box-shadow: 0 1px 2px rgba(0,0,0,0.02) !important;">
            <div class="user-profile-content" style="display: flex; align-items: center; gap: 10px;">
                <div class="user-profile-initial" style="width: 32px; height: 32px; background: #f1f5f9; border-radius: 50%; display: flex; align-items: center; justify-content: center; color: #64748b; font-weight: 700; font-size: 13px; border: 1px solid #e2e8f0; flex-shrink: 0;">
                    {initial}
                </div>
                <div class="user-profile-text" style="overflow: hidden;">
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
        dashboard.render()
    elif page == "Audit Logs":
        from src.frontend.views import audit
        audit.render()
    elif page == "Connectors":
        from src.frontend.views import connectors
        connectors.render()
    elif page == "User Management":
        from src.frontend.views import users
        users.render()
    elif page == "Pipeline Inspector":
        from src.frontend.views import inspector
        inspector.render()
    else:
        st.title(page)
        st.info(f"{page} module is currently under development or maintenance.")
