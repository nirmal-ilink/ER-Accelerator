# Styling Guide — ER-Accelerator

## CSS Injection Method

All CSS is injected inline via Streamlit's `st.markdown()`:
```python
st.markdown("""<style> ... </style>""", unsafe_allow_html=True)
```
- **Global styles**: `app.py` (lines ~60–800) — fonts, inputs, buttons, sidebar
- **View-specific styles**: Each view injects its own CSS block at the top of `render()`

## Color Palette

| Token | Hex | Usage |
|-------|-----|-------|
| Brand Red | `#D11F41` | Primary buttons, accents, hover states |
| Dark Red | `#9f1239` | Button hover, deep accents |
| Slate 900 | `#0f172a` | Text, dark backgrounds |
| Slate 100 | `#f1f5f9` | Light backgrounds, cards |
| Gray 100 | `#f3f4f6` | Sidebar background (authenticated) |
| White | `#F8FAFC` | Main content background |
| Green | `#10b981` | Success states, positive trends |
| Amber | `#f59e0b` | Warning states |

## Typography

- **Font**: `Inter` from Google Fonts (imported in `app.py`)
- **Weights used**: 400 (body), 500 (labels), 600 (subheadings), 700 (headings), 800 (hero)
- Labels: uppercase, `letter-spacing: 1px`, `font-size: 12px`

## Streamlit Selectors (commonly targeted)

```css
.stApp                                    /* Root app container */
[data-testid="stSidebar"]                /* Sidebar */
[data-testid="stAppViewContainer"]       /* Main content area */
[data-testid="stMain"]                   /* Inner main */
.stTextInput > div > div[data-baseweb]   /* Text inputs */
div[data-baseweb="select"]              /* Dropdowns */
div[data-testid="stButton"] button       /* Buttons */
div[data-testid="stDataFrame"]          /* Data tables */
```

## UI Patterns

- **Cards**: White bg, `border-radius: 12px`, `box-shadow: 0 1px 3px rgba(0,0,0,0.1)`, padding 24px
- **Buttons**: Red bg (`#D11F41`), white text, 8px radius, `transition: all 0.2s ease`
- **Sidebar (collapsed)**: Shows only icons in rounded square buttons, first letter of user name
- **Login page**: Animated gradient background with floating orbs and enterprise grid pattern
- **Hover effects**: `translateY(-2px)`, scale, enhanced box-shadow

## Important Notes

1. Streamlit re-renders the entire page on interaction — CSS must be re-injected every render
2. Use `!important` liberally — Streamlit's default styles are aggressive
3. Use `f-strings` with **double braces** `{{}}` inside `st.markdown` f-strings for CSS braces
4. The `clean_html()` helper (in `inspector.py`, `match_review.py`) strips leading whitespace to prevent Streamlit treating HTML as markdown code blocks
