---
description: How to add a new frontend page/view to the Streamlit app
---

# Add a Frontend View

## Steps

1. **Create the view** at `src/frontend/views/<name>.py`
   - Must export a `render()` function — this is the entry point
   - Import and use `st` (Streamlit) for UI
   - Inject view-specific CSS at the top of `render()` if needed
   ```python
   import streamlit as st

   def render():
       st.title("My New Page")
       # ... your UI here
   ```

2. **Register in app.py** — edit `src/frontend/app.py`
   - Add import at the top: `from src.frontend.views import new_view`
   - Add routing in the nav dispatch block (search for `PAGE_PERMISSIONS`):
     ```python
     elif st.session_state['current_page'] == "New Page":
         new_view.render()
     ```

3. **Set RBAC permissions** — in `app.py`, update the `PAGE_PERMISSIONS` dict:
   ```python
   PAGE_PERMISSIONS = {
       "Admin": ["Dashboard", ..., "New Page"],
       ...
   }
   ```

4. **Add sidebar icon** — in the sidebar rendering section of `app.py`, add a button entry with an icon for the new page.

5. **Test** — login with a role that has access, verify the page renders and sidebar icon works.
