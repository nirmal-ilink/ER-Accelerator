import streamlit as st

def render():
    st.title("Executive Dashboard")
    st.caption("Real-time insights on Data Quality and Entity Resolution performance.")
    
    col1, col2, col3 = st.columns(3)
    
    col1.metric("TOTAL RECORDS", "1,024", "12.5%")
    col2.metric("DUPLICATES FOUND", "342", "-2%")
    col3.metric("AUTO-MERGE RATE", "88%", "5%")
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### Data Quality Score")
        st.bar_chart({"Accuracy": 90, "Completeness": 85, "Consistency": 92})
        
    with c2:
        st.markdown("### Records by Source")
        st.bar_chart({"EMR": 500, "Registry": 524})
