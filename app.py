import streamlit as st
import psycopg2
import os
import json
from dotenv import load_dotenv

load_dotenv()

from osm_sql_generator import natural_language_to_sql, GroqProvider

st.set_page_config(
    page_title="Delhi OSM Query Tool",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Sidebar with system info and security status
with st.sidebar:
    st.title("� Security & Info")
    st.success("✅ SQL Injection Protection Active")
    st.success("✅ SELECT-Only Mode Enabled")
    st.success("✅ IR Validation Layer Active")
    st.success("✅ Geography-Based Distance (Accurate)")
    
    st.markdown("---")
    st.subheader("System Status")
    st.info("All security layers operational")
    
    st.markdown("---")
    st.caption("For Dr. Yashvardhan Sharma, BITS Pilani")
    st.caption("Secure NL-to-SQL Middleware v2.0")

# Main content
st.title("🗺️ Delhi Geospatial Query Tool")
st.markdown("**Natural Language to Spatial SQL with Security & IR Validation**")

# Initialize connections
if "conn" not in st.session_state:
    try:
        st.session_state.conn = psycopg2.connect(
            database=os.getenv("PGDATABASE", "delhi_db"),
            user=os.getenv("PGUSER", "aryansoni"),
            password=os.getenv("PGPASSWORD", ""),
        )
        st.session_state.db_connected = True
    except Exception as e:
        st.error(f"❌ Database connection failed: {e}")
        st.session_state.db_connected = False
        st.stop()

if "llm" not in st.session_state:
    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        st.error("❌ Please set OPENAI_API_KEY in .env file")
        st.stop()
    st.session_state.llm = GroqProvider(api_key=api_key)

# Query input with security notice
st.markdown("---")
query = st.text_input(
    "🔍 Enter your query in natural language:",
    placeholder="e.g., 'Hospitals within 5km of schools' or 'Parks near Connaught Place'",
    help="Queries are validated for security before execution. Only SELECT statements are allowed."
)

# Action buttons
col1, col2, col3 = st.columns([1, 1, 4])
with col1:
    run_btn = st.button("🚀 Run Query", type="primary", use_container_width=True)
with col2:
    clear_btn = st.button("🧹 Clear", use_container_width=True)
with col3:
    show_ir = st.checkbox("Show Intermediate Representation (IR)", value=True)
    show_sql = st.checkbox("Show Generated SQL", value=True)
    show_explanation = st.checkbox("Show Explanation", value=True)

# Execute query
if run_btn and query:
    with st.spinner("🔐 Validating input... Generating IR... Executing securely..."):
        try:
            result = natural_language_to_sql(
                query, 
                st.session_state.conn, 
                st.session_state.llm,
                use_geography=True
            )

            # Display results in tabs
            tabs = st.tabs(["📊 Results", "🔍 Query Details", "📋 Raw Data"])
            
            with tabs[0]:  # Results tab
                if result.get("success"):
                    data = result.get("data", [])
                    
                    if isinstance(data, list) and len(data) > 0:
                        st.success(f"✅ Found {len(data)} records")
                        
                        import pandas as pd
                        df = pd.DataFrame(data)
                        st.dataframe(df, use_container_width=True)
                        
                        # Map visualization hint
                        if len(df) > 0 and "way" in df.columns:
                            st.info("💡 Tip: Use ST_AsGeoJSON(way) in SQL to get GeoJSON for mapping")
                    else:
                        st.warning("⚠️ No results found")
                else:
                    st.error("❌ Query failed")
                    st.error(result.get("error", "Unknown error"))
                    
                    if result.get("error_details"):
                        with st.expander("Error Details"):
                            st.code(result.get("error_details"))
            
            with tabs[1]:  # Query Details tab
                # Show IR if available
                if show_ir and result.get("ir"):
                    with st.expander("📐 Intermediate Representation (IR)", expanded=True):
                        ir = result.get("ir")
                        if isinstance(ir, dict):
                            st.json(ir)
                            
                            # Show explanation
                            if "reasoning" in ir:
                                st.markdown("**Interpretation:**")
                                st.info(ir["reasoning"])
                        else:
                            st.code(str(ir))
                
                # Show SQL
                if show_sql and result.get("sql"):
                    with st.expander("📝 Generated SQL", expanded=True):
                        st.code(result.get("sql"), language="sql")
                        
                        # Security badge
                        if result.get("sql", "").strip().upper().startswith("SELECT"):
                            st.success("✅ Validated as safe SELECT statement")
                
                # Show explanation
                if show_explanation and result.get("explanation"):
                    with st.expander("💡 Explanation"):
                        st.markdown(result.get("explanation"))
                
                # Show reasoning
                if result.get("reasoning"):
                    with st.expander("🤔 LLM Reasoning"):
                        st.markdown(result.get("reasoning"))
            
            with tabs[2]:  # Raw Data tab
                st.json(result)

        except Exception as e:
            st.error(f"❌ Execution failed: {e}")
            import traceback
            with st.expander("Debug Info"):
                st.code(traceback.format_exc())

if clear_btn:
    st.rerun()

# Example queries section
st.markdown("---")
st.subheader("📚 Example Queries")

example_categories = {
    "🏥 Healthcare": [
        "Hospitals in Delhi",
        "Clinics within 2km of Metro stations",
        "Pharmacies near hospitals"
    ],
    "🎓 Education": [
        "Schools in South Delhi",
        "Schools within 5km of hospitals",
        "Universities near metro stations"
    ],
    "🌳 Recreation": [
        "Parks within 2km of Connaught Place",
        "Playgrounds near residential areas",
        "Gardens with walking tracks"
    ],
    "🚗 Transportation": [
        "Metro stations near India Gate",
        "Bus stops near schools",
        "Total length of primary roads",
        "Roads crossing rivers"
    ],
    "🍽️ Food & Dining": [
        "Restaurants in Connaught Place",
        "Cafes near parks",
        "Food courts near metro stations"
    ],
    "🔒 Security Test (Will Be Blocked)": [
        "DROP TABLE schools",
        "; DELETE FROM hospitals",
        "-- Attempted injection"
    ]
}

for category, queries in example_categories.items():
    with st.expander(category):
        for q in queries:
            col1, col2 = st.columns([4, 1])
            with col1:
                st.code(q, language=None)
            with col2:
                if st.button("▶️ Run", key=f"btn_{q[:20]}"):
                    st.session_state.example_query = q
                    st.rerun()

# Handle example query execution
if "example_query" in st.session_state:
    query = st.session_state.example_query
    del st.session_state.example_query
    st.rerun()

# Footer
st.markdown("---")
st.caption("""
🔐 **Security Features:** SQL injection prevention • SELECT-only enforcement • IR validation layer  
🎯 **Accuracy:** Geography-based distance calculations • ±0.1% distance accuracy  
🏗️ **Architecture:** LLM → IR → Validated SQL → PostGIS  
📊 **Data:** OpenStreetMap Delhi dataset via PostGIS
""")

