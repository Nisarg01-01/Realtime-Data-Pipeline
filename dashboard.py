import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from streamlit_autorefresh import st_autorefresh

# --- Page Configuration ---
st.set_page_config(
    page_title="E-Commerce Analytics Dashboard",
    page_icon="🛒",
    layout="wide",
)

# --- Database Connection ---
# Use st.cache_resource for the connection engine
@st.cache_resource
def get_connection_engine():
    """Creates a SQLAlchemy engine to connect to PostgreSQL."""
    db_url = "postgresql://user:password@localhost:5432/ecommerce"
    return create_engine(db_url)

# Use st.cache_data for query results
@st.cache_data(ttl=10)
def run_query(query):
    """Runs a SQL query using the SQLAlchemy engine."""
    return pd.read_sql(query, get_connection_engine())

# --- Main Dashboard ---
st.title("🛒 Real-Time E-Commerce Analytics")
st_autorefresh(interval=15000, key="datarefresher")

try:
    # Fetch data
    views_df = run_query("SELECT COUNT(*) as views FROM events WHERE event_type = 'view'")
    carts_df = run_query("SELECT COUNT(*) as carts FROM events WHERE event_type = 'cart'")
    purchases_df = run_query("SELECT COUNT(*) as purchases FROM events WHERE event_type = 'purchase'")

    # --- KPIs ---
    total_views = views_df['views'][0]
    total_carts = carts_df['carts'][0]
    total_purchases = purchases_df['purchases'][0]

    st.header("Key Performance Indicators (KPIs)")
    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric(label="👀 Total Product Views", value=f"{total_views:,}")
    kpi2.metric(label="🛒 Items Added to Cart", value=f"{total_carts:,}")
    kpi3.metric(label="💳 Total Purchases", value=f"{total_purchases:,}")
    st.markdown("---")

    # --- Charts ---
    chart1, chart2 = st.columns(2)
    with chart1:
        st.subheader("🛍️ Top 10 Purchased Brands")
        top_brands_df = run_query("""
            SELECT brand, COUNT(*) AS purchase_count
            FROM events WHERE event_type = 'purchase'
            GROUP BY brand ORDER BY purchase_count DESC LIMIT 10
        """)
        st.bar_chart(top_brands_df.set_index('brand'))

    with chart2:
        st.subheader("📦 Top 10 Purchased Products")
        top_products_df = run_query("""
            SELECT product_id, COUNT(*) AS purchase_count
            FROM events WHERE event_type = 'purchase'
            GROUP BY product_id ORDER BY purchase_count DESC LIMIT 10
        """)
        top_products_df['product_id'] = top_products_df['product_id'].astype(str)
        st.bar_chart(top_products_df.set_index('product_id'))
    st.markdown("---")
    
    # --- Live Event Feed ---
    st.subheader("📡 Live Event Feed")
    live_feed_df = run_query("SELECT event_time, event_type, brand, price FROM events ORDER BY event_time DESC LIMIT 10")
    st.dataframe(live_feed_df, use_container_width=True)

except Exception as e:
    st.error(f"An error occurred: {e}")
    st.info("Please ensure your data pipeline (consumer_spark.py) and Docker services are running.")