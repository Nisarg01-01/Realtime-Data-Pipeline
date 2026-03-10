import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from streamlit_autorefresh import st_autorefresh
from agent import create_db_agent, ask_agent
import altair as alt
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="E-Commerce Analytics Dashboard",
    layout="wide",
)

@st.cache_resource
def get_connection_engine():
    """Creates a SQLAlchemy engine with optimized connection pooling."""
    db_user = os.getenv("DB_USER", "user")
    db_password = os.getenv("DB_PASSWORD", "password")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "ecommerce")
    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    # Performance: Add connection pooling
    return create_engine(
        db_url,
        pool_size=3,
        max_overflow=5,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False
    )

@st.cache_data(ttl=10)
def run_query(query):
    """Runs a SQL query using the SQLAlchemy engine with connection reuse."""
    with get_connection_engine().connect() as conn:
        return pd.read_sql(query, conn)

st.title("Real-Time E-Commerce Analytics")
refresh_interval = int(os.getenv("DASHBOARD_REFRESH_INTERVAL", "15000"))
st_autorefresh(interval=refresh_interval, key="datarefresher")

try:
    # Performance: Combine queries where possible to reduce round trips
    metrics_df = run_query("""
        SELECT 
            COUNT(*) FILTER (WHERE event_type = 'view') as views,
            COUNT(*) FILTER (WHERE event_type = 'cart') as carts,
            COUNT(*) FILTER (WHERE event_type = 'purchase') as purchases
        FROM events
    """)
    
    total_views = metrics_df['views'][0]
    total_carts = metrics_df['carts'][0]
    total_purchases = metrics_df['purchases'][0]

    st.header("Live Event Metrics")
    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric(label="Product Views", value=f"{total_views:,}")
    kpi2.metric(label="Items Added to Cart", value=f"{total_carts:,}")
    kpi3.metric(label="Total Purchases", value=f"{total_purchases:,}")
    st.markdown("---")

    st.header("User Session Analytics (from Aggregated Table)")

    summary_df = run_query("SELECT * FROM user_session_summary")

    avg_duration = summary_df['session_duration_minutes'].mean()
    total_sessions = len(summary_df)
    conversion_rate = (summary_df['has_purchase_event'].sum() / total_sessions * 100) if total_sessions > 0 else 0

    kpi_s1, kpi_s2, kpi_s3 = st.columns(3)
    kpi_s1.metric(label="Avg. Session Duration (Mins)", value=f"{avg_duration:.2f}")
    kpi_s2.metric(label="Total User Sessions", value=f"{total_sessions:,}")
    kpi_s3.metric(label="Session Conversion Rate (%)", value=f"{conversion_rate:.2f}%")

    chart1, chart2 = st.columns(2)
    with chart1:
        st.subheader("Top 10 Purchased Brands")
        top_brands_df = run_query("""
            SELECT 
                -- Capitalize the first letter of the brand name
                INITCAP(brand) AS brand, 
                COUNT(*) AS purchase_count
            FROM events 
            WHERE event_type = 'purchase'
            GROUP BY brand 
            ORDER BY purchase_count DESC 
            LIMIT 10
        """)
        st.bar_chart(top_brands_df.set_index('brand'))

    with chart2:
        st.subheader("Top 10 Purchased Products")
        top_products_df = run_query("""
            SELECT 
                -- Create a new, descriptive label
                brand || ' - ' || CAST(product_id AS VARCHAR) AS product_label, 
                COUNT(*) AS purchase_count
            FROM events 
            WHERE event_type = 'purchase'
            GROUP BY brand, product_id 
            ORDER BY purchase_count DESC 
            LIMIT 10
        """)
        # Set the new descriptive label as the index for the chart
        st.bar_chart(top_products_df.set_index('product_label'))
    st.markdown("---")

    st.subheader("Session Duration Distribution")
    duration_df = run_query("""
        SELECT
            CASE
                WHEN session_duration_minutes < 1 THEN '0-1 Mins'
                WHEN session_duration_minutes < 5 THEN '1-5 Mins'
                WHEN session_duration_minutes < 15 THEN '5-15 Mins'
                WHEN session_duration_minutes < 30 THEN '15-30 Mins'
                ELSE '30+ Mins'
            END AS duration_bucket,
            COUNT(*) AS session_count
        FROM user_session_summary
        GROUP BY duration_bucket
    """)

    sort_order = ['0-1 Mins', '1-5 Mins', '5-15 Mins', '15-30 Mins', '30+ Mins']

    chart = alt.Chart(duration_df).mark_bar().encode(
        x=alt.X('duration_bucket', sort=sort_order, title='Session Duration'),
        y=alt.Y('session_count', title='Number of Sessions')
    )

    st.altair_chart(chart, use_container_width=True)

    st.markdown("---")

    st.header("AI Business Analyst")
    st.write("Ask a question about your data in plain English:")

    # Initialize agent with error handling
    if 'agent' not in st.session_state:
        try:
            st.session_state.agent = create_db_agent()
            st.session_state.agent_error = None
        except Exception as e:
            st.session_state.agent = None
            st.session_state.agent_error = str(e)

    # Check if agent is available
    if st.session_state.agent is None:
        st.error("⚠️ AI Analyst unavailable: Google API key has been suspended or is invalid.")
        st.info("""
        **To enable AI Analyst:**
        1. Get a new Google API key from [Google AI Studio](https://makersuite.google.com/app/apikey)
        2. Update your `.env` file: `GOOGLE_API_KEY=your-new-key-here`
        3. Restart the dashboard
        
        **The rest of the dashboard works fine without AI!** 📊
        """)
    else:
        question = st.text_input("e.g., 'What are the top 5 most viewed products?'", key="agent_question")

        if st.button("Ask AI Analyst"):
            if question:
                with st.spinner("The AI Analyst is thinking..."):
                    try:
                        answer = ask_agent(st.session_state.agent, question)
                        st.success(answer)
                    except Exception as e:
                        if "CONSUMER_SUSPENDED" in str(e) or "Permission denied" in str(e):
                            st.error("⚠️ Google API key has been suspended. Please update your API key in `.env` file.")
                            st.session_state.agent = None  # Mark agent as unavailable
                        else:
                            st.error(f"Error: {e}")
            else:
                st.warning("Please enter a question.")

except Exception as e:
    st.error(f"An error occurred: {e}")
    st.info("Please ensure your data pipeline (`consumer_spark.py`) and the aggregation script (`build_aggregates.py`) have been run.")
