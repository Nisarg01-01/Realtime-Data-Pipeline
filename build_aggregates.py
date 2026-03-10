# build_aggregates.py
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import time

load_dotenv()

def main():
    db_user = os.getenv("DB_USER", "user")
    db_password = os.getenv("DB_PASSWORD", "password")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "ecommerce")
    
    print("Connecting to the PostgreSQL database...")
    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    # Performance: Add connection pooling and optimizations
    engine = create_engine(
        db_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        echo=False
    )
    
    start_time = time.time()

    sessionization_query = """
    WITH session_events AS (
        SELECT
            user_session,
            user_id,
            event_time,
            event_type,
            ROW_NUMBER() OVER(PARTITION BY user_session ORDER BY event_time) as event_order
        FROM
            events
    ),
    session_start_end AS (
        SELECT
            user_session,
            user_id,
            MIN(event_time) as session_start,
            MAX(event_time) as session_end
        FROM
            session_events
        GROUP BY
            user_session, user_id
    ),
    session_details AS (
        SELECT
            user_session,
            COUNT(*) as total_events,
            SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) > 0 as has_cart_event,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) > 0 as has_purchase_event
        FROM
            events
        GROUP BY
            user_session
    )
    SELECT
        sse.user_session,
        sse.user_id,
        sse.session_start,
        sse.session_end,
        EXTRACT(EPOCH FROM (sse.session_end - sse.session_start)) / 60.0 as session_duration_minutes,
        sd.total_events,
        sd.has_cart_event,
        sd.has_purchase_event
    FROM
        session_start_end sse
    JOIN
        session_details sd ON sse.user_session = sd.user_session
    ORDER BY
        sse.session_start DESC;
    """

    print("Running advanced SQL query to build session summary...")
    # Execute the query and store the result in a Pandas DataFrame
    # Performance: Use connection directly for better control
    with engine.connect() as conn:
        summary_df = pd.read_sql(sessionization_query, conn)
    
    query_time = time.time() - start_time
    print(f"Query completed in {query_time:.2f}s. Aggregated {len(summary_df)} sessions.")

    # Write the aggregated data to a new table in PostgreSQL
    print("Writing summary data to table 'user_session_summary'...")
    write_start = time.time()
    
    # Performance: Use method='multi' for faster bulk inserts
    summary_df.to_sql(
        'user_session_summary',
        engine,
        if_exists='replace',  # 'replace' will drop the table if it exists and create a new one
        index=False,
        method='multi',  # Faster bulk insert
        chunksize=1000  # Insert in chunks of 1000 rows
    )
    
    write_time = time.time() - write_start
    total_time = time.time() - start_time
    print(f"Write completed in {write_time:.2f}s")
    print(f"\n--- Aggregation complete in {total_time:.2f}s! ---")
    print(f"Performance: {len(summary_df)/total_time:.0f} sessions/sec")
    print("\n--- Sample of the new summary table: ---")
    print(summary_df.head())


if __name__ == "__main__":
    main()