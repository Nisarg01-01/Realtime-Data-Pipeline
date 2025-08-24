# build_aggregates.py
import pandas as pd
from sqlalchemy import create_engine

def main():

    print("Connecting to the PostgreSQL database...")
    db_url = "postgresql://user:password@localhost:5432/ecommerce"
    engine = create_engine(db_url)

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
    summary_df = pd.read_sql(sessionization_query, engine)

    print(f"Successfully aggregated {len(summary_df)} sessions.")

    # Write the aggregated data to a new table in PostgreSQL
    print("Writing summary data to new table 'user_session_summary'...")
    summary_df.to_sql(
        'user_session_summary',
        engine,
        if_exists='replace', # 'replace' will drop the table if it exists and create a new one
        index=False
    )

    print("--- Aggregation build complete! ---")
    print("\n--- Sample of the new summary table: ---")
    print(summary_df.head())


if __name__ == "__main__":
    main()