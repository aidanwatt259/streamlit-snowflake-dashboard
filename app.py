import streamlit as st
import pandas as pd
import numpy as np

# -----------------------------
# 1. Get Snowflake Lab Session
# -----------------------------
from snowflake.snowpark.context import get_active_session
session = get_active_session()

# Load tables
records = session.table("LA_PERMIT_DATA.PUBLIC.PERMIT_RECORDS").to_pandas()
census_tracts = session.table("LA_PERMIT_DATA.PUBLIC.CENSUS_TRACTS").to_pandas()
contractors = session.table("LA_PERMIT_DATA.PUBLIC.MASTER_LICENSE").to_pandas()

# -----------------------------
# 2. Preprocessing
# -----------------------------
records['ISSUE_DATE'] = pd.to_datetime(records['ISSUE_DATE'], errors='coerce')
records['YEAR'] = records['ISSUE_DATE'].dt.year

records['VALUATION'] = (
    records['VALUATION'].astype(str)
    .str.replace("$", "", regex=False)
    .str.replace(",", "", regex=False)
)
records['VALUATION'] = pd.to_numeric(records['VALUATION'], errors='coerce')

merged_df = records.merge(
    contractors[['BUSINESS_NAME', 'BUSINESS_TYPE']],
    left_on='CONTRACTOR_BUSINESS_NAME',
    right_on='BUSINESS_NAME',
    how='left'
)

# -----------------------------
# 3. Sidebar Filters
# -----------------------------
st.sidebar.title("Filters")

year_range = st.sidebar.slider(
    "Select Year Range",
    min_value=2013,
    max_value=2023,
    value=(2013, 2023)
)

permit_options = records['PERMIT_TYPE'].dropna().unique().tolist()
selected_permits = st.sidebar.multiselect("Permit Types", permit_options, default=permit_options)

business_options = contractors['BUSINESS_TYPE'].dropna().unique().tolist()
selected_businesses = st.sidebar.multiselect("Business Types", business_options, default=business_options)

filtered_df = merged_df[
    (merged_df['YEAR'] >= year_range[0]) &
    (merged_df['YEAR'] <= year_range[1]) &
    (merged_df['PERMIT_TYPE'].isin(selected_permits)) &
    (merged_df['BUSINESS_TYPE'].isin(selected_businesses))
]

# -----------------------------
# 4. Dashboard Title
# -----------------------------
st.title("Los Angeles Building Permit Dashboard")

# -----------------------------
# 5. Tabs for Visuals
# -----------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "Contractors by Permit Type",
    "Contractors vs Avg Valuation",
    "Permit Counts Over Time",
    "Summary Statistics"
])

# -----------------------------
# Tab 1: Contractors by Permit Type
# -----------------------------
with tab1:
    st.subheader("Contractors by Permit Type")
    
    df_permit = (
        filtered_df.groupby("PERMIT_TYPE")
        .agg(
            num_contractors=('CONTRACTOR_BUSINESS_NAME', 'nunique'),
            num_permits=('PERMIT_TYPE', 'count')
        )
        .reset_index()
        .sort_values("num_contractors", ascending=False)
    )
    
    # Display the table
    #st.dataframe(df_permit)
    
    # Display bar chart
    st.bar_chart(df_permit.set_index("PERMIT_TYPE")["num_contractors"])
# -----------------------------
# Tab 2: Contractors vs Average Valuation
# -----------------------------
with tab2:
    st.subheader("Contractors vs Average Valuation")
    
    # Aggregate by census tract
    df_scatter = filtered_df.groupby("CENSUS_TRACT").agg(
        num_contractors=('CONTRACTOR_BUSINESS_NAME', 'nunique'),
        avg_cost=('VALUATION', 'mean')
    ).reset_index()
    
    st.write("Scatter Chart: Number of Contractors vs Average Valuation")

    # Use Streamlit's vega_lite_chart for a proper scatter plot
    st.vega_lite_chart(
        df_scatter,
        {
            "mark": "circle",
            "encoding": {
                "x": {"field": "num_contractors", "type": "quantitative", "title": "Number of Contractors"},
                "y": {"field": "avg_cost", "type": "quantitative", "title": "Average Valuation"},
                "size": {"field": "avg_cost", "type": "quantitative"},
                "color": {"field": "avg_cost", "type": "quantitative", "scale": {"scheme": "reds"}}
            }
        },
        use_container_width=True
    )

# -----------------------------
# Tab 3: Permit Counts Over Time
# -----------------------------
with tab3:
    st.subheader("Permit Counts Over Time")
    
    # Aggregate permit counts by year
    df_time = (
        filtered_df.groupby("YEAR")
        .agg(permit_count=('PERMIT_TYPE', 'count'))
        .reset_index()
        .sort_values("YEAR")
    )
    
    # Rename column for clarity in chart legend
    df_time.rename(columns={"permit_count": "Number of Permits"}, inplace=True)
    
    # Set YEAR as index for line_chart
    st.line_chart(
        df_time.set_index("YEAR"),
        use_container_width=True
    )

    # Optional: show raw numbers below the chart
    st.write("Permit counts per year:")
    st.dataframe(df_time)

# -----------------------------
# Tab 4: Summary Statistics
# -----------------------------
with tab4:
    st.subheader("Summary Statistics")
    
    # Use plain dataframe display
    st.dataframe(filtered_df.describe(include='all'))