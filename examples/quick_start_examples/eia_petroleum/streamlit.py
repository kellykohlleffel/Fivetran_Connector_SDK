import examples.quick_start_examples.eia_petroleum.files.streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

# Page configuration
st.set_page_config(
    page_title="Crude Oil Analysis Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Get Snowflake session
session = get_active_session()

# Load data functions
def load_import_data():
    return session.sql("""
        SELECT 
            PERIOD,
            SERIES,
            VALUE,
            AREA_NAME,
            DESCRIPTION,
            UNITS
        FROM crude_imports
        WHERE _FIVETRAN_DELETED = FALSE
        AND PERIOD >= '2019-01'
    """).to_pandas()

def load_reserves_data():
    return session.sql("""
        SELECT 
            PERIOD,
            SERIES,
            VALUE,
            AREA_NAME,
            DESCRIPTION,
            UNITS
        FROM crude_reserves_production
        WHERE _FIVETRAN_DELETED = FALSE
        AND PERIOD >= '2019'
    """).to_pandas()

try:
    # Load data
    with st.spinner("Loading crude oil data..."):
        imports_data = load_import_data()
        reserves_data = load_reserves_data()

    # Dashboard Title
    st.title("🛢️ U.S. Crude Oil Analysis Dashboard")
    st.markdown("""
    Comprehensive analysis of U.S. crude oil imports and reserves data from the EIA.
    """)

    # Tabs for different analyses
    tab1, tab2 = st.tabs(["Imports Analysis", "Reserves Analysis"])

    with tab1:
        st.header("📊 Crude Oil Imports Analysis")

        # Key Metrics for Imports
        col1, col2, col3 = st.columns(3)
        
        with col1:
            latest_imports = imports_data[
                imports_data['DESCRIPTION'].str.contains('Imports of Crude Oil', case=False, na=False)
            ]['VALUE'].sum()
            st.metric("Total Crude Imports (Latest)", f"{latest_imports:,.0f} MBBL")
        
        with col2:
            avg_imports = imports_data[
                imports_data['DESCRIPTION'].str.contains('Imports of Crude Oil', case=False, na=False)
            ]['VALUE'].mean()
            st.metric("Average Monthly Imports", f"{avg_imports:,.0f} MBBL")
        
        with col3:
            unique_areas = imports_data['AREA_NAME'].nunique()
            st.metric("Import Regions", str(unique_areas))

        # Regional Imports Analysis
        st.subheader("Regional Crude Oil Imports")
        st.caption(f"Data from {imports_data['PERIOD'].min()} to {imports_data['PERIOD'].max()}")
        region_data = imports_data[
            imports_data['DESCRIPTION'].str.contains('Imports of Crude Oil', case=False, na=False)
        ].groupby('AREA_NAME')['VALUE'].sum().reset_index()
        
        region_chart = alt.Chart(region_data).mark_bar().encode(
            x=alt.X('VALUE:Q', title='Total Imports (MBBL)'),
            y=alt.Y('AREA_NAME:N', sort='-x', title='Region'),
            color=alt.Color('VALUE:Q', scale=alt.Scale(scheme='viridis')),
            tooltip=[
                alt.Tooltip('AREA_NAME:N', title='Region'),
                alt.Tooltip('VALUE:Q', title='Total Imports', format=',')
            ]
        ).properties(height=300)
        
        st.altair_chart(region_chart, use_container_width=True)

        # Time Series Analysis
        st.subheader("Import Trends Over Time")
        st.caption(f"Data from {imports_data['PERIOD'].min()} to {imports_data['PERIOD'].max()}")
        time_data = imports_data[
            imports_data['DESCRIPTION'].str.contains('Imports of Crude Oil', case=False, na=False)
        ].groupby('PERIOD')['VALUE'].sum().reset_index()
        
        line_chart = alt.Chart(time_data).mark_line(point=True).encode(
            x=alt.X('PERIOD:T', title='Date'),
            y=alt.Y('VALUE:Q', title='Imports (MBBL)'),
            tooltip=[
                alt.Tooltip('PERIOD:T', title='Date'),
                alt.Tooltip('VALUE:Q', title='Imports', format=',')
            ]
        ).properties(height=400)
        
        st.altair_chart(line_chart, use_container_width=True)

    with tab2:
        st.header("📈 Crude Oil Reserves Analysis")

        # Key Metrics for Reserves
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_reserves = reserves_data[
                reserves_data['DESCRIPTION'].str.contains('Proved Reserves', case=False, na=False)
            ]['VALUE'].sum()
            st.metric("Total Proved Reserves", f"{total_reserves:,.0f} MMBBL")
        
        with col2:
            total_production = reserves_data[
                reserves_data['DESCRIPTION'].str.contains('Production from Reserves', case=False, na=False)
            ]['VALUE'].sum()
            st.metric("Total Production", f"{total_production:,.0f} MMBBL")
        
        with col3:
            avg_reserves = reserves_data[
                reserves_data['DESCRIPTION'].str.contains('Proved Reserves', case=False, na=False)
            ]['VALUE'].mean()
            st.metric("Average Reserves per Region", f"{avg_reserves:,.0f} MMBBL")

        # Top States by Reserves
        st.subheader("Top States by Proved Reserves")
        st.caption(f"Data from {reserves_data['PERIOD'].min()} to {reserves_data['PERIOD'].max()}")
        state_reserves = reserves_data[
            reserves_data['DESCRIPTION'].str.contains('Proved Reserves', case=False, na=False)
        ].nlargest(10, 'VALUE')
        
        states_chart = alt.Chart(state_reserves).mark_bar().encode(
            x=alt.X('VALUE:Q', title='Proved Reserves (MMBBL)'),
            y=alt.Y('AREA_NAME:N', sort='-x', title='State'),
            color=alt.Color('VALUE:Q', scale=alt.Scale(scheme='blues')),
            tooltip=[
                alt.Tooltip('AREA_NAME:N', title='State'),
                alt.Tooltip('VALUE:Q', title='Proved Reserves', format=',')
            ]
        ).properties(height=300)
        
        st.altair_chart(states_chart, use_container_width=True)

        # Reserves vs Production Analysis
        st.subheader("Reserves vs Production Analysis")
        st.caption(f"Data from {reserves_data['PERIOD'].min()} to {reserves_data['PERIOD'].max()}")
        
        # Create comparison data
        reserves_by_state = reserves_data[
            reserves_data['DESCRIPTION'].str.contains('Proved Reserves', case=False, na=False)
        ].groupby('AREA_NAME')['VALUE'].sum()
        
        production_by_state = reserves_data[
            reserves_data['DESCRIPTION'].str.contains('Production from Reserves', case=False, na=False)
        ].groupby('AREA_NAME')['VALUE'].sum()
        
        comparison_data = pd.DataFrame({
            'State': reserves_by_state.index,
            'Reserves': reserves_by_state.values,
            'Production': production_by_state
        }).dropna()
        
        comparison_data = comparison_data.melt(
            id_vars=['State'],
            var_name='Metric',
            value_name='Value'
        )
        
        comparison_chart = alt.Chart(comparison_data).mark_bar().encode(
            x=alt.X('State:N', title='State'),
            y=alt.Y('Value:Q', title='MMBBL'),
            color=alt.Color('Metric:N', scale=alt.Scale(scheme='set2')),
            tooltip=[
                alt.Tooltip('State:N', title='State'),
                alt.Tooltip('Metric:N', title='Metric'),
                alt.Tooltip('Value:Q', title='Value', format=',')
            ]
        ).properties(height=400)
        
        st.altair_chart(comparison_chart, use_container_width=True)

        # Detailed Data View
        if st.checkbox("Show Raw Data"):
            st.dataframe(reserves_data)

except Exception as e:
    st.error(f"An error occurred while loading the dashboard: {str(e)}")
    st.error("Please ensure you have the correct database and schema context set in Streamlit in Snowflake.")