import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

# Change this list as needed to add/remove model capabilities.
MODELS = [
    "llama3.2-3b",
    "claude-3-5-sonnet",
    "mistral-large2",
    "llama3.1-8b",
    "llama3.1-405b",
    "llama3.1-70b",
    "mistral-7b",
    "jamba-1.5-large",
    "mixtral-8x7b",
    "reka-flash",
    "gemma-7b"
]

# Page configuration
st.set_page_config(
    page_title="Oura Gen AI Insights",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Get Snowflake session
session = get_active_session()

# Load data functions
def load_daily_activity():
    return session.sql("""
        SELECT 
            DATE, 
            STEPS, 
            TOTAL_CALORIES, 
            ACTIVE_CALORIES
        FROM daily_activity
        WHERE _FIVETRAN_DELETED = FALSE
    """).to_pandas()

def load_daily_sleep():
    return session.sql("""
        SELECT 
            DATE, 
            TOTAL_SLEEP_DURATION, 
            DEEP_SLEEP_DURATION, 
            LIGHT_SLEEP_DURATION, 
            REM_SLEEP_DURATION, 
            SLEEP_EFFICIENCY
        FROM daily_sleep
        WHERE _FIVETRAN_DELETED = FALSE
    """).to_pandas()

def generate_ai_analysis(activity_summary, sleep_summary, model_name):
    """
    Uses Snowflake Cortex to generate AI-driven insights with structured analysis.
    """
    cortex_prompt = f"""
    You are a health analytics expert reviewing data from a fitness tracker. Based on the following health data:

    - **Activity Summary**: {activity_summary}
    - **Sleep Summary**: {sleep_summary}

    Provide a structured and actionable analysis with:
    1️⃣ **Key Observations** (highlight patterns, trends, and anomalies)
    2️⃣ **Correlations** (e.g., does increased activity improve sleep efficiency?)
    3️⃣ **Recommendations** (customized health tips)
    4️⃣ **AI Confidence Score** (rate the confidence of each insight from 1-100%)

    📌 Ensure the response is well-structured with bullet points, emojis for clarity, and easy-to-understand language.

    Example format:
    **Key Observations**
    - 📉 Your steps have decreased by 10% over the past week.
    - ⏰ Average sleep duration is below 7 hours per night.

    **Correlations**
    - 🔄 Days with higher activity show improved sleep efficiency (+12%).
    - 📊 Lack of deep sleep correlates with lower active calorie burn.

    **Recommendations**
    - 🏃 Increase daily steps by 1,000 to improve sleep recovery.
    - ☀️ Try morning sunlight exposure to improve REM sleep.
    - 💧 Hydration levels might be impacting sleep efficiency.

    **AI Confidence Score**
    🔹 85% - Based on historical trends and scientific correlations.
    """

    query = """
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        ?,
        ?
    ) AS response
    """
    result = session.sql(query, params=[model_name, cortex_prompt]).collect()
    return result[0]["RESPONSE"] if result else "No response generated."


# Load data
try:
    with st.spinner("Loading Oura data..."):
        activity_data = load_daily_activity()
        sleep_data = load_daily_sleep()

    # Dashboard Title
    # Imgur-hosted Oura logo (Replace with your actual Imgur image URL)
    oura_logo_url = "https://i.imgur.com/Jqpmg5L.png"
    
    col1, col2 = st.columns([0.08, 0.99])  # Adjust column width for balance
    
    with col1:
        st.image(oura_logo_url, width=70)  # Adjust width as needed
    
    with col2:
        st.title("Oura API Gen AI Health Insights")
    st.markdown("""
    Gain **Snowflake Cortex Gen AI-powered health insights** based on daily **activity** and **sleep** patterns.
    """)

    # Tabs for different analyses
    tab1, tab2, tab3 = st.tabs(["Daily Activity", "Daily Sleep", "Cortex Health Apps"])

    ### Daily Activity Analysis ###
    with tab1:
        st.header("🏃 Daily Activity Overview")

        # Calculate key metrics
        total_steps = activity_data["STEPS"].sum()
        best_steps = activity_data["STEPS"].max()
        avg_steps = activity_data["STEPS"].mean()
        total_calories = activity_data["TOTAL_CALORIES"].sum()
        best_active_calories = activity_data["ACTIVE_CALORIES"].max()
        avg_active_calories = activity_data["ACTIVE_CALORIES"].mean()

        # Display Metrics
        st.markdown("### **Key Activity Metrics**")
        # Display date range for Daily Activity
        st.caption(f"Data from {activity_data['DATE'].min()} to {activity_data['DATE'].max()}")


        col1, col2, col3, col4, col5, col6 = st.columns(6)

        with col1:
            st.metric("🏆 Total Steps", f"{total_steps:,.0f}")

        with col2:
            st.metric("🏅 Best Steps in a Day", f"{best_steps:,.0f}")

        with col3:
            st.metric("🚶 Avg Steps/Day", f"{avg_steps:,.0f}")

        with col4:
            st.metric("🔥 Total Calories Burned", f"{total_calories:,.0f}")

        with col5:
            st.metric("⚡ Best Active Calorie Burn", f"{best_active_calories:,.0f}")

        with col6:
            st.metric("🔥 Avg Active Calorie Burn", f"{avg_active_calories:,.0f}")

        # Steps Over Time
        st.subheader("📈 Steps Trend Over Time")
        steps_chart = alt.Chart(activity_data).mark_line(point=True).encode(
            x=alt.X("DATE:T", title="Date"),
            y=alt.Y("STEPS:Q", title="Steps"),
            tooltip=["DATE", "STEPS"]
        ).properties(height=400)

        st.altair_chart(steps_chart, use_container_width=True)

        # Calories Breakdown
        st.subheader("🔥 Calories Breakdown")
        activity_data_melted = activity_data.melt(
            id_vars=["DATE"],
            value_vars=["TOTAL_CALORIES", "ACTIVE_CALORIES"],
            var_name="Calorie Type",
            value_name="Calories"
        )

        calorie_chart = alt.Chart(activity_data_melted).mark_bar().encode(
            x=alt.X("DATE:T", title="Date"),
            y=alt.Y("Calories:Q", title="Calories Burned"),
            color=alt.Color("Calorie Type:N", scale=alt.Scale(scheme="dark2")),
            tooltip=["DATE", "Calories"]
        ).properties(height=400)

        st.altair_chart(calorie_chart, use_container_width=True)

    ### Daily Sleep Analysis ###
    with tab2:
        st.header("😴 Sleep Patterns & Quality")

        # Calculate Daily Sleep Metrics
        # Convert duration columns from seconds to hours
        sleep_data["TOTAL_SLEEP_HOURS"] = sleep_data["TOTAL_SLEEP_DURATION"] / 3600
        sleep_data["DEEP_SLEEP_HOURS"] = sleep_data["DEEP_SLEEP_DURATION"] / 3600
        sleep_data["REM_SLEEP_HOURS"] = sleep_data["REM_SLEEP_DURATION"] / 3600

        # Group by DATE to get correct per-day values
        daily_sleep = sleep_data.groupby("DATE").agg({
            "TOTAL_SLEEP_HOURS": ["mean", "max"],
            "DEEP_SLEEP_HOURS": ["mean", "max"],
            "REM_SLEEP_HOURS": ["mean", "max"]
        })

        # Extract values correctly (avoid applying .mean() twice)
        avg_total_sleep_per_day = daily_sleep["TOTAL_SLEEP_HOURS"]["mean"].mean()
        max_total_sleep_per_day = daily_sleep["TOTAL_SLEEP_HOURS"]["max"].max()

        avg_deep_sleep_per_day = daily_sleep["DEEP_SLEEP_HOURS"]["mean"].mean()
        max_deep_sleep_per_day = daily_sleep["DEEP_SLEEP_HOURS"]["max"].max()

        avg_rem_sleep_per_day = daily_sleep["REM_SLEEP_HOURS"]["mean"].mean()
        max_rem_sleep_per_day = daily_sleep["REM_SLEEP_HOURS"]["max"].max()

        avg_efficiency = sleep_data["SLEEP_EFFICIENCY"].mean() * 100

        # Display Sleep Metrics
        st.markdown("### **Key Sleep Metrics (Per Day)**")
        # Display date range for Daily Sleep
        st.caption(f"Data from {sleep_data['DATE'].min()} to {sleep_data['DATE'].max()}")


        col1, col2, col3, col4, col5, col6 = st.columns(6)

        with col1:
            st.metric("💤 Avg Total Sleep/Day (hrs)", f"{avg_total_sleep_per_day:.1f}")
        
        with col2:
            st.metric("😴 Max Total Sleep/Day (hrs)", f"{max_total_sleep_per_day:.1f}")
        
        with col3:
            st.metric("💙 Avg Deep Sleep/Day (hrs)", f"{avg_deep_sleep_per_day:.1f}")
        
        with col4:
            st.metric("🛌 Max Deep Sleep/Day (hrs)", f"{max_deep_sleep_per_day:.1f}")
        
        with col5:
            st.metric("🌙 Avg REM Sleep/Day (hrs)", f"{avg_rem_sleep_per_day:.1f}")
        
        with col6:
            st.metric("✨ Max REM Sleep/Day (hrs)", f"{max_rem_sleep_per_day:.1f}")

        # Sleep Efficiency
        st.subheader("📊 Sleep Efficiency Trends")
        efficiency_chart = alt.Chart(sleep_data).mark_line(point=True).encode(
            x=alt.X("DATE:T", title="Date"),
            y=alt.Y("SLEEP_EFFICIENCY:Q", title="Efficiency"),
            tooltip=["DATE", "SLEEP_EFFICIENCY"]
        ).properties(height=400)

        st.altair_chart(efficiency_chart, use_container_width=True)

        # Convert sleep stage durations from seconds to hours
        sleep_data["DEEP_SLEEP_HOURS"] = sleep_data["DEEP_SLEEP_DURATION"] / 3600
        sleep_data["LIGHT_SLEEP_HOURS"] = sleep_data["LIGHT_SLEEP_DURATION"] / 3600
        sleep_data["REM_SLEEP_HOURS"] = sleep_data["REM_SLEEP_DURATION"] / 3600

        # Melt the DataFrame for Altair visualization
        sleep_data_melted = sleep_data.melt(
            id_vars=["DATE"],
            value_vars=["DEEP_SLEEP_HOURS", "LIGHT_SLEEP_HOURS", "REM_SLEEP_HOURS"],
            var_name="Sleep Stage",
            value_name="Duration (hours)"
        )

        # Sleep Stages Over Time Chart (Now in Hours)
        st.subheader("💤 Sleep Stages Over Time (in Hours)")
        sleep_chart = alt.Chart(sleep_data_melted).mark_area(opacity=0.7).encode(
            x=alt.X("DATE:T", title="Date"),
            y=alt.Y("Duration (hours):Q", title="Sleep Duration (hours)"),
            color=alt.Color("Sleep Stage:N", scale=alt.Scale(scheme="viridis")),
            tooltip=["DATE", "Duration (hours)"]
        ).properties(height=400)

        st.altair_chart(sleep_chart, use_container_width=True)

    ### Snowflake Cortex-Powered Health Apps ###
    with tab3:
        # Left-justified title without unnecessary column structure
        st.markdown(
            """
            <h3 style="text-align: left; margin-top: -18px;">Snowflake Cortex Health Apps</h3>
            """,
            unsafe_allow_html=True
        )

        # Display date range for Cortex Health Insights
        st.caption(f"Data from {activity_data['DATE'].min()} to {activity_data['DATE'].max()}")

    
        # Adjust column widths for better alignment and compact layout
        col1, col2, col3 = st.columns([0.1, 0.2, 0.1])  # Adjusted for proper alignment
    
        with col1:
            model_name = st.selectbox("**Select a Cortex Model:**", MODELS, key="model_name", index=0)
    
        with col2:
            selected_app = st.selectbox("**Select a Health Path:**", [
                "📌 Generate Cortex Health Insights",
                "📈 Cortex-Powered Health Forecasting",
                "🚨 Cortex-Detected Health Anomalies"
            ], key="health_app", index=0)
    
        with col3:
            st.markdown("<br>", unsafe_allow_html=True)  # Empty space to adjust alignment for the "Go" button
            go_button = st.button("Go", key="go_button", help="Run selected health app")
    
        # ✅ Ensure activity_summary and sleep_summary are always available
        activity_summary = f"""
        - Total steps: {total_steps:,.0f}
        - Best steps in a day: {best_steps:,.0f}
        - Avg steps/day: {avg_steps:,.0f}
        - Total calories burned: {total_calories:,.0f}
        - Best active calorie burn: {best_active_calories:,.0f}
        - Avg active calorie burn: {avg_active_calories:,.0f}
        """
    
        sleep_summary = f"""
        - Avg total sleep per day: {avg_total_sleep_per_day:.1f} hours
        - Max total sleep per day: {max_total_sleep_per_day:.1f} hours
        - Avg deep sleep per day: {avg_deep_sleep_per_day:.1f} hours
        - Max deep sleep per day: {max_deep_sleep_per_day:.1f} hours
        - Avg REM sleep per day: {avg_rem_sleep_per_day:.1f} hours
        - Max REM sleep per day: {max_rem_sleep_per_day:.1f} hours
        - Avg sleep efficiency: {avg_efficiency:.1f}%
        """
    
        # ✅ Process the selected action
        if go_button:
            with st.spinner("Processing..."):
                if selected_app == "📌 Generate Cortex Health Insights":
                    ai_analysis = generate_ai_analysis(activity_summary, sleep_summary, model_name)
                    st.markdown(f"**📌 Cortex-Generated Health Insights:**\n\n{ai_analysis}")
    
                elif selected_app == "📈 Cortex-Powered Health Forecasting":
                    forecast_prompt = f"""
                    Based on the past activity and sleep trends, generate a **7-day forecast** for:
                    - Estimated **step count trends** for next week
                    - Expected **calories burned per day**
                    - Projected **deep sleep duration** trends
    
                    Ensure the forecast is realistic and follows prior trends.
                    """
    
                    query = """
                    SELECT SNOWFLAKE.CORTEX.COMPLETE(
                        ?,
                        ?
                    ) AS response
                    """
                    result = session.sql(query, params=[model_name, forecast_prompt]).collect()
                    forecast_response = result[0]["RESPONSE"] if result else "No response generated."
                    st.markdown(f"**📈 Health Forecast Summary:**\n\n{forecast_response}")
    
                elif selected_app == "🚨 Cortex-Detected Health Anomalies":
                    anomaly_prompt = f"""
                    You are analyzing a user's historical health data to detect real anomalies.
                    
                    Use the provided activity and sleep summaries to:
                    - Detect **specific days** with unusual **drops or spikes** in step count, calorie burn, or sleep efficiency.
                    - Highlight **the exact date** of the anomaly.
                    - Explain **possible causes** for each anomaly in **one sentence**.
                    - Suggest **one actionable step** to fix or improve.
    
                    Example Format:
                    **📅 Date:** Jan 15, 2025  
                    - **🔻 Step Count Drop**: Down by 40% (5,200 steps instead of 8,700)
                      - *Possible Cause:* High workload and sedentary behavior.
                      - *Fix:* Add a short 10-minute walk at lunchtime.
                    **📅 Date:** Jan 18, 2025  
                    - **📈 High Calorie Burn**: 3,500 calories burned (50% above average)
                      - *Possible Cause:* Intense workout session.
                      - *Fix:* Increase hydration and protein intake.
    
                    User Data:
                    - **Activity Summary**: {activity_summary}
                    - **Sleep Summary**: {sleep_summary}
                    """
    
                    query = """
                    SELECT SNOWFLAKE.CORTEX.COMPLETE(
                        ?,
                        ?
                    ) AS response
                    """
                    result = session.sql(query, params=[model_name, anomaly_prompt]).collect()
                    anomaly_response = result[0]["RESPONSE"] if result else "No anomalies detected."
                    st.markdown(f"**⚠️ Health Findings:**\n\n{anomaly_response}")

except Exception as e:
    st.error(f"An error occurred while loading the dashboard: {str(e)}")
    st.error("Please ensure you have the correct database and schema context set in Streamlit in Snowflake.")
