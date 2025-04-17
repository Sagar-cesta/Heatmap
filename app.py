import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.snowpark.context import get_active_session

# Map full state names → 2-letter codes
us_state_abbr = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
    'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
    'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
    'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
    'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
    'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
    'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
    'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
    'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
    'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
    'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT',
    'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
    'Wisconsin': 'WI', 'Wyoming': 'WY'
}

# Start Snowflake session
session = get_active_session()

# Query: category counts per state
query1 = """
    SELECT STATE, CATEGORY, COUNT(*) AS CATEGORY_COUNT
    FROM MEDFAIR_DATABASE.PUBLIC.PROCESSED_MASTER_FILE_CATEGORY
    WHERE STATE IS NOT NULL AND CATEGORY IS NOT NULL
    GROUP BY STATE, CATEGORY
"""
df = session.sql(query1).to_pandas()

# Total per state
totals = df.groupby("STATE")["CATEGORY_COUNT"].sum().reset_index(name="TOTAL_COUNT")

# Hover text per state
hover = df.groupby("STATE").apply(
    lambda x: "<br>".join(f"{r['CATEGORY']}: {r['CATEGORY_COUNT']}" for _, r in x.iterrows())
).reset_index(name="HOVER_TEXT")

# Merge + map
data = totals.merge(hover, on="STATE")
data["STATE_CODE"] = data["STATE"].map(us_state_abbr)
data = data.dropna(subset=["STATE_CODE"])

# Choropleth map
fig = px.choropleth(
    data,
    locations="STATE_CODE",
    locationmode="USA-states",
    color="TOTAL_COUNT",
    hover_name="STATE",
    hover_data={"HOVER_TEXT": True, "STATE_CODE": False, "TOTAL_COUNT": False},
    scope="usa",
    color_continuous_scale="Turbo",
    title="📍 Hover on a State to See CATEGORY Counts"
)

st.plotly_chart(fig, use_container_width=True)

# Dropdown: Select state
selected_state = st.selectbox(
    "👇 Select a state to view average NEGOTIATED_RATE:",
    options=data["STATE"].sort_values().unique()
)

# Query: average NEGOTIATED_RATE for selected state
query2 = f"""
    SELECT ROUND(AVG(NEGOTIATED_RATE), 2) AS AVG_NEGOTIATED_RATE
    FROM MEDFAIR_DATABASE.PUBLIC.PROCESSED_MASTER_FILE_CATEGORY
    WHERE STATE = '{selected_state}'
"""
avg_rate = session.sql(query2).to_pandas().iloc[0, 0]

# Display result
st.markdown(f"📌 **Average Negotiated Rate for `{selected_state}`:** `${avg_rate}`")
