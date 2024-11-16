import streamlit as st
import pandas as pd
from pinotdb import connect
import plotly.figure_factory as ff
import plotly.graph_objects as go
import plotly.express as px

st.title("✨ Stock-Trade User Interactions")
st.write(
    "For the Midterm Examination in DADS6005 Data Streaming and Realtime Analytics "
)

# ตั้งค่าเพื่อให้ dashboard ใช้พื้นที่เต็มหน้าจอ
# st.set_page_config(page_title="DADS6005 Realtime Dashboard", layout="wide")

# Function for connecting to Druid
def create_connection():
    conn = connect(host='13.212.62.78', port=8099, path='/query/sql', scheme='http')
    return conn

# สร้างการเชื่อมต่อกับ Druid
conn = create_connection()

### Qurey 4 
query4 = """
SELECT 
    gender,
    regionid
FROM 
    users_table;
"""
curs4 = conn.cursor()
curs4.execute(query4)
result4 = curs4.fetchall()
df4 = pd.DataFrame(result4, columns=['gender', 'regionid'])

# Group data to count occurrences of gender-region combinations
heatmap_data4 = df4.groupby(["gender", "regionid"]).size().reset_index(name="count")

# Pivot the data to create a matrix for the heatmap
heatmap_matrix4 = heatmap_data4.pivot(index="gender", columns="regionid", values="count").fillna(0)

# Create the heatmap
fig4 = go.Figure(
    data=go.Heatmap(  # Use 'data' instead of 'data4'
        z=heatmap_matrix4.values,  # Heatmap values
        x=heatmap_matrix4.columns,  # Regions on the X-axis
        y=heatmap_matrix4.index,  # Genders on the Y-axis
        colorscale=px.colors.sequential.Cividis_r  # Reverse Cividis color scheme
    )
)

# Customize layout
fig4.update_layout(
    title="User Distribution by Gender and Region",
    xaxis_title="Region",
    yaxis_title="Gender"
)

# Display the heatmap in Streamlit
st.plotly_chart(fig4)

autumn_colorscale = [
    [0.0, "yellow"],  # Start with yellow
    [0.5, "orange"],  # Transition to orange
    [1.0, "red"]      # End with red
]

## Query 1 Transaction count by Stock
query1 = """
SELECT 
    userid,
    COUNT(symbol) AS Transaction_Count
FROM 
    Stock_stream
WHERE 
    side = 'BUY'
GROUP BY 
    userid
ORDER BY 
    Transaction_Count DESC;
"""

curs1 = conn.cursor()
curs1.execute(query1)
result1 = curs1.fetchall()
df1 = pd.DataFrame(result1, columns=['userid', 'Transaction_Count'])
# print(df1)

# Create a horizontal bar chart
fig1 = go.Figure(
    go.Bar(
        x=df1["Transaction_Count"],
        y=df1["userid"],
        orientation='h',  # Horizontal bar chart
        text=df1["Transaction_Count"],  # Display amount as text
        textposition='outside',  # Position text outside the bars 
        marker=dict(
            color=df1["Transaction_Count"],  # Use Transaction_Count for color mapping
            colorscale=autumn_colorscale    # Apply the autumn color scale
        ) 
    )
)

# Customize layout
fig1.update_layout(
    title="User Ranking in Purchase Stock_Trade",
    xaxis_title="Transaction Count",
    yaxis_title="User ID",
    yaxis=dict(categoryorder='total ascending'),  # Order bars by value
)

# Streamlit app
st.plotly_chart(fig1)

## Query 2 Transaction count by Stock
query2 = """
SELECT 
    userid,
    COUNT(symbol) AS Transaction_Count
FROM 
    Stock_stream
WHERE 
    side = 'SELL'
GROUP BY 
    userid
ORDER BY 
    Transaction_Count DESC;
"""

curs2 = conn.cursor()
curs2.execute(query2)
result2 = curs2.fetchall()
df2 = pd.DataFrame(result2, columns=['userid', 'Transaction_Count'])
# print(df2)

# Create a horizontal bar chart
fig2 = go.Figure(
    go.Bar(
        x=df2["Transaction_Count"],
        y=df2["userid"],
        orientation='h', # Horizontal bar chart
        text=df2["Transaction_Count"],  # Display amount as text
        textposition='outside',  # Position text outside the bars  
    )
)

# Customize layout
fig2.update_layout(
    title="User Ranking in Selling Stock_Trade",
    xaxis_title="Transaction Count",
    yaxis_title="User ID",
    yaxis=dict(categoryorder='total ascending'),  # Order bars by value
)

# Streamlit app
st.plotly_chart(fig2)


## Query 3 User Interested Stock
query3 = """
SELECT 
    symbol,
    COUNT(DISTINCT userid) AS user_count
FROM 
    Stock_stream
GROUP BY 
    symbol
ORDER BY 
    user_count DESC;
"""

curs3 = conn.cursor()
curs3.execute(query3)
result3 = curs3.fetchall()
df3 = pd.DataFrame(result3, columns=['symbol', 'user_count'])
# print(df3)

# Create a horizontal bar chart
fig3 = go.Figure(
    go.Bar(
        x=df3["user_count"],
        y=df3["symbol"],
        orientation='h',  # Horizontal bar chart
        marker=dict(
        color=df3["user_count"],  # Use user_count to vary the color
        colorscale="YlOrBr"  # Yellow autumn-like gradient
        ),
        text=df2["Transaction_Count"],  # Display amount as text
        textposition='outside',  # Position text outside the bars
    )
)

# Customize layout
fig3.update_layout(
    title="User Interested Stock",
    xaxis_title="user_count",
    yaxis_title="symbol",
    yaxis=dict(categoryorder='total ascending'),  # Order bars by value
)

# Streamlit app
st.plotly_chart(fig3)
