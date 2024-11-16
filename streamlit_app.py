import streamlit as st
import pandas as pd
from pinotdb import connect
import plotly.figure_factory as ff
import plotly.graph_objects as go

st.title("✨ Stock-Trade User Interactions")
st.write(
    "For the Midterm Examination in DADS6005 Data Streaming and Realtime Analytics "
)

# ตั้งค่าเพื่อให้ dashboard ใช้พื้นที่เต็มหน้าจอ
# st.set_page_config(page_title="DADS6005 Realtime Dashboard", layout="wide")

# Function for connecting to Druid
def create_connection():
    conn = connect(host='13.229.87.146', port=8099, path='/query/sql', scheme='http')
    return conn

# สร้างการเชื่อมต่อกับ Druid
conn = create_connection()

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
        orientation='h',  # Horizontal bar chart
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