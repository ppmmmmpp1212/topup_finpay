import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import plotly.graph_objects as go
import json
from datetime import date

# Fetch credentials from Streamlit secrets
try:
    credentials_json = st.secrets["bigquery"]["credentials"]
    credentials = service_account.Credentials.from_service_account_info(json.loads(credentials_json))
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
except Exception as e:
    st.error(f"Error loading BigQuery credentials: {e}")
    st.stop()

# BigQuery query to fetch all data
query = """
SELECT *
FROM `alfred-analytics-406004.analytics_alfred.finpay_topup_joined`
"""

@st.cache_data
def load_data(_client, _query):
    """Loads data from BigQuery into a Pandas DataFrame."""
    try:
        query_job = _client.query(_query)
        df_result = query_job.to_dataframe()
        return df_result
    except Exception as e:
        st.error(f"Error executing BigQuery query: {e}")
        return pd.DataFrame()

# Streamlit App UI
st.title("Finpay Topup Data Dashboard")

if st.button("Clear Cache"):
    st.cache_data.clear()
    st.experimental_rerun()

# Load data
df = load_data(client, query)

if df.empty:
    st.warning("No data loaded from BigQuery. Please check the connection and table.")
    st.stop()

# Data preprocessing
required_columns = ['TransactionDate', 'Amount', 'TransactionType']
if not all(col in df.columns for col in required_columns):
    st.error(f"Required columns {required_columns} not found in the data.")
    st.stop()

df['TransactionDate'] = pd.to_datetime(df['TransactionDate'], errors='coerce')
df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')

st.write(f"Rows loaded: {len(df)}")

# ---
## Raw Data Display (Hidden by Default)

with st.expander("Show Raw Data from BigQuery"):
    st.dataframe(df.head(100), use_container_width=True)

# ---
## Daily Transaction Count Chart

if not df['TransactionDate'].dropna().empty:
    daily_counts = df.groupby(df['TransactionDate'].dt.date).size().reset_index(name='count')
    
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=daily_counts['TransactionDate'],
            y=daily_counts['count'],
            mode='lines+markers',
            name='Daily Transaction Count'
        )
    )
    fig.update_layout(
        title="Daily Transaction Count",
        xaxis_title="Date",
        yaxis_title="Count",
        template="plotly_dark"
    )
    st.plotly_chart(fig)

# ---
## Running Balance Calculation

st.sidebar.header("Data Filters & Settings")
min_date = df['TransactionDate'].min().date()
max_date = df['TransactionDate'].max().date()

date_range = st.sidebar.date_input(
    "Select Date Range",
    [min_date, max_date],
    min_value=min_date,
    max_value=max_date
)

saldo_awal = st.sidebar.number_input(
    "Initial Balance",
    min_value=0.0,
    value=0.0,
    step=1000.0,
    format="%.0f"
)

if len(date_range) == 2:
    start_date, end_date = date_range
    filtered_df = df[(df['TransactionDate'].dt.date >= start_date) & 
                     (df['TransactionDate'].dt.date <= end_date)].copy()
    
    if filtered_df.empty:
        st.warning("No data found for the selected date range.")
    else:
        # Sort by date and time to ensure chronological calculation
        filtered_df.sort_values('TransactionDate', ascending=True, inplace=True)
        
        # Create a new column 'NetChange' for calculation
        # It's positive 'Amount' for 'Kredit' and negative 'Amount' for 'Debit'
        filtered_df['NetChange'] = filtered_df.apply(
            lambda row: row['Amount'] if row['TransactionType'] == 'Kredit' else 
                       -row['Amount'] if row['TransactionType'] == 'Debit' else 0,
            axis=1
        )
        
        # Calculate the cumulative sum of NetChange and add it to the initial balance
        filtered_df['RunningSaldo'] = saldo_awal + filtered_df['NetChange'].cumsum()
        
        st.subheader("Filtered Data with Running Balance")
        st.dataframe(filtered_df, use_container_width=True)
        
        final_balance = filtered_df['RunningSaldo'].iloc[-1]
        st.markdown(f"**Final Balance: Rp {final_balance:,.0f}**")
