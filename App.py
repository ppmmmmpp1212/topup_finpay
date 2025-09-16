import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import plotly.graph_objects as go
import json
from datetime import date
import io

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
st.title("Monitoring Finpay Topup Dashboard")

if st.button("Clear Cache"):
    st.cache_data.clear()
    st.experimental_rerun()

# Load data
df = load_data(client, query)

if df.empty:
    st.warning("No data loaded from BigQuery. Please check the connection and table.")
    st.stop()

# Data preprocessing
required_columns = ['TransactionDate', 'Amount', 'TransactionType', 'Nama', 'ClusterID', 'Sender']
if not all(col in df.columns for col in required_columns):
    st.error(f"Required columns {required_columns} not found in the data.")
    st.stop()

df['TransactionDate'] = pd.to_datetime(df['TransactionDate'], errors='coerce')
try:
    df['TransactionDate'] = df['TransactionDate'].dt.tz_localize(None)
except TypeError:
    pass

df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
df['Nama'] = df['Nama'].fillna("tanpa_nama")
df['TransactionType'] = df['TransactionType'].fillna("tanpa_tipe")
df['ClusterID'] = df['ClusterID'].fillna("tanpa_cluster").astype(str)
df['Sender'] = df['Sender'].fillna("tanpa_sender")

st.write(f"Total Baris data: {len(df)}")

# ---
## Raw Data Display (Hidden by Default)

with st.expander("Lihat Raw Data"):
    st.dataframe(df, use_container_width=True)

    # Convert DataFrame to Excel format
    excel_buffer = io.BytesIO()
    df.to_excel(excel_buffer, index=False, engine='xlsxwriter')
    excel_buffer.seek(0)
    
    st.download_button(
        label="Download Data Mentah",
        data=excel_buffer,
        file_name='data_finpay_mentah.xlsx',
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        help='Klik untuk mengunduh seluruh data dalam format Excel.'
    )

# ---
## Scorecard
col1, col2, col3 = st.columns(3)

# Calculate total credit
total_kredit = df[df['TransactionType'] == 'Kredit']['Amount'].sum()
col1.metric("Total Kredit", f"Rp {total_kredit:,.0f}")

# Calculate total debit
total_debit = df[df['TransactionType'] == 'Debit']['Amount'].sum()
col2.metric("Total Debit", f"Rp {total_debit:,.0f}")

# Calculate and display running balance
current_balance = total_kredit - total_debit
col3.metric("Running Balance (Semua Data)", f"Rp {current_balance:,.0f}")

# ---
## Daily Debit and Credit Amounts Chart

if not df['TransactionDate'].dropna().empty:
    daily_summary = df.groupby([df['TransactionDate'].dt.date, 'TransactionType'])['Amount'].sum().unstack(fill_value=0)
    
    if not daily_summary.empty:
        fig = go.Figure()
        
        # Add a trace for Credit amounts
        if 'Kredit' in daily_summary.columns:
            fig.add_trace(
                go.Scatter(
                    x=daily_summary.index,
                    y=daily_summary['Kredit'],
                    mode='lines+markers',
                    name='Kredit'
                )
            )

        # Add a trace for Debit amounts
        if 'Debit' in daily_summary.columns:
            fig.add_trace(
                go.Scatter(
                    x=daily_summary.index,
                    y=daily_summary['Debit'],
                    mode='lines+markers',
                    name='Debit'
                )
            )
        
        fig.update_layout(
            title="Daily Debit and Credit Amounts",
            xaxis_title="Date",
            yaxis_title="Amount (Rp)",
            template="plotly_dark"
        )
        st.plotly_chart(fig)
    else:
        st.warning("Tidak ada data untuk menampilkan grafik jumlah debit/kredit harian.")

# ---
## Running Balance Calculation

st.sidebar.header("Data Filters & Settings")

# Create a filtered dataframe to be used by all subsequent filters
filtered_df = df.copy()

# 1. TransactionType Filter
unique_transaction_types = sorted(filtered_df['TransactionType'].unique())
selected_transaction_types = st.sidebar.multiselect(
    "1. Filter by Transaction Type",
    options=unique_transaction_types,
    default=unique_transaction_types
)

# Filter the dataframe based on the first selection
filtered_df = filtered_df[filtered_df['TransactionType'].isin(selected_transaction_types)]

# 2. ClusterID Filter (cascading)
unique_cluster_ids = sorted(filtered_df['ClusterID'].unique())
selected_cluster_ids = st.sidebar.multiselect(
    "2. Filter by Cluster ID",
    options=unique_cluster_ids,
    default=unique_cluster_ids
)

# Filter the dataframe based on the second selection
filtered_df = filtered_df[filtered_df['ClusterID'].isin(selected_cluster_ids)]

# 3. Sender Filter (cascading)
unique_senders = sorted(filtered_df['Sender'].unique())
selected_senders = st.sidebar.multiselect(
    "3. Filter by Sender",
    options=unique_senders,
    default=unique_senders
)

# Filter the dataframe based on the third selection
filtered_df = filtered_df[filtered_df['Sender'].isin(selected_senders)]

# 4. Name Filter (cascading)
unique_names = sorted(filtered_df['Nama'].unique())
selected_names = st.sidebar.multiselect(
    "4. Filter by Name",
    options=unique_names,
    default=unique_names
)

# Filter the dataframe based on the fourth selection
filtered_df = filtered_df[filtered_df['Nama'].isin(selected_names)].copy()

# Date Filter (applied last for final display)
min_date = filtered_df['TransactionDate'].min().date() if not filtered_df.empty and not pd.isna(filtered_df['TransactionDate'].min()) else date.today()
max_date = filtered_df['TransactionDate'].max().date() if not filtered_df.empty and not pd.isna(filtered_df['TransactionDate'].max()) else date.today()
date_range = st.sidebar.date_input(
    "Select Date Range",
    [min_date, max_date],
    min_value=min_date,
    max_value=max_date
)

# Initial Balance Input
saldo_awal = st.sidebar.number_input(
    "Initial Balance",
    min_value=0.0,
    value=0.0,
    step=1000.0,
    format="%.0f"
)

if len(date_range) == 2:
    start_date, end_date = date_range
    
    # Apply date filter on the already-filtered dataframe
    filtered_df = filtered_df[(filtered_df['TransactionDate'].dt.date >= start_date) & 
                             (filtered_df['TransactionDate'].dt.date <= end_date)].copy()
    
    if filtered_df.empty:
        st.warning("No data found for the selected filters.")
    else:
        # Sort by date and time to ensure chronological calculation
        filtered_df.sort_values('TransactionDate', ascending=True, inplace=True)
        
        # Create a new column 'NetChange' for calculation
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
