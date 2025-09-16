import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import plotly.graph_objects as go
import json
from datetime import date
import io

# Set Streamlit page to wide mode
st.set_page_config(layout="wide")

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
st.markdown(
    """
    <h1 style='text-align: center;'>Monitoring Finpay Topup Dashboard
    </h1>
    """,
    unsafe_allow_html=True
)

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

# Date Filter (now a single date)
min_date = filtered_df['TransactionDate'].min().date() if not filtered_df.empty and not pd.isna(filtered_df['TransactionDate'].min()) else date.today()
max_date = filtered_df['TransactionDate'].max().date() if not filtered_df.empty and not pd.isna(filtered_df['TransactionDate'].max()) else date.today()
selected_date = st.sidebar.date_input(
    "Select a Date",
    value=min_date,
    min_value=min_date,
    max_value=max_date
)

# ---
## Interactive Scorecards & Filtered Charts
col1, col2, col3 = st.columns(3)

# The rest of the content will be displayed only if a date is selected
if selected_date:
    
    # Define the initial balances based on ClusterID
    initial_balances_by_cluster = {
        '411311': 33725650,
        '421315': 8270000,
        '421318': 22681438,
        '421320': 52467000,
        '421307': 64689000,
        '421306': 48291500,
    }
    
    # Calculate the dynamic saldo_awal based on selected ClusterIDs
    saldo_awal = sum(initial_balances_by_cluster.get(cid, 0) for cid in selected_cluster_ids)

    # Apply date filter on the already-filtered dataframe
    final_filtered_df = filtered_df[filtered_df['TransactionDate'].dt.date == selected_date].copy()
    
    # Calculate values for scorecards
    total_debit_filtered = final_filtered_df[final_filtered_df['TransactionType'] == 'Debit']['Amount'].sum()
    total_kredit_filtered = final_filtered_df[final_filtered_df['TransactionType'] == 'Kredit']['Amount'].sum()
    
    final_balance_value = saldo_awal + (total_kredit_filtered - total_debit_filtered)

    def create_card(title, value):
        return f"""
            <div style="
                background-color: #F0F2F6;
                padding: 15px; /* Reduced padding */
                border-radius: 8px; /* Slightly smaller border-radius */
                box-shadow: 0 4px 6px 0 rgba(0, 0, 0, 0.1); /* Reduced shadow */
                text-align: center;
            ">
                <h5 style="margin: 0; color: #555;">{title}</h5>
                <h3 style="margin: 5px 0 0; color: #0078A1;">Rp {value:,.0f}</h3>
            </div>
        """

    with col1:
        st.markdown(create_card("Total Kredit", total_kredit_filtered), unsafe_allow_html=True)

    with col2:
        st.markdown(create_card("Total Debit", total_debit_filtered), unsafe_allow_html=True)

    with col3:
        st.markdown(create_card("Running Balance", final_balance_value), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True) # Menambahkan baris kosong sebagai pemisah

    # ---
    ## Daily Debit and Credit Amounts Chart (Now inside the filtered block)
    if not final_filtered_df['TransactionDate'].dropna().empty:
        daily_summary = final_filtered_df.groupby([final_filtered_df['TransactionDate'].dt.date, 'TransactionType'])['Amount'].sum().unstack(fill_value=0)
        
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
                title="Daily Debit and Credit Amounts (Filtered)", # Updated title
                xaxis_title="Date",
                yaxis_title="Amount (Rp)",
                template="plotly_dark"
            )
            st.plotly_chart(fig)
        else:
            st.warning("Tidak ada data untuk menampilkan grafik jumlah debit/kredit harian pada filter yang dipilih.")
    
    st.markdown("<br>", unsafe_allow_html=True) # Menambahkan baris kosong menggunakan HTML
    st.markdown("<br>", unsafe_allow_html=True) # Menambahkan baris kosong menggunakan HTML
    
    if final_filtered_df.empty:
        st.warning("No data found for the selected filters.")
    else:
        # Sort by date and time to ensure chronological calculation
        final_filtered_df.sort_values('TransactionDate', ascending=True, inplace=True)
        
        # Create a new column 'NetChange' for calculation
        final_filtered_df['NetChange'] = final_filtered_df.apply(
            lambda row: row['Amount'] if row['TransactionType'] == 'Kredit' else 
                       -row['Amount'] if row['TransactionType'] == 'Debit' else 0,
            axis=1
        )
        
        # Calculate the cumulative sum of NetChange and add it to the initial balance
        final_filtered_df['RunningSaldo'] = saldo_awal + final_filtered_df['NetChange'].cumsum()
        
        st.markdown(
            """
            <h2 style='text-align: center;'>Filtered Data with Running Balance
            </h2>
            """,
            unsafe_allow_html=True
        )
        st.dataframe(final_filtered_df, use_container_width=True)

        # Download button for filtered data
        excel_buffer_filtered = io.BytesIO()
        final_filtered_df.to_excel(excel_buffer_filtered, index=False, engine='xlsxwriter')
        excel_buffer_filtered.seek(0)
        
        st.download_button(
            label="Download Filtered Data",
            data=excel_buffer_filtered,
            file_name='data_finpay_filtered.xlsx',
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            help='Klik untuk mengunduh data yang sudah difilter dalam format Excel.'
        )
        
        final_balance_display = final_filtered_df['RunningSaldo'].iloc[-1]
        st.markdown(f"**Final Balance: Rp {final_balance_display:,.0f}**")
else:
    st.info("Pilih tanggal untuk melihat data yang difilter dan scorecard.")
