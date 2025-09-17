import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import plotly.graph_objects as go
import json
from datetime import date, datetime
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

# Define the full table ID
table_id = "alfred-analytics-406004.analytics_alfred.finpay_topup_joined"

# BigQuery query to fetch all data
query = f"""
SELECT *
FROM `{table_id}`
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
    st.rerun()

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
df['Sender'] = pd.to_numeric(df['Sender'], errors='coerce').fillna(0).astype(int)  # Ensure Sender is integer

# Get latest data update timestamp
latest_date = df['TransactionDate'].max()
st.info(f"Data Update: {latest_date.strftime('%Y-%m-%d %H:%M:%S')}")

st.write(f"Total Baris data: {len(df)}")

# Define the initial balances based on ClusterID
initial_balances_by_cluster = {
    '411311': 33725650,
    '421315': 50622293,
    '421318': 22681438,
    '421320': 52467000,
    '421307': 64689000,
    '421306': 48291500,
}

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
## Sidebar
st.sidebar.header("Data Filters & Settings")

# Form Input Data
with st.sidebar.expander("Tambah Data Baru"):
    with st.form("new_data_form"):
        st.subheader("Form Input Transaksi")
        
        # Form fields
        transaction_date = st.date_input("Transaction Date", value=date.today())
        transaction_time = st.time_input("Transaction Time", value=datetime.now().time())
        amount = st.number_input("Amount", min_value=0, step=1000, format="%d")
        transaction_type = st.selectbox("Transaction Type", options=['Kredit', 'Debit'])
        nama = st.text_input("Nama")
        
        # Use unique ClusterIDs from the data for consistency
        unique_cluster_ids_form = sorted(df['ClusterID'].unique())
        cluster_id = st.selectbox("Cluster ID", options=unique_cluster_ids_form)
        
        sender = st.number_input("Sender", min_value=0, step=1, format="%d")
        
        submitted = st.form_submit_button("Submit")

        if submitted:
            # Combine date and time
            full_transaction_date = datetime.combine(transaction_date, transaction_time)
            
            # Create a dictionary to represent the new row
            new_row = {
                'TransactionDate': full_transaction_date.isoformat(),
                'Amount': float(amount),
                'TransactionType': transaction_type,
                'Nama': nama,
                'ClusterID': cluster_id,
                'Sender': int(sender)
            }
            
            # Insert the new row into the BigQuery table
            try:
                errors = client.insert_rows_json(table_id, [new_row])
                
                if errors:
                    st.error(f"Gagal memasukkan data: {errors}")
                else:
                    st.success("Data berhasil dimasukkan ke tabel BigQuery.")
                    st.info("Memperbarui dashboard dengan data terbaru...")
                    st.cache_data.clear()
                    st.rerun()
            except Exception as e:
                st.error(f"Terjadi kesalahan saat memasukkan data ke BigQuery: {e}")

# ---
# Cascading filters
filtered_df = df.copy()

# 1. TransactionType Filter
unique_transaction_types = sorted(filtered_df['TransactionType'].unique())
selected_transaction_types = st.sidebar.multiselect(
    "1. Filter by Transaction Type",
    options=unique_transaction_types,
    default=unique_transaction_types
)
filtered_df = filtered_df[filtered_df['TransactionType'].isin(selected_transaction_types)]

# 2. ClusterID Filter (cascading)
unique_cluster_ids = sorted(filtered_df['ClusterID'].unique())
selected_cluster_ids = st.sidebar.multiselect(
    "2. Filter by Cluster ID",
    options=unique_cluster_ids,
    default=unique_cluster_ids
)
filtered_df = filtered_df[filtered_df['ClusterID'].isin(selected_cluster_ids)]

# 3. Sender Filter (cascading)
unique_senders = sorted(filtered_df['Sender'].unique())
selected_senders = st.sidebar.multiselect(
    "3. Filter by Sender",
    options=unique_senders,
    default=unique_senders
)
filtered_df = filtered_df[filtered_df['Sender'].isin(selected_senders)]

# 4. Name Filter (cascading)
unique_names = sorted(filtered_df['Nama'].unique())
selected_names = st.sidebar.multiselect(
    "4. Filter by Name",
    options=unique_names,
    default=unique_names
)
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

# ---
## Fitur Hapus Data di Sidebar
with st.sidebar.expander("Hapus Data"):
    st.warning("PERINGATAN: Tindakan ini akan menghapus data secara PERMANEN dari BigQuery.")
    
    # Tambahkan indeks unik sementara untuk identifikasi baris
    df_for_delete = df.copy()
    df_for_delete['row_index'] = df_for_delete.index
    
    # Gunakan multiselect untuk memilih baris yang akan dihapus
    rows_to_delete_index = st.multiselect(
        "Pilih baris yang akan dihapus (berdasarkan indeks):",
        options=df_for_delete.index.tolist(),
        format_func=lambda idx: f"Index {idx}: {df_for_delete.loc[idx, 'TransactionDate'].strftime('%Y-%m-%d %H:%M:%S')} - Rp {df_for_delete.loc[idx, 'Amount']} ({df_for_delete.loc[idx, 'TransactionType']})"
    )
    
    # Tombol hapus berada dalam form untuk mencegah re-eksekusi tidak terduga
    with st.form("delete_form"):
        delete_button = st.form_submit_button("Hapus Data yang Dipilih")
        
        if delete_button and rows_to_delete_index:
            st.info(f"Mencoba menghapus {len(rows_to_delete_index)} baris...")
            
            rows_to_delete_df = df.loc[rows_to_delete_index]
            deletion_status = {'success': [], 'failed': []}
            
            for index, row in rows_to_delete_df.iterrows():
                try:
                    # Validate Sender value
                    if pd.isna(row['Sender']):
                        raise ValueError("Sender value is missing or invalid")
                    
                    # Construct a WHERE clause that uniquely identifies the row
                    delete_query = f"""
                    DELETE FROM `{table_id}`
                    WHERE 
                        TransactionDate = PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', '{row['TransactionDate'].isoformat()}') AND
                        Amount = {row['Amount']} AND
                        TransactionType = '{row['TransactionType']}' AND
                        Nama = '{row['Nama'].replace("'", "''")}' AND
                        ClusterID = '{row['ClusterID']}' AND
                        Sender = {int(row['Sender'])}
                    """
                    
                    # Debug: Show the query for verification
                    # st.write(f"Executing query: {delete_query}")
                    
                    query_job = client.query(delete_query)
                    query_job.result()
                    
                    deletion_status['success'].append(f"Row {index}")
                    
                except Exception as e:
                    deletion_status['failed'].append(f"Row {index} (Error: {e}, Sender: {row['Sender']})")
            
            if deletion_status['success']:
                st.success(f"Berhasil menghapus {len(deletion_status['success'])} baris.")
            if deletion_status['failed']:
                st.error(f"Gagal menghapus {len(deletion_status['failed'])} baris: {', '.join(deletion_status['failed'])}")

            st.cache_data.clear()
            st.rerun()

# ---
## Interactive Scorecards & Filtered Charts
col1, col2, col3 = st.columns(3)

if len(date_range) == 2:
    start_date, end_date = date_range

    # Calculate the dynamic saldo_awal based on selected ClusterIDs
    saldo_awal = sum(initial_balances_by_cluster.get(cid, 0) for cid in selected_cluster_ids)

    # Apply date filter on the already-filtered dataframe
    final_filtered_df = filtered_df[(filtered_df['TransactionDate'].dt.date >= start_date) & 
                                    (filtered_df['TransactionDate'].dt.date <= end_date)].copy()
    
    # Calculate values for scorecards
    total_debit_filtered = final_filtered_df[final_filtered_df['TransactionType'] == 'Debit']['Amount'].sum()
    total_kredit_filtered = final_filtered_df[final_filtered_df['TransactionType'] == 'Kredit']['Amount'].sum()
    
    final_balance_value = saldo_awal + (total_kredit_filtered - total_debit_filtered)

    def create_card(title, value):
        return f"""
            <div style="
                background-color: #F0F2F6;
                padding: 15px;
                border-radius: 8px;
                box-shadow: 0 4px 6px 0 rgba(0, 0, 0, 0.1);
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

    st.markdown("<br>", unsafe_allow_html=True)

    # ---
    ## Daily Debit and Credit Amounts Chart
    if not final_filtered_df['TransactionDate'].dropna().empty:
        daily_summary = final_filtered_df.groupby([final_filtered_df['TransactionDate'].dt.date, 'TransactionType'])['Amount'].sum().unstack(fill_value=0)
        
        if not daily_summary.empty:
            fig = go.Figure()
            
            if 'Kredit' in daily_summary.columns:
                fig.add_trace(
                    go.Scatter(
                        x=daily_summary.index,
                        y=daily_summary['Kredit'],
                        mode='lines+markers',
                        name='Kredit'
                    )
                )

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
                title="Daily Debit and Credit Amounts (Filtered)",
                xaxis_title="Date",
                yaxis_title="Amount (Rp)",
                template="plotly_dark"
            )
            st.plotly_chart(fig)
        else:
            st.warning("Tidak ada data untuk menampilkan grafik jumlah debit/kredit harian pada filter yang dipilih.")
    
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)
    
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

        # Reformat numeric columns for display with commas
        final_filtered_df['Amount'] = final_filtered_df['Amount'].apply(lambda x: f"{x:,.0f}")
        final_filtered_df['RunningSaldo'] = final_filtered_df['RunningSaldo'].apply(lambda x: f"{x:,.0f}")
        
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
        st.markdown(f"**Final Balance: Rp {final_balance_display}**")
        
    # ---
    ## Summary Table of All Clusters
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(
        """
        <h2 style='text-align: center;'>Ringkasan Saldo Berdasarkan Cluster
        </h2>
        """,
        unsafe_allow_html=True
    )

    # Create a summary DataFrame
    summary_df = df.groupby('ClusterID')['Amount'].sum().reset_index()
    summary_df.rename(columns={'Amount': 'Total Transaksi'}, inplace=True)

    # Separate Debit and Kredit
    total_kredit_by_cluster = df[df['TransactionType'] == 'Kredit'].groupby('ClusterID')['Amount'].sum()
    total_debit_by_cluster = df[df['TransactionType'] == 'Debit'].groupby('ClusterID')['Amount'].sum()
    
    # Calculate latest transaction date per cluster
    latest_date_by_cluster = df.groupby('ClusterID')['TransactionDate'].max()

    # Merge into a single summary DataFrame
    summary_df = summary_df.merge(total_kredit_by_cluster, on='ClusterID', how='left').rename(columns={'Amount': 'Total Kredit'})
    summary_df = summary_df.merge(total_debit_by_cluster, on='ClusterID', how='left').rename(columns={'Amount': 'Total Debit'})
    summary_df = summary_df.merge(latest_date_by_cluster, on='ClusterID', how='left').rename(columns={'TransactionDate': 'Data Update'})

    # Fill NaN with 0 for clusters with no credit or debit transactions
    summary_df[['Total Kredit', 'Total Debit']] = summary_df[['Total Kredit', 'Total Debit']].fillna(0)

    # Add Initial Balance and Running Balance columns
    summary_df['Initial Balance'] = summary_df['ClusterID'].map(initial_balances_by_cluster).fillna(0)
    summary_df['Running Balance'] = summary_df['Initial Balance'] + summary_df['Total Kredit'] - summary_df['Total Debit']
    
    # Calculate the grand total of the running balances
    total_running_balance = summary_df['Running Balance'].sum()

    # Add a summary row at the bottom of the dataframe
    summary_row = pd.DataFrame([['Total', 
                                 '---',
                                 summary_df['Total Transaksi'].sum(), 
                                 summary_df['Total Kredit'].sum(), 
                                 summary_df['Total Debit'].sum(), 
                                 summary_df['Initial Balance'].sum(), 
                                 total_running_balance]], 
                               columns=['ClusterID', 'Data Update', 'Total Transaksi', 'Total Kredit', 'Total Debit', 'Initial Balance', 'Running Balance'])
    
    summary_df = pd.concat([summary_df, summary_row], ignore_index=True)

    # Reformat numeric and datetime columns for display
    summary_df['Data Update'] = summary_df['Data Update'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) and x != '---' else '---')
    summary_df['Total Transaksi'] = summary_df['Total Transaksi'].apply(lambda x: f"{x:,.0f}")
    summary_df['Total Kredit'] = summary_df['Total Kredit'].apply(lambda x: f"{x:,.0f}")
    summary_df['Total Debit'] = summary_df['Total Debit'].apply(lambda x: f"{x:,.0f}")
    summary_df['Initial Balance'] = summary_df['Initial Balance'].apply(lambda x: f"{x:,.0f}")
    summary_df['Running Balance'] = summary_df['Running Balance'].apply(lambda x: f"{x:,.0f}")

    # Reorder columns for better readability
    summary_df = summary_df[['ClusterID', 'Data Update', 'Total Kredit', 'Total Debit', 'Initial Balance', 'Running Balance']]

    st.dataframe(summary_df, use_container_width=True)

    # Download button for the summary table
    summary_excel_buffer = io.BytesIO()
    summary_df.to_excel(summary_excel_buffer, index=False, engine='xlsxwriter')
    summary_excel_buffer.seek(0)
    st.download_button(
        label="Download Ringkasan Klaster",
        data=summary_excel_buffer,
        file_name='ringkasan_klaster.xlsx',
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        help='Klik untuk mengunduh ringkasan saldo klaster dalam format Excel.'
    )

    st.markdown("<br>", unsafe_allow_html=True)

else:
    st.info("Pilih rentang tanggal untuk menampilkan data yang difilter dan scorecard.")
