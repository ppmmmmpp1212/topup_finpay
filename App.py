import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, date
import json
import io

# Mengambil credentials dari Streamlit secrets
credentials_json = st.secrets["bigquery"]["credentials"]
credentials = service_account.Credentials.from_service_account_info(json.loads(credentials_json))

# Inisialisasi BigQuery client
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Query ke tabel BigQuery tanpa batasan LIMIT
query = """
SELECT *
FROM `alfred-analytics-406004.analytics_alfred.finpay_topup_joined`
"""

# Menjalankan query dan mengubah hasil ke DataFrame
@st.cache_data
def load_data(_client, _query):
    query_job = _client.query(_query)
    return query_job.to_dataframe()

# Tombol untuk membersihkan cache
if st.button("Clear Cache"):
    st.cache_data.clear()
    st.success("Cache telah dihapus. Muat ulang data...")

# Memuat data
df = load_data(client, query)

# Konversi TransactionDate ke datetime jika ada
if 'TransactionDate' in df.columns:
    df['TransactionDate'] = pd.to_datetime(df['TransactionDate'])

# Konversi Amount ke numeric jika ada
if 'Amount' in df.columns:
    df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')

# Menampilkan jumlah baris yang dimuat
st.write(f"Jumlah baris yang dimuat: {len(df)}")

# Menampilkan judul aplikasi
st.title("Dashboard Finpay Topup Data")

# Menampilkan data dalam tabel
st.subheader("Data dari BigQuery")
st.dataframe(df, use_container_width=True)

# Contoh visualisasi sederhana (misalnya, jumlah transaksi per tanggal)
if 'TransactionDate' in df.columns:
    daily_counts = df.groupby(df['TransactionDate'].dt.date).size().reset_index(name='count')

    # Membuat plot dengan Plotly
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=daily_counts['TransactionDate'],
            y=daily_counts['count'],
            mode='lines+markers',
            name='Jumlah Transaksi'
        )
    )
    fig.update_layout(
        title="Jumlah Transaksi Harian",
        xaxis_title="Tanggal",
        yaxis_title="Jumlah Transaksi",
        template="plotly_dark"
    )
    st.plotly_chart(fig)
else:
    st.warning("Kolom 'TransactionDate' tidak ditemukan. Silakan sesuaikan nama kolom untuk visualisasi.")

# Sidebar untuk filter (opsional)
st.sidebar.header("Filter Data")
if 'TransactionDate' in df.columns:
    min_date = df['TransactionDate'].min().date()
    max_date = df['TransactionDate'].max().date()
    date_range = st.sidebar.date_input(
        "Pilih Rentang Tanggal",
        [min_date, max_date],
        min_value=min_date,
        max_value=max_date
    )

    # Input Saldo Awal
    saldo_awal = st.sidebar.number_input(
        "Saldo Awal",
        min_value=0.0,
        value=8000000.0,
        step=1000.0,
        format="%.0f"
    )

    if len(date_range) == 2:
        start_date, end_date = date_range
        filtered_df = df[(df['TransactionDate'].dt.date >= start_date) & 
                         (df['TransactionDate'].dt.date <= end_date)].copy()
        
        if 'Amount' in filtered_df.columns and 'TransactionType' in filtered_df.columns:
            # Urutkan berdasarkan TransactionDate
            filtered_df.sort_values('TransactionDate', inplace=True)
            
            # Buat kolom baru yang akan digunakan untuk kalkulasi
            # Nilai Amount diatur 0 jika TransactionType bukan 'Debit'
            filtered_df['DebitAmount'] = filtered_df.apply(
                lambda row: row['Amount'] if row['TransactionType'] == 'Debit' else 0,
                axis=1
            )
            
            # Kalkulasi running saldo (mengurangi DebitAmount secara kumulatif)
            filtered_df['RunningSaldo'] = saldo_awal - filtered_df['DebitAmount'].cumsum()
            
            st.subheader("Data Tabel yang Difilter dengan Running Saldo (Debit Only)")
            st.dataframe(filtered_df, use_container_width=True)
            
            if not filtered_df.empty:
                sisa_saldo = filtered_df['RunningSaldo'].iloc[-1]
            else:
                sisa_saldo = saldo_awal
            st.write(f"Sisa Saldo Akhir: {sisa_saldo:,.0f}")
        elif 'Amount' not in filtered_df.columns:
            st.warning("Kolom 'Amount' tidak ditemukan. Tidak dapat melakukan kalkulasi saldo.")
            st.subheader("Data Tabel yang Difilter")
            st.dataframe(filtered_df, use_container_width=True)
        elif 'TransactionType' not in filtered_df.columns:
            st.warning("Kolom 'TransactionType' tidak ditemukan. Tidak dapat melakukan kalkulasi saldo sesuai tipe transaksi.")
            st.subheader("Data Tabel yang Difilter")
            st.dataframe(filtered_df, use_container_width=True)
else:
    st.warning("Kolom 'TransactionDate' tidak ditemukan. Silakan sesuaikan nama kolom untuk visualisasi.")
