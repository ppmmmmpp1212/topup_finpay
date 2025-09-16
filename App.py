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

# Query ke tabel BigQuery
query = """
SELECT *
FROM `alfred-analytics-406004.analytics_alfred.finpay_topup_joined`
LIMIT 100
"""

# Menjalankan query dan mengubah hasil ke DataFrame
@st.cache_data
def load_data():
    query_job = client.query(query)
    return query_job.to_dataframe()

# Memuat data
df = load_data()

# Menampilkan judul aplikasi
st.title("Dashboard Finpay Topup Data")

# Menampilkan data dalam tabel
st.subheader("Data dari BigQuery")
st.dataframe(df)

# Contoh visualisasi sederhana (misalnya, jumlah transaksi per tanggal)
# Ganti 'transaction_date' dengan nama kolom tanggal yang sesuai di tabel Anda
if 'transaction_date' in df.columns:
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    daily_counts = df.groupby(df['transaction_date'].dt.date).size().reset_index(name='count')

    # Membuat plot dengan Plotly
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=daily_counts['transaction_date'],
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
    st.warning("Kolom 'transaction_date' tidak ditemukan. Silakan sesuaikan nama kolom untuk visualisasi.")

# Sidebar untuk filter (opsional)
st.sidebar.header("Filter Data")
if 'transaction_date' in df.columns:
    min_date = df['transaction_date'].min().date()
    max_date = df['transaction_date'].max().date()
    date_range = st.sidebar.date_input(
        "Pilih Rentang Tanggal",
        [min_date, max_date],
        min_value=min_date,
        max_value=max_date
    )
    if len(date_range) == 2:
        start_date, end_date = date_range
        filtered_df = df[(df['transaction_date'].dt.date >= start_date) & 
                        (df['transaction_date'].dt.date <= end_date)]
        st.subheader("Data Tabel yang Difilter")
        st.dataframe(filtered_df)
