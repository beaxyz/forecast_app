import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd
import io
from databricks.sdk import WorkspaceClient
from data import get_sales_data, get_uc_data
from job import job_start, job_status
from data import sql_query_with_user_token
from databricks.connect import DatabricksSession
import time

st.set_page_config(layout="wide")
spark = (
        DatabricksSession.builder
        .serverless()
        .getOrCreate()
    )

if "file_name" not in st.session_state:
    st.session_state.file_name = None
if "job_done" not in st.session_state:
    st.session_state.job_done = True
if "run_id" not in st.session_state:
    st.session_state.run_id = None

st.header("Sales Forecast")
col1, col2 = st.columns([1, 3])

# Extract user access token from the request headers
user_token = st.context.headers.get('X-Forwarded-Access-Token')

# Query the SQL data with the user credentials
sales_data = get_sales_data()
uc_data = get_uc_data()

# In order to query with Service Principal credentials, comment the above line and uncomment the below line
# data = sql_query_with_service_principal("SELECT * FROM samples.nyctaxi.trips LIMIT 5000")
with col1:
    Store = st.selectbox("Store", sales_data["Store"].unique())
    Dept = st.multiselect("Dept", sales_data['Dept'].unique(), default = 2)
    filtered_data = sales_data[(sales_data["Store"] == Store) & (sales_data["Dept"].isin(Dept))]
    st.subheader("Predict Profit")
    Profit = st.number_input("Percentage Revenue", value=0)
    profit = filtered_data['sales'].sum()*Profit
    st.write(f"Profit: {profit}")

with col2:
    grouped_data = filtered_data.groupby(['date','actuals']).agg(sales_agg=('sales', 'sum')).reset_index()
    st.line_chart(data=grouped_data, y="sales_agg", x="date",color = 'actuals')

#Upload data to forecast
#st.dataframe(data=uc_data, height=200, use_container_width=True)

st.header("Forecast Custom Dataset")
col1, col2 = st.columns([1, 3])
with col1:
    w = WorkspaceClient()
    upload_file = st.file_uploader("Upload an Excel")
    Catalog = st.selectbox("Catalog", uc_data["volume_catalog"].unique())
    Schema = st.selectbox("Schema", uc_data[uc_data['volume_catalog'] == Catalog]['volume_schema'].unique())
    Volume = st.selectbox("Volume", uc_data[uc_data['volume_catalog'] == Catalog]['volume_name'].unique())
    

with col2:
    if st.button("Save File"):
        file_bytes = upload_file.read()
        binary_data = io.BytesIO(file_bytes)
        file_name = upload_file.name
        st.session_state.file_name = file_name

        volume_file_path = f"/Volumes/{Catalog}/{Schema}/{Volume}/{file_name}"
        w.files.upload(volume_file_path, binary_data, overwrite = True)
        st.success(f"File {file_name} uploaded successfully!")

        sql_query_with_user_token(f"""CREATE OR REPLACE TABLE {Catalog}.{Schema}.{file_name.split('.')[0]} AS SELECT * FROM read_files('{volume_file_path}',format => 'excel', headerRows => 1,dataAddress => "'Sheet 1'!A2:E9764", schemaEvolutionMode => "none")""", user_token = user_token)
        st.success(f"File {file_name} written to UC. Training model now, this usually takes a few minutes")
        st.subheader(f"Forecast for {st.session_state.file_name}")
        st.session_state.job_done = False
        run = job_start(Catalog, Schema, st.session_state.file_name)
        st.session_state.run_id = run.run_id
        
    if st.session_state.file_name:
        store_pd = spark.read.table(f'{Catalog}.{Schema}.{st.session_state.file_name.split(".")[0]}').toPandas()
        st.dataframe(store_pd)

if not st.session_state.job_done and st.session_state.run_id:
    if job_status(st.session_state.run_id) is None:
        with st.status(f"Job still running", state = 'running'):
            time.sleep(5)
            st.rerun()

    elif job_status(st.session_state.run_id).name =="SUCCESS":
        st.session_state.job_done = True
        st.success(f"Job successfully ran")

    
if st.session_state.job_done and st.session_state.run_id:
    forecast_df = sql_query_with_user_token(f"SELECT * FROM {Catalog}.{Schema}.{st.session_state.file_name.split('.')[0]}_forecast", user_token=user_token)
    forecast_df_grouped = forecast_df.groupby(['date','actuals']).agg(sales_agg=('sales', 'sum')).reset_index()
    st.line_chart(data=forecast_df_grouped, y="sales_agg", x="date",color = 'actuals')