# aemo_tracker
a very simple pipeline to display Aemo data, 2 script to download data from aemo website and save it in a cloud storage ( Cloudflare R2, you can use S3, GCP etc)
and a front end using streamlit, the Queries are run using DuckDB


# Data ingestion
using python script that download the data from AEMO website and save it in a cloud, the script run every 5 minutes, currently hosted in Google Cloud Function

# Storage
Hosted in cloudflare R2

# Front End
hosted in streamlit cloud free edition.
