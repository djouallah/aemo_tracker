# Introduction
a very simple pipeline to display AEMO data, 2 script to download data from aemo website and save it in a cloud storage (Cloudflare R2, you can use S3, GCP etc)
and a front end using streamlit, the Queries are run using DuckDB, all the code is written in Python, except some queries in SQL


# Data ingestion
using python script that download the data from AEMO website and save it in a cloud, the script run every 5 minutes, currently hosted in Google Cloud Function

# Storage
Hosted in cloudflare R2

# Front End
hosted in streamlit cloud free edition, DuckDB query Cloudflare R2 using fsspec and cache the data locally, the Visual interaction is very fast as all the data is loaded into memory.

# Bottleneck 
The data in Streamlit is refreshed from the remote storage every 10 minutes, currently it take around 10 second, you can’t interact with the app when the refresh is happening, it would have being nice if it was a background operation.
