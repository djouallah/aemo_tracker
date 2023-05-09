# Introduction
A very simple pipeline to display AEMO data, 2 script to download data from aemo website and save it in a cloud storage (Cloudflare R2, you can use S3, GCP etc)
and a front end using streamlit, the Queries are run using DuckDB, all the code is written in Python, except some queries in SQL


![diagram](https://user-images.githubusercontent.com/12554469/236982047-98433eae-8f36-4fac-b67d-02be00517bfe.JPG)

alternatively you can everything in your laptop, or just deploy it in a cloud VM, or a container I suppose.


# Data ingestion
using python script that download the data from AEMO website and save it in a cloud, the script run every 5 minutes, currently hosted in Google Cloud Function

# Storage
Hosted in cloudflare R2

# Front End
hosted in streamlit cloud free edition, DuckDB query Cloudflare R2 using fsspec and cache the data locally, the Visual interaction is very fast as all the data is loaded into memory.

# Bottleneck 
Downloading data is still very slow from R2
DuckDB does not support vacuum, the database file size keep increasing


