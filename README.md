# Introduction
A very simple pipeline to display AEMO data, everything is open source and written only using Python and SQL


![diagram](https://user-images.githubusercontent.com/12554469/236982047-98433eae-8f36-4fac-b67d-02be00517bfe.JPG)

alternatively you can everything in your laptop, or just deploy it in a cloud VM, or a container I suppose.


# Data ingestion
using python script that download the data from AEMO website and save it in a cloud, the script run every 5 minutes, currently using Google Cloud Function

# Storage
Hosted in cloudflare R2 in Delta table format

# Front End
hosted in streamlit cloud free edition, DuckDB import Data from Cloudflare R2, as running Query directly will be too slow for a subsecond expectation,
the Visual interaction is very fast as all the data is loaded into memory.

# Bottleneck 
DuckDB does not support vacuum, the database file size keep increasing
The total size of data is limited by the size of the local SSD


