# Introduction
A very simple pipeline to display AEMO data, everything is open source and written only using Python and SQL

![image](https://github.com/djouallah/aemo_tracker/assets/12554469/f5382496-a42a-4d36-8e78-4cdc89991875)


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


