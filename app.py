import streamlit as st
import duckdb 

st.set_page_config(
    page_title="Example of Delta Table and DuckDB",
    page_icon="✅",
    layout="wide",
)
                   

# dashboard title
st.title("Example of Cloudflare R2 and DuckDB")

col1, col2 = st.columns([1, 1])

########################################################## import Data from R2##############################
@st.experimental_singleton
def import_data(ttl=5*60):
    con=duckdb.connect('db')
    con.execute(f'''
    install httpfs;
    LOAD httpfs;
    --SET enable_http_metadata_cache=true ;
    --PRAGMA enable_object_cache ;
    set s3_region = 'auto';
    set s3_access_key_id = "{st.secrets["aws_access_key_id_secret"]}" ;
    set s3_secret_access_key = '{st.secrets["aws_secret_access_key_secret"] }';
    set s3_endpoint = '{st.secrets["endpoint_url_secret"].replace("https://", "")}'  ;
    SET s3_url_style='path';
    create or replace table scada as Select SETTLEMENTDATE, (SETTLEMENTDATE - INTERVAL 10 HOUR) as LOCALDATE ,
                      DUID,MIN(SCADAVALUE) as mwh from  parquet_scan('s3://delta/aemo/scada/data/*/*.parquet' , HIVE_PARTITIONING = 1,filename= 1) group by all  ;
    ''')
    return con

########################################################## Query the Data #####################################
con=import_data()

results= con.execute(''' Select SETTLEMENTDATE,LOCALDATE, sum(mwh) as mwh from  scada group by all order by SETTLEMENTDATE desc''').df() 

st.subheader("Latest Updated: " + str(results["SETTLEMENTDATE"].max()))

############################################################# Visualisation ####################################
#localdate is just a stupid hack, Javascript read datetime as UTC not local time :(
import altair as alt
c = alt.Chart(results).mark_area().encode( x='LOCALDATE:T', y='mwh:Q',
                                          tooltip=['LOCALDATE','mwh']).properties(
                                            width=1200,
                                            height=400)
st.write(c)


###########################################################Buttons and Links #############################################################
#Download Button


def convert_df(df):
     # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

csv = convert_df(results[['SETTLEMENTDATE','mwh']])
col2.download_button(
     label="Download data as CSV",
     data=csv,
     file_name='large_df.csv',
     mime='text/csv',
 )


link='[Data Source](http://nemweb.com.au/Reports/Current/Dispatch_SCADA/)'
col1.markdown(link,unsafe_allow_html=True)

link='[Blog](https://datamonkeysite.com/2022/06/28/using-delta-lake-with-python/)'
col1.markdown(link,unsafe_allow_html=True)
