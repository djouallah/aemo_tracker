import streamlit as st
import duckdb 
from timeit import default_timer as timer
import altair as alt

st.set_page_config(
    page_title="Example of Cloudflare R2 and DuckDB",
    page_icon="✅",
    layout="wide",
)
                   

# dashboard title
st.title("Example of Cloudflare R2 and DuckDB")

col1, col2 = st.columns([1, 1])

########################################################## import Data from R2##############################
@st.experimental_singleton(ttl=5*60)
def import_data():
    con=duckdb.connect()
    con.execute(f'''
    install httpfs;
    LOAD httpfs;
    PRAGMA enable_object_cache;
    set s3_region = 'auto';
    set s3_access_key_id = "{st.secrets["aws_access_key_id_secret"]}" ;
    set s3_secret_access_key = '{st.secrets["aws_secret_access_key_secret"] }';
    set s3_endpoint = '{st.secrets["endpoint_url_secret"].replace("https://", "")}'  ;
    SET s3_url_style='path';
    create or replace table scada as Select SETTLEMENTDATE, (SETTLEMENTDATE - INTERVAL 10 HOUR) as LOCALDATE ,
         DUID,MIN(SCADAVALUE) as mw from  parquet_scan('s3://delta/aemo/scada/data/*/*.parquet' , HIVE_PARTITIONING = 1,filename= 1)
         group by all
    ''')
    return con

start = timer()
con=import_data()
end = timer()
#st.write(round(end - start,2))

########################################################## Query the Data #####################################
DUID_Select= st.sidebar.multiselect('Select Station', con.execute(''' Select distinct DUID from  scada WHERE mw !=0 ''').df() )

xxxx = "','".join(DUID_Select)
filter =  "'"+xxxx+"'"
#st.write(filter)
if len(DUID_Select) != 0 :
    results= con.execute(f''' Select SETTLEMENTDATE,LOCALDATE,DUID, sum(mw) as mw from  scada where DUID in ({filter}) group by all  order by SETTLEMENTDATE  desc ''').df() 
    c = alt.Chart(results).mark_area().encode( x='LOCALDATE:T', y='mw:Q',color='DUID:N',
                                          tooltip=['LOCALDATE','DUID','mw']).properties(
                                            
                                            width=1200,
                                            height=400)
else:
   results= con.execute(''' Select SETTLEMENTDATE,LOCALDATE, sum(mw) as mw from  scada group by all order by SETTLEMENTDATE desc''').df()
   c = alt.Chart(results).mark_area().encode( x='LOCALDATE:T', y='mw:Q',
                                          tooltip=['LOCALDATE','mw']).properties(
                                            width=1200,
                                            height=400)

st.subheader("Latest Updated: " + str(results["SETTLEMENTDATE"].max()))

############################################################# Visualisation ####################################
#localdate is just a stupid hack, Javascript read datetime as UTC not local time :(


st.write(c)


###########################################################Buttons and Links #############################################################
#Download Button


def convert_df(df):
     # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

csv = convert_df(results)
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
