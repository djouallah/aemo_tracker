import streamlit as st
import duckdb 
import altair as alt
from datetime import datetime
import pytz
import pyarrow.dataset as ds


st.set_page_config(
    page_title="Australian Electricity Market, only a POC how to build a full data pipeline using Only Python from Ingestion to Viz",
    page_icon="✅",
    layout="wide",
)
                   

# dashboard title
st.title("Australian Electricity Market, POC using only Python, Cloudflare R2, DuckDB and Streamlit")

col1, col2 = st.columns([1, 1])

table_path = "./xxx"
########################################################## Query the Data #####################################
con=duckdb.connect()
try :
    scada =  ds.dataset(table_path)
    DUID_Select= st.sidebar.multiselect('Select Station', con.execute(''' Select distinct stationame from  scada WHERE mw !=0 order by stationame ''').df() )

    xxxx = "','".join(DUID_Select)
    filter =  "'"+xxxx+"'"
    if len(DUID_Select) != 0 :
        results= con.execute(f''' Select SETTLEMENTDATE,LOCALDATE,stationame, sum(mw) as mw from  scada where stationame in ({filter}) group by all  order by SETTLEMENTDATE  desc ''').df() 
        c = alt.Chart(results).mark_area().encode( x='LOCALDATE:T', y='mw:Q',color='stationame:N',
                                            tooltip=['LOCALDATE','stationame','mw']).properties(
                                                
                                                width=1200,
                                                height=400)
    else:
        results= con.execute(''' Select SETTLEMENTDATE,LOCALDATE,FuelSourceDescriptor, sum(mw) as mw from  scada group by all order by SETTLEMENTDATE desc''').df()
        c = alt.Chart(results).mark_area().encode( x='LOCALDATE:T', y='mw:Q',color='FuelSourceDescriptor:N',
                                                tooltip=['LOCALDATE','FuelSourceDescriptor','mw']).properties(
                                                    width=1200,
                                                    height=400)

    st.subheader("Latest Updated: " + str(results["SETTLEMENTDATE"].max()))

    ############################################################# Visualisation ####################################
    #localdate is just a stupid hack, Javascript read datetime as UTC not local time :(

    st.write(c)
    del c
    ###########################################################Buttons and Links ####################################
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
    del results


    link='[for a Full experience go to Nemtracker Dashboard](https://datastudio.google.com/reporting/1Fah7mn1X9itiFAMIvCFkj_tEYXHdxAll/page/TyK1)'
    col1.markdown(link,unsafe_allow_html=True)

    link='[Source code](https://github.com/djouallah/aemo_tracker/blob/main/app.py)'
    col1.markdown(link,unsafe_allow_html=True)
except:
    st.write('first run will take time')
########################################################## import Data from R2##############################
@st.experimental_singleton(ttl=10*60)
def import_data(table_path):
    cut_off=datetime.strftime(datetime.now(pytz.timezone('Australia/Brisbane')), '%Y-%m-%d')
    #Date={cut_off}
    con=duckdb.connect()
    con.execute(f"""
    install httpfs;
    LOAD httpfs;
    PRAGMA enable_object_cache;
    SET enable_http_metadata_cache=true ;
    set s3_region = 'auto';
    set s3_access_key_id = "{st.secrets["aws_access_key_id_secret"]}" ;
    set s3_secret_access_key = '{st.secrets["aws_secret_access_key_secret"] }';
    set s3_endpoint = '{st.secrets["endpoint_url_secret"].replace("https://", "")}'  ;
    SET s3_url_style='path';
    create or replace table station as Select DUID,min(Region) as Region,	min(FuelSourceDescriptor) as FuelSourceDescriptor ,
                                   min(stationame) as stationame, min(DispatchType) as DispatchType from  parquet_scan('s3://delta/aemo/duid/duid.parquet' ) group by all ;
    
    """)
    tb=con.execute(f"""
    Select SETTLEMENTDATE, (SETTLEMENTDATE - INTERVAL 10 HOUR) as LOCALDATE ,
         xx.DUID,Region,FuelSourceDescriptor, replace(stationame, '''', '') as stationame,MIN(SCADAVALUE) as mw from  parquet_scan('s3://delta/aemo/scada/data/*/*.parquet' , HIVE_PARTITIONING = 1) as xx
         inner join station
         on xx.DUID = station.DUID
         group by all order by xx.DUID,SETTLEMENTDATE
    """).arrow()
    ds.write_dataset(tb,table_path, format="parquet",existing_data_behavior="overwrite_or_ignore")
    del tb
    
import_data(table_path )
