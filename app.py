import streamlit as st
import duckdb 
import altair as alt
import s3fs
from fsspec.implementations.cached import WholeFileCacheFileSystem

st.set_page_config(
    page_title="Australian Electricity",
    page_icon="✅",
    layout="wide",
)
                   

# dashboard title
st.title("Australian Electricity Market")

col1, col2 = st.columns([1, 1])

@st.cache_resource(ttl=10*60)
def import_data(x):
   s3_file_system = s3fs.S3FileSystem(
         key=  st.secrets["aws_access_key_id_secret"],
         secret= st.secrets["aws_secret_access_key_secret"] ,
         client_kwargs={
            'endpoint_url': st.secrets["endpoint_url_secret"] 
         } ,
       listings_expiry_time = 10
      )
   fs = WholeFileCacheFileSystem(fs=s3_file_system,cache_storage="./cache")
   duckdb.register_filesystem(fs)
   duckdb.sql('PRAGMA disable_progress_bar')
   df = duckdb.sql(x).df()
   return df

########################################################## Query the Data #####################################

station = import_data("""Select DUID,min(Region) as Region,	min(FuelSourceDescriptor) as FuelSourceDescriptor ,
                                    replace(min(stationame), '''', '') as stationame, min(DispatchType) as DispatchType
                                    from  parquet_scan('s3://aemo/aemo/duid/duid.parquet' ) group by all """)
scada=import_data(f"""
      Select SETTLEMENTDATE, DUID, MIN(SCADAVALUE) as mw
            from  parquet_scan('s3://aemo/aemo/scada/data/*/*.parquet' , HIVE_PARTITIONING = 1)
            group by all order by DUID,SETTLEMENTDATE    
                  """)

try :
    DUID_Select= st.sidebar.multiselect('Select Station', duckdb.sql(''' Select distinct stationame from  station order by stationame ''').df() )

    xxxx = "','".join(DUID_Select)
    filter =  "'"+xxxx+"'"
    if len(DUID_Select) != 0 :
        results= duckdb.sql(f''' Select SETTLEMENTDATE,(SETTLEMENTDATE - INTERVAL 10 HOUR) as LOCALDATE,stationame,sum(mw) as mw from  scada
                            inner join station
                            on scada.DUID = station.DUID
                            where stationame in ({filter}) group by all  order by SETTLEMENTDATE  desc
                            ''').df() 
        c = alt.Chart(results).mark_area().encode(x=alt.X('LOCALDATE:T', axis=alt.Axis(title="")), y='mw:Q',color='stationame:N',
                                            tooltip=['LOCALDATE','stationame','mw']).properties(
                                                
                                                width=1200,
                                                height=400)
    else:
        results= duckdb.sql(f''' Select SETTLEMENTDATE,(SETTLEMENTDATE - INTERVAL 10 HOUR) as LOCALDATE,FuelSourceDescriptor,sum(mw) as mw from  scada
                            inner join station
                            on scada.DUID = station.DUID
                            group by all  order by SETTLEMENTDATE  desc
                            ''').df() 
        c = alt.Chart(results).mark_area().encode( x=alt.X('LOCALDATE:T', axis=alt.Axis(title="")), y='mw:Q',color='FuelSourceDescriptor:N',
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
    csv = duckdb.sql(''' Select * EXCLUDE(LOCALDATE) from  results ''').df()
    col2.download_button(
        label="Download data as CSV",
        data=csv.to_csv().encode('utf-8'),
        file_name='large_df.csv',
        mime='text/csv',
    )
    del results
    del csv
    del scada


    link='[for a Full experience go to Nemtracker Dashboard](https://datastudio.google.com/reporting/1Fah7mn1X9itiFAMIvCFkj_tEYXHdxAll/page/TyK1)'
    col1.markdown(link,unsafe_allow_html=True)

    link='[Source code](https://github.com/djouallah/aemo_tracker/blob/main/app.py)'
    col1.markdown(link,unsafe_allow_html=True)
except:
    st.write('first run will take time')
