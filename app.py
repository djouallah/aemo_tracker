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
st.title("Australian Electricity Market")
col1, col2 = st.columns([1, 1])

#@st.cache_resource(ttl=10*60)
#def import_data(x):
s3_file_system = s3fs.S3FileSystem(
         key=  st.secrets["aws_access_key_id_secret"],
         secret= st.secrets["aws_secret_access_key_secret"] ,
         client_kwargs={
            'endpoint_url': st.secrets["endpoint_url_secret"] 
         } ,
       listings_expiry_time = 600
      )
fs = WholeFileCacheFileSystem(fs=s3_file_system,cache_storage="./cache",cache_check=600)
duckdb.register_filesystem(fs)
duckdb.sql('PRAGMA disable_progress_bar')

########################################################## Query the Data #####################################
station = duckdb.sql("""Select DUID,min(Region) as Region,	min(FuelSourceDescriptor) as FuelSourceDescriptor ,
                          replace(min(stationame), '''', '') as stationame, min(DispatchType) as DispatchType
                          from  parquet_scan('s3://aemo/aemo/duid/duid.parquet' ) group by all
                          """).arrow()
scada=duckdb.sql("""
             Select SETTLEMENTDATE, DUID, MIN(SCADAVALUE) as mw
            from  parquet_scan('s3://aemo/aemo/scada/data/*/*.parquet' )
            group by all order by DUID,SETTLEMENTDATE    
                  """).arrow()

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
        results= duckdb.sql(f''' Select date_trunc('day',SETTLEMENTDATE) as day,FuelSourceDescriptor,sum(mw)/12 as mwh from  scada
                            inner join station
                            on scada.DUID = station.DUID
                            group by all
                            ''').df() 
        c = alt.Chart(results).mark_bar().encode( x=alt.X('day:N', axis=alt.Axis(labels=False)), y='mwh:Q',color='FuelSourceDescriptor:N',
                                                tooltip=['day','FuelSourceDescriptor','mwh']).properties(
                                                    width=1200,
                                                    height=400)

    st.subheader("Latest Updated: " + str(scada["SETTLEMENTDATE"].max()))

    ############################################################# Visualisation ####################################
    #localdate is just a stupid hack, Javascript read datetime as UTC not local time :(

    st.write(c)
    
    ###########################################################Buttons and Links ####################################
    #Download Button
    col2.download_button(
        label="Download data as CSV",
        data=results.to_csv().encode('utf-8'),
        file_name='large_df.csv',
        mime='text/csv',
    )
    
    link='[for a Full experience go to Nemtracker Dashboard](https://datastudio.google.com/reporting/1Fah7mn1X9itiFAMIvCFkj_tEYXHdxAll/page/TyK1)'
    col1.markdown(link,unsafe_allow_html=True)

    link='[Source code](https://github.com/djouallah/aemo_tracker/blob/main/app.py)'
    col1.markdown(link,unsafe_allow_html=True)
except:
    st.write('first run will take time')
