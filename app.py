import streamlit as st
from datetime import datetime , timedelta
import duckdb 
import pytz
import altair as alt
import s3fs
import os
from fsspec.implementations.cached import WholeFileCacheFileSystem
st.set_page_config(
    page_title="Australian Electricity",
    page_icon="✅"
)
st.title("Australian Electricity Market")
col1, col2 = st.columns([1, 1])
nbr_days=34
now = datetime.now(pytz.timezone('Australia/Brisbane'))
@st.cache_resource(ttl=5*60)
def import_data():
  
  s3_file_system = s3fs.S3FileSystem(
         key=  st.secrets["aws_access_key_id_secret"],
         secret= st.secrets["aws_secret_access_key_secret"] ,
         client_kwargs={
            'endpoint_url': st.secrets["endpoint_url_secret"] 
         } ,
      listings_expiry_time = 10
      )
  fs = WholeFileCacheFileSystem(fs=s3_file_system,cache_storage="./cache")
  
  con=duckdb.connect('db')
  con.register_filesystem(fs)
  con.sql('PRAGMA disable_progress_bar ; install httpfs; LOAD httpfs;')
  con.sql(""" create or replace table station as 
            Select DUID,min(Region) as Region,	min(trim(FuelSourceDescriptor)) as FuelSourceDescriptor ,
            replace(min(stationame), '''', '') as stationame, min(DispatchType) as DispatchType
            from  parquet_scan('s3://aemo/aemo/duid/*.parquet' ) group by all
                          """)
  array_list_ls =[f" 's3://aemo/aemo/scada/data/Date={datetime.strftime(now - timedelta(days=x), '%Y-%m-%d')}/*.parquet' " for x in range(0, nbr_days) ]
  array_list =", ".join(array_list_ls)
  con.sql(f"""create or replace table scada as 
             Select SETTLEMENTDATE, DUID, MIN(SCADAVALUE) as mw
            from  parquet_scan([{array_list}])  group by all  
                  """)
  return con
########################################################## Query the Data #####################################
max_day = st.slider('Filter days', 0, nbr_days, 7)

con=duckdb.connect('db')
try :
    station_list = con.sql(''' Select distinct stationame from  station
                               order by stationame''').df()
    DUID_Select= st.multiselect('Select Station', station_list  )

    xxxx = "','".join(DUID_Select)
    filter =  "'"+xxxx+"'" 
    if len(DUID_Select) != 0 :
        
        results= con.sql(f''' Select SETTLEMENTDATE,(SETTLEMENTDATE - INTERVAL 10 HOUR) as UTC,stationame,sum(mw) as mw from  scada
                            inner join station
                            on scada.DUID = station.DUID
                            where stationame in ({filter}) and SETTLEMENTDATE >= '{datetime.strftime(now - timedelta(days=max_day), '%Y-%m-%d')}' 
                            group by all
                            ''').df() 
        c = alt.Chart(results).mark_area().encode(x=alt.X('UTC:T', axis=alt.Axis(title="")), y='mw:Q',color='stationame:N',
                                            tooltip=['SETTLEMENTDATE','stationame','mw']).properties(
                                                width=1200,
                                                height=400)
        
    else:
        if max_day <= 2 :
            details='minutes'
        else:
            details='hour'
        results= con.sql(f''' Select date_trunc('{details}',(SETTLEMENTDATE - INTERVAL 10 HOUR)) as UTC,date_trunc('hour',SETTLEMENTDATE) as SETTLEMENTDATE,
                            FuelSourceDescriptor,sum(mw)/12 as mwh from  scada
                            inner join station
                            on scada.DUID = station.DUID
                            where SETTLEMENTDATE >= '{datetime.strftime(now - timedelta(days=max_day), '%Y-%m-%d')}'
                            group by all
                            ''').df() 
        
        selection = alt.selection_multi(fields=['FuelSourceDescriptor'], bind='legend')
        c = alt.Chart(results).mark_area().encode( x=alt.X('UTC:T', axis=alt.Axis(title="")),
                                                   y='mwh:Q',
                                                   color='FuelSourceDescriptor:N',
                                                  opacity=alt.condition(selection, alt.value(1), alt.value(0)),
                                                tooltip=['SETTLEMENTDATE','FuelSourceDescriptor','mwh']).properties(
                                                    width=1200,
                                                    height=400).add_selection(
                                                             selection
                                                      )
    max= con.sql('''select strftime(max(SETTLEMENTDATE), '%A, %-d %B %Y - %I:%M:%S %p') as max from scada''').fetchone()
    st.write(max)
    #st.subheader("Latest Updated: " + str(max[['test']].values[0][0]))

    ############################################################# Visualisation ####################################
    #localdate is just a stupid hack, Javascript read datetime as UTC not local time :(

    st.write(c)
    #st.write(con.sql('select count(*) as total_records from scada').df())
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
    con = import_data()
except:
    st.write('first run will take time')
    con =import_data()
