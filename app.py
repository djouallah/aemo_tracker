import streamlit as st
from datetime import datetime , timedelta
import duckdb 
import pytz
import altair as alt
import time
st.set_page_config(
    page_title="Australian Electricity",
    page_icon="✅"
)
st.title("Australian Electricity Market")
col1, col2 = st.columns([1, 1])
now = datetime.now(pytz.timezone('Australia/Brisbane'))
################################## Data import from Cloudflare R2#########################
@st.cache_resource(ttl=5*60) 
def import_data():
  con=duckdb.connect('db')
  con.sql(f'''
        install httpfs; LOAD httpfs; set enable_progress_bar=false;
        PRAGMA enable_object_cache; SET enable_http_metadata_cache=true ;
        set s3_region = 'auto';
        set s3_access_key_id = "{st.secrets["aws_access_key_id_secret"]}" ;
        set s3_secret_access_key = '{st.secrets["aws_secret_access_key_secret"] }';
        set s3_endpoint = '{st.secrets["endpoint_url_secret"]}'  ;
        SET s3_url_style='path';
        ''')
  dt = con.sql(f''' select distinct filename from  parquet_scan('s3://aemo/aemo/scada/data/*/*.parquet',filename=1)''').df()
  con.sql(''' CREATE TABLE IF NOT EXISTS scada(filename VARCHAR, SETTLEMENTDATE TIMESTAMP, DUID VARCHAR, mw  DOUBLE ) ''')
  con.sql(""" create or replace table station as 
            Select DUID,min(Region) as Region,	min(trim(FuelSourceDescriptor)) as FuelSourceDescriptor ,
            replace(min(stationame), '''', '') as stationame, min(DispatchType) as DispatchType
            from  parquet_scan('s3://aemo/aemo/duid/*.parquet' ) group by all
                          """)
  
  delta=dt['filename'].to_list()
  duck=con.sql(''' select distinct filename from scada ''').df()
  duck=duck['filename'].to_list()
  files_to_insert = list(set(delta) - set(duck))
  files_to_delete = list(set(duck)-set(delta))
  if len(files_to_insert) != 0 :
    con.execute(f''' insert into scada  Select filename ,SETTLEMENTDATE, DUID,MIN(SCADAVALUE) as mw  from  parquet_scan({files_to_insert},filename=1) group by all  ''')
  if len(files_to_delete) != 0 :
   for i in files_to_delete :
    con.execute(f''' delete from scada where filename = '{i}' ''')
  return con
########################################################## Query the Data ########################
start = time.time()
con = import_data()
stop = time.time()
duration = round(stop-start,2)
if duration > 1 :
     st.write('total import duration: '+str(duration))
     st.write(con.sql('select count(*) as total_records from scada').df())
max_day = st.slider('Filter days', 0, 1, 60)
con=duckdb.connect('db')
try :
    station_list = con.sql(''' Select distinct stationame from  station order by stationame''').df()
    DUID_Select= st.multiselect('Select Station', station_list  )

    xxxx = "','".join(DUID_Select)
    filter =  "'"+xxxx+"'" 
    if len(DUID_Select) != 0 :
        
        results= con.sql(f''' Select SETTLEMENTDATE,(SETTLEMENTDATE - INTERVAL 10 HOUR) as date,stationame,sum(mw) as mw from  scada
                            inner join station
                            on scada.DUID = station.DUID
                            where stationame in ({filter}) and SETTLEMENTDATE >= '{datetime.strftime(now - timedelta(days=max_day), '%Y-%m-%d')}' 
                            group by all
                            ''').df() 
        c = alt.Chart(results).mark_area().encode(x=alt.X('date:T', axis=alt.Axis(title="")), y='mw:Q',color='stationame:N',
                                            tooltip=[alt.Tooltip("date:T", format="%Y-%b-%d %I:%M%p"), 'stationame','mw']).properties(
                                                width=1200,
                                                height=400)
        
    else:
        results= con.sql(f''' Select date_trunc('hour',(SETTLEMENTDATE - INTERVAL 10 HOUR)) as date,date_trunc('hour',SETTLEMENTDATE) as SETTLEMENTDATE,
                            FuelSourceDescriptor,sum(mw)/12 as mwh from  scada
                            inner join station
                            on scada.DUID = station.DUID
                            where SETTLEMENTDATE >= '{datetime.strftime(now - timedelta(days=max_day), '%Y-%m-%d')}'
                            group by all
                            ''').df() 
        
        selection = alt.selection_multi(fields=['FuelSourceDescriptor'], bind='legend')
        c = alt.Chart(results).mark_area().encode( x=alt.X('date:T', axis=alt.Axis(title="")),
                                                   y='mwh:Q',
                                                   color='FuelSourceDescriptor:N',
                                                  opacity=alt.condition(selection, alt.value(1), alt.value(0)),
                                                  tooltip=[alt.Tooltip("date:T", format="%Y-%b-%d %I%p"), 'FuelSourceDescriptor','mwh']).properties(
                                                    width=1200,
                                                    height=400).add_selection(
                                                             selection
                                                      )
    max= con.sql('''select strftime(max(SETTLEMENTDATE), '%A, %-d %B %Y - %I:%M:%S %p') as max from scada''').df()
    #st.write(max)
    st.write("Latest Updated: " + str(max[['max']].values[0][0]))

    ############################################################# Visualisation ####################################
    #UTC is just a stupid hack, Javascript read datetime as UTC not local time :(
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
except:
    st.write('first run will take time')
    con = import_data()
