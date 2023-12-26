import streamlit as st
from datetime import datetime ,timedelta
import duckdb 
import pytz
import altair as alt
from deltalake import DeltaTable
import time
st.set_page_config(
    page_title="Building a delta lake using Python and DuckDB",
    page_icon="✅" ,
    layout="wide"
)
st.title("End to End Solution using Python, Cloudflare R2 (with Delta table),  DuckDB and Streamlit")
st.write(" Source Data, Australian Energy Market Operator: [AEMO](http://nemweb.com.au/Reports/Current/Dispatch_SCADA/) ") 
col1, col2 = st.columns([1, 1])
now = datetime.now(pytz.timezone('Australia/Brisbane'))
################################## generate DB#######################################
@st.cache_resource(ttl=24*60*60) 
def build_DB():
  con=duckdb.connect('db')
  con.sql(f'''
        set enable_progress_bar=false;
        PRAGMA enable_object_cache; SET enable_http_metadata_cache=true ;
        set s3_region = 'auto';
        set s3_access_key_id = "{st.secrets["aws_access_key_id_secret"]}" ;
        set s3_secret_access_key = '{st.secrets["aws_secret_access_key_secret"] }';
        set s3_endpoint = '{st.secrets["endpoint_url_secret"].replace("https://", "")}'  ;
        SET s3_url_style='path';
        ''')
  con.sql(''' CREATE TABLE IF NOT EXISTS scada(filename VARCHAR, SETTLEMENTDATE TIMESTAMP, DUID VARCHAR, mw  DOUBLE ) ''')
  con.sql(''' CREATE or replace view scada_view as select SETTLEMENTDATE, DUID, min(mw) as mw from scada group by all ''')
  con.sql(""" create or replace table station as 
            Select DUID,min(Region) as Region,	min(trim(FuelSourceDescriptor)) as FuelSourceDescriptor ,
            replace(min(stationame), '''', '') as stationame, min(DispatchType) as DispatchType
            from  parquet_scan('s3://aemo/duid/*.parquet' ) group by all
                          """)
  return con
################################## Data import from Cloudflare R2#########################
@st.cache_resource(ttl=5*60) 
def import_data():
  delta_path = 's3://aemo/scada'
  storage_options = {
  "Region": "us-east-1",   
  "AWS_ACCESS_KEY_ID":  st.secrets["aws_access_key_id_secret"],
  "AWS_SECRET_ACCESS_KEY": st.secrets["aws_secret_access_key_secret"]   ,   
  "AWS_ENDPOINT_URL" : st.secrets["endpoint_url_secret"]
        }
  ###  dt.files() return the list of parquet files, from the delta log
  #    without an expensive directory list, that's pretty much the core idea here
  start = time.time()
  first_run = con.sql(''' Select count(*) as total from scada ''').df()
  xx=first_run[['total']].values[0][0]
  dt = DeltaTable(delta_path,storage_options=storage_options)
  cw=now.strftime('%Y%U')
  if xx == 0  :
   filelist= dt.files(partition_filters = [("week","=",cw)])
  else :
   array_list_ls =[(now-timedelta(days=x)).strftime('%Y%U') for x in range(0, 70,7) ]
   filelist= dt.files(partition_filters = [("week","in",array_list_ls)])
  stop = time.time()
  duration = round(stop-start,2)
  with st.expander("General Stats"):
   st.write('Delta Lake file listing duration (Second): '+str(duration))
   st.write('Total Nbr of files: '+str(len(filelist)))
   st.write("Total records " + str(xx))
  delta = [delta_path +"/" + i for i in filelist]
  duck=con.sql(''' select distinct filename from scada ''').df()
  duck=duck['filename'].to_list()
  files_to_insert = list(set(delta) - set(duck))
  files_to_delete = list(set(duck)-set(delta))
  if len(files_to_delete) != 0 :
   for i in files_to_delete :
    con.execute(f''' delete from scada where filename = '{i}' ''')
  if len(files_to_insert) != 0 :
    start = time.time()
    insert = con.execute(f''' insert into scada  Select filename ,SETTLEMENTDATE, DUID,MIN(SCADAVALUE) as mw
    from  parquet_scan({files_to_insert},filename=1) group by all  ''').df()
    stop = time.time()
    duration = round(stop-start,2)
    with st.expander("Data inserted"):
     st.write('Reading from R2 and Inserting into DuckDB (Second): '+str(duration))
     st.write('Total Nbr of files: '+str(len(files_to_insert)))
     st.write("nbr of records inserted " + str(insert[['Count']].values[0][0]))
  with st.expander("Remote Storage Data"):
   st.dataframe(dt.get_add_actions(flatten=True).to_pandas(),use_container_width=True)
  with st.expander("DuckDB Database"):
   st.dataframe(con.execute('PRAGMA database_size').df())
  duck=con.sql(''' checkpoint''')
  return con
########################################################## Query the Data ########################
max_day = col1.selectbox('Filter days', (1, 7,14,21,28,35))
con=duckdb.connect('db')
try :
    station_list = con.sql(''' Select distinct stationame from  station order by stationame''').df()
    DUID_Select= col2.multiselect('Select Station', station_list  )

    xxxx = "','".join(DUID_Select)
    filter =  "'"+xxxx+"'" 
    if len(DUID_Select) != 0 :
        
        results= con.sql(f''' Select SETTLEMENTDATE,(SETTLEMENTDATE - INTERVAL 10 HOUR) as date,stationame,sum(mw) as mw from  scada_view
                            inner join station
                            on scada_view.DUID = station.DUID
                            where stationame in ({filter}) and SETTLEMENTDATE >= '{datetime.strftime(now - timedelta(days=max_day), '%Y-%m-%d')}' 
                            group by all
                            ''').df() 
        c = alt.Chart(results).mark_area().encode(x=alt.X('date:T', axis=alt.Axis(title="")), y='mw:Q',color='stationame:N',
                                            tooltip=[alt.Tooltip("date:T", format="%Y-%b-%d %I:%M%p"), 'stationame','mw']).properties(
                                                width=1600,
                                                height=400)
        
    else:
        results= con.sql(f''' Select date_trunc('hour',(SETTLEMENTDATE - INTERVAL 10 HOUR)) as date,date_trunc('hour',SETTLEMENTDATE) as SETTLEMENTDATE,
                            FuelSourceDescriptor,sum(mw)/12 as mwh from  scada_view
                            inner join station
                            on scada_view.DUID = station.DUID
                            where SETTLEMENTDATE >= '{datetime.strftime(now - timedelta(days=max_day), '%Y-%m-%d')}'
                            group by all
                            ''').df() 
        
        selection = alt.selection_point(fields=['FuelSourceDescriptor'], bind='legend')
        c = alt.Chart(results).mark_area().encode( x=alt.X('date:T', axis=alt.Axis(title="")),
                                                   y='mwh:Q',
                                                   color='FuelSourceDescriptor:N',
                                                  opacity=alt.condition(selection, alt.value(1), alt.value(0)),
                                                  tooltip=[alt.Tooltip("date:T", format="%Y-%b-%d %I%p"), 'FuelSourceDescriptor','mwh']).properties(
                                                    width=1600,
                                                    height=400).add_params (
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
    
    link='[About](https://datamonkeysite.com/about/)'
    col1.markdown(link,unsafe_allow_html=True)
    con = build_DB()
    con = import_data()
except:
    st.write('first run will take time')
    con = build_DB()
    con = import_data()
