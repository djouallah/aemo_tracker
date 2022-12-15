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

########################################################## Query arrow table as an ordinary SQL Table#####################################
@st.experimental_singleton
def define_view():
    con=duckdb.connect()
    con.execute(f'''
    install httpfs;
    LOAD httpfs;
    --SET enable_http_metadata_cache=true ;
    PRAGMA enable_object_cache ;
    set s3_region = 'auto';
    set s3_access_key_id = "{st.secrets["aws_access_key_id_secret"]}" ;
    set s3_secret_access_key = '{st.secrets["aws_secret_access_key_secret"] }';
    set s3_endpoint = '{st.secrets["endpoint_url_secret"].replace("https://", "")}'  ;
    SET s3_url_style='path';
    create or replace view scada as select  *  from parquet_scan('s3://delta/aemo/scada/data/*/*.parquet' , HIVE_PARTITIONING = 1,filename= 1) ;
    ''')
    return con
con=define_view()
@st.experimental_memo (ttl=5*60)
def get_data():
  return con.execute('''
     with xx as (Select SETTLEMENTDATE, (SETTLEMENTDATE - INTERVAL 10 HOUR) as LOCALDATE , DUID,MIN(SCADAVALUE) as mwh from  scada group by all)
     Select SETTLEMENTDATE,LOCALDATE, sum(mwh) as mwh from  xx group by all order by SETTLEMENTDATE desc
      ''').df() 
results = get_data()
column = results["SETTLEMENTDATE"]
now = str (column.max())
st.subheader("Latest Updated: " + now)

############################################################# Visualisation ##############################################################
#localdate is just a stuid hack, Javascript read datetime as UTC not local time :(
import altair as alt
c = alt.Chart(results).mark_area().encode( x='LOCALDATE:T', y='mwh:Q',
                                          tooltip=['LOCALDATE','mwh']).properties(
                                            width=1200,
                                            height=400)
st.write(c)
df=results[['SETTLEMENTDATE','mwh']]
#st.dataframe(df)

###########################################################Buttons and Links #############################################################
#Download Button


def convert_df(df):
     # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

csv = convert_df(df)
col2.download_button(
     label="Download data as CSV",
     data=csv,
     file_name='large_df.csv',
     mime='text/csv',
 )

del csv,df
link='[Data Source](http://nemweb.com.au/Reports/Current/Dispatch_SCADA/)'
col1.markdown(link,unsafe_allow_html=True)

link='[Blog](https://datamonkeysite.com/2022/06/28/using-delta-lake-with-python/)'
col1.markdown(link,unsafe_allow_html=True)
