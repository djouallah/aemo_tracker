import requests
import pyarrow as pa
import pandas as pd 
from io import BytesIO
from pyarrow import fs
import streamlit as st
import pyarrow.parquet as pq
def loadduid(requet):
  
 url = "https://www.aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/NEM-Registration-and-Exemption-List.xls"

 s = requests.Session()
 headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'}
 r = s.get(url,headers=headers)
 r.content
 df1=pd.read_excel(BytesIO(r.content),sheet_name="Generators and Scheduled Loads",dtype=str)
 df=df1[["Region","DUID","Fuel Source - Descriptor","Reg Cap (MW)","Station Name","Dispatch Type","Participant"]]
 df2=df.rename(columns={"Fuel Source - Descriptor": "FuelSourceDescriptor", "Station Name": "stationame","Dispatch Type":"DispatchType","Reg Cap (MW)":"regcap"})
 df2['length'] = df.DUID.str.len()
 df2 = df2[df2.length >1]
 del df2['length']
 data = [['Rooftop VIC', 'VIC1','Rooftop','VIC1'],['Rooftop SA', 'SA1','Rooftop','SA1'],['Rooftop QLD', 'QLD1','Rooftop','QLD1'],['Rooftop NSW', 'NSW1','Rooftop','NSW1'],['Rooftop TAS', 'TAS1','Rooftop','TAS1']]
 dfrooftop = pd.DataFrame(data, columns = ['stationame', 'Region','FuelSourceDescriptor','DUID'])
 df3 = df2.append(dfrooftop, ignore_index=True)
 #print(df3)
 if len(df3.index) < 523 :
    pass
 else: 
    tb=pa.Table.from_pandas(df3,preserve_index=False)
    s3 = fs.S3FileSystem(region="us-east-1",
                         access_key = st.secrets["aws_access_key_id_secret"],
                         secret_key=st.secrets["aws_secret_access_key_secret"] ,
                         endpoint_override=st.secrets["endpoint_url_secret"] )
    pq.write_table(tb,"aemo/duid/duid.parquet",filesystem=s3)
loadduid('x')
