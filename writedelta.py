import urllib.request as urllib2
import tempfile
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import re ,shutil
from urllib.request import urlopen
import os
from pyarrow import fs
import pyarrow.parquet as pq
from deltalake.writer import write_deltalake
from deltalake import DeltaTable
delta_path = 's3://aemo/scada'
region="us-east-1"
access_key = os.environ.get("aws_access_key_id_secret")
secret_key=os.environ.get("aws_secret_access_key_secret")
endpoint=os.environ.get("endpoint_url_secret")
storage_options = {
"Region": region,   
"AWS_ACCESS_KEY_ID":     access_key ,
"AWS_SECRET_ACCESS_KEY": secret_key  ,   
"AWS_ENDPOINT_URL" :     endpoint ,
"AWS_S3_ALLOW_UNSAFE_RENAME":"true"
}
def get_file_path(filename):
    return os.path.join(tempfile.gettempdir(), filename)
def load(request):   
    
    s3 = fs.S3FileSystem(region= region,
                         access_key =        access_key,
                         secret_key=         secret_key ,
                         endpoint_override=  endpoint
                         )
        
    appended_data = []
    url = "http://nemweb.com.au/Reports/Current/Dispatch_SCADA/"
    result = urlopen(url).read().decode('utf-8')
    pattern = re.compile(r'[\w.]*.zip')
    filelist1 = pattern.findall(result)
    filelist_unique = dict.fromkeys(filelist1)
    filelist_sorted=sorted(filelist_unique, reverse=True)
    filelist = filelist_sorted[:1000]
    try:
        df = ds.dataset("aemo/log/scada/log.parquet",filesystem=s3).to_table().to_pandas()
    except:
        df=pd.DataFrame(columns=['file'])     
    file_loaded= df['file'].unique()
    #print (df)

    current = file_loaded.tolist()
    #print(current)

    files_to_upload = list(set(filelist) - set(current))
    files_to_upload = list(dict.fromkeys(files_to_upload)) 
    print(str(len(files_to_upload)) + ' New File Loaded')
    if len(files_to_upload) != 0 :
      for x in files_to_upload:
            with urlopen(url+x) as source, open(get_file_path(x), 'w+b') as target:
                shutil.copyfileobj(source, target)
            df = pd.read_csv(get_file_path(x),skiprows=1,usecols=["SETTLEMENTDATE", "DUID", "SCADAVALUE"],parse_dates=["SETTLEMENTDATE"])
            df=df.dropna(how='all') #drop na
            df['SETTLEMENTDATE']= pd.to_datetime(df['SETTLEMENTDATE'])
            df['Date'] = df['SETTLEMENTDATE'].dt.date
            df['week'] = df['SETTLEMENTDATE'].dt.strftime('%Y%U')
            df['file'] = x
            appended_data.append(df)
            # see pd.concat documentation for more info
      appended_data = pd.concat(appended_data,ignore_index=True)
      existing_file = pd.DataFrame( file_loaded)
      new_file = pd.DataFrame(  appended_data['file'].unique())
      log = pd.concat ([new_file,existing_file], ignore_index=True)
      #print(log)
      log.rename(columns={0: 'file'}, inplace=True)
      
      log_tb=pa.Table.from_pandas(log,preserve_index=False)
      #print(log_tb)
      log_schema = pa.schema([pa.field('file', pa.string())])
      log_tb=log_tb.cast(target_schema=log_schema)
      
      tb=pa.Table.from_pandas(appended_data,preserve_index=False)
      my_schema = pa.schema([
                      pa.field('SETTLEMENTDATE', pa.timestamp('us')),
                      pa.field('DUID', pa.string()),
                      pa.field('SCADAVALUE', pa.float64()),
                      pa.field('Date', pa.date32()),
                      pa.field('week', pa.string()),
                      pa.field('file', pa.string())
                      ]
                                                       )
      xx=tb.cast(target_schema=my_schema)
      write_deltalake(delta_path, xx,mode='append',partition_by=['week'],storage_options=storage_options)
      pq.write_table(log_tb,"aemo/log/scada/log.parquet",filesystem=s3)
      ########################## Compact and delete old Copies ##########################
      dt = DeltaTable(delta_path,storage_options=storage_options) 
      if len(dt.file_uris(partition_filters = [("week","=",appended_data.at[0,'week'])])) > 20 :
            dt.create_checkpoint()
            dt.optimize.compact()
            dt.vacuum(retention_hours=0,dry_run=False,  enforce_retention_duration=False)
      return "done"
