from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import duckdb
import os


delta_path = 's3://delta/scada'
storage_options = {
"Region": "us-east-1",   
"AWS_ACCESS_KEY_ID":     os.environ.get("aws_access_key_id_secret") ,
"AWS_SECRET_ACCESS_KEY": os.environ.get("aws_secret_access_key_secret")   ,   
"AWS_ENDPOINT_URL" :     os.environ.get("endpoint_url_secret") ,
"AWS_S3_ALLOW_UNSAFE_RENAME":"true"
}
def compaction(request):
    dt = DeltaTable(delta_path,storage_options=storage_options)
    xx =dt.get_add_actions(flatten=True).to_pandas()
    partition = duckdb.sql('''select cast("partition.Date" as string) as  partition , count(*) as tt from xx group by 1 having tt >1''').df()
    days=partition['partition'].to_list()
    if len(days) > 0 :
     for i in days :
      dt = DeltaTable(delta_path,storage_options=storage_options)
      at = dt.to_pyarrow_table(partitions=[("Date", "=",i )])
      write_deltalake(dt, at, partition_filters=[("Date", "=", i)], mode="overwrite")
    return 'done'
