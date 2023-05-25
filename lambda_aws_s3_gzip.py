import tarfile
import io
import boto3
import gzip
import re

s3 = boto3.resource('s3')
client = boto3.client('s3')
s3_paginator = client.get_paginator('list_objects_v2')

json_gzip_ext_pattern = re.compile(".*\.json\.gz")
json_ext_pattern = re.compile(".*\.json")

def _get_bucket_objects(bucket_name,prefix):

  s3filename = bucket_name + '_' + prefix.replace('/','_') + ".tar.gz"
  fh_tarfile = io.BytesIO()
  bucket = dict()
  
  params = { 'Bucket': bucket_name, 'Prefix': prefix }
  
  s3_iterator = s3_paginator.paginate(**params)
  with tarfile.open(fileobj=fh_tarfile, mode='w:gz') as tar:
    for page in s3_iterator:
        if 'Contents' in page:
          for obj in page['Contents']:
            obj_key = obj["Key"]
            
            if (json_ext_pattern.match(obj_key) or json_gzip_ext_pattern.match(obj_key)):
              response = client.get_object(
                  Bucket=bucket_name,
                  Key=obj_key
              )

              # if file extension is .json.gz
              # gunzip file
              if json_gzip_ext_pattern.match(obj_key):
                body = response["Body"].read()
                obj_body_utf8 = gzip.decompress(body)
                # remove .gz extension
                new_obj_key = obj_key.replace('.gz','')
              else:
                obj_body_utf8 = response['Body'].read()
                new_obj_key = obj_key

              try:                  
                obj_body_str = obj_body_utf8.decode('utf-8')
              except:
                obj_body_str = 'unable to decode body (utf-8)'

              try:
                obj_date = t.strftime('%Y-%d-%m %H:%M:%S'), 
              except:
                obj_date = 'unknown object date'

              data_length = len(obj_body_str)
              data_bytes = io.BytesIO(obj_body_str.encode('utf-8'))

              # add to tarball
              info = tarfile.TarInfo(new_obj_key)
              info.size = data_length
              tar.addfile(tarinfo=info, fileobj=data_bytes)

              bucket[obj_key] = {
                'ContentLength': response['ContentLength'],
                'length': data_length,
                'date': obj_date,
                'body': obj_body_str,
                'key': new_obj_key,
                'data_bytes_size': data_bytes.getbuffer().nbytes,
              }
    tar.close()
  tarball_size = fh_tarfile.getbuffer().nbytes
  bucket['fh_tarfile_size'] = tarball_size
  
  if tarball_size > 0:
    fh_tarfile.seek(0)
    client.put_object(Body=fh_tarfile, Bucket='acx-tarballs', Key=s3filename)

  return bucket

def lambda_handler(event, context):
  
  bucket_list = s3.buckets.all()

  buckets = dict()
  #s3://acxdssplunklicence1/2023/24/05/
  bucket_list = [ 
      { 'name': 'acxdssplunklicence1', 'prefix': '2023/24/05/' }
  ]
  
  for bucket in bucket_list:
    buckets[bucket['name']] = _get_bucket_objects(bucket['name'], bucket['prefix'])

  try:
    return {
      "statusCode": 200,
      "body": buckets 
    }
  except:
    return {
      "statusCode": 500,
      "body": "Unable to complete process"
    }
