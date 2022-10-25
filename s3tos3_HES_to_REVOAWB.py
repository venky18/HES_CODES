export PYTHONWARNINGS="ignore:Unverified HTTPS request"
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py
pip install progressbar 
pip install boto3

screen -S Download

python downloadS3.py

import os
import boto3
from boto3.s3.transfer import TransferConfig
import progressbar
import urllib3
import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

Credentials = {}
Credentials['AccessKeyId']="XX"
Credentials['SecretAccessKey']="XX"

DEST_BUCKET = "aws-a0095-glbl-00-p-s3b-hes-awb-data20"
SRC_BUCKET = "aws-a0095-use1-00-d-s3b-shrd-shr-input01"
# Change Here .........
src_prefix = 'Input_Files/SHS/2021JAN/t_physician/'
dest_prefix = 'Input_Files/SHS/2021JAN/t_physician/'

src_s3 = boto3.client(
    "s3",
    aws_access_key_id=Credentials["AccessKeyId"],
    aws_secret_access_key=Credentials["SecretAccessKey"],
    verify=False,
)
dest_s3 = boto3.session.Session().client("s3", verify=False)


paginator =src_s3.get_paginator("list_objects_v2")
operation_parameters = {'Bucket': SRC_BUCKET,
                        'Prefix': src_prefix}
page_iterator = paginator.paginate(**operation_parameters)

def upload_progress(chunk):
    up_progress.update(up_progress.currval + chunk)

config = TransferConfig(multipart_threshold=1024*20,  
                        max_concurrency=10, 
                        multipart_chunksize=1024*20, 
                        use_threads=True)

#Connect to Dest S3, Download as TempFile to /mnt or wherever there is space (max 20GB needed )
#Download the content to disk and upload.   
i = 0
for page in page_iterator:
    for obj in page["Contents"]:
        src_key = obj["Key"]
        des_key = dest_prefix + src_key[len(src_prefix) :]
        # if src_key.split("/")[-1] in downloaded:
        #     continue
        print("started downloading")
        print(obj["Key"])
        print(datetime.datetime.now())
        src_s3.download_file(SRC_BUCKET,obj["Key"],'/mnt/tranTemp')
        statinfo = os.stat('/mnt/tranTemp')
        print("completed downloading",statinfo)
        print(datetime.datetime.now())
        up_progress = progressbar.progressbar.ProgressBar(maxval=statinfo.st_size)
        up_progress.start()
        dest_s3.upload_file('/mnt/tranTemp', DEST_BUCKET, des_key,Config = config,Callback=upload_progress)
        up_progress.finish()
        print("finished uploading")
        os.remove("/mnt/tranTemp")
        i+=1
        print(datetime.datetime.now())
        print(i)