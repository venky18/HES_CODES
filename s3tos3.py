import os
import boto3
from boto3.s3.transfer import TransferConfig
import progressbar
import urllib3
import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

Credentials = {}
Credentials['AccessKeyId']="ASIA6J2D7HCNBLAVGYT4"
Credentials['SecretAccessKey']="Zxhy75/1h4j9rGHeXEZyNjazDC9VE9t7+MuOljvl"
Credentials['SessionToken']="FwoGZXIvYXdzEKL//////////wEaDLeuyIr1PJU26XbldSK5ASAWF/210bdPpFl8AwIfva6Qt5HJG5QMGNzxnR2DUuAmP5hl5MbsdvQaxo+REbV3Zl9CLgmDfmEJtJVwzsU0zBwP8TpTUiblieIVjvFmAOizvJHrbE//m4PETDYSgU0rgrVB7euK2dkfk6jrbJz1mYMb29GGXUYPIJWSX3ZstAAnpZJxpt6UrA68latCh0iP43yHVaA72uMQsE0s46Zz8gDdHTFSS4MgSV/rFIP3ySXpPzP6H2AFidEnKPGn4P8FMi0SFFKuhKeOoFKgCD6wPnUtTE9mAQZ2EQNCNSciUbtXOhk41hGDUCVeZjZAp0c="
DEST_BUCKET = "aws-a0095-glbl-00-p-s3b-hes-awb-data20"
SRC_BUCKET = "aws-a0095-use1-00-d-s3b-shrd-shr-input01"

dest_s3 = boto3.client(
    "s3",
    aws_access_key_id=Credentials["AccessKeyId"],
    aws_secret_access_key=Credentials["SecretAccessKey"],
    aws_session_token=Credentials["SessionToken"],
    verify=False,
)
src_s3 = boto3.session.Session().client("s3", verify=False)

src_prefix = 'Output_Files/Px/WO_PRIORI/p02_px_dc_ta_phyrole_wo_priori/'
dest_prefix = 'Output_Files/Px/WO_PRIORI/p02_px_dc_ta_phyrole_wo_priori/'

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
#
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