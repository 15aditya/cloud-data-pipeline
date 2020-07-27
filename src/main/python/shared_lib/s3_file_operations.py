import os
import boto3

from urllib.parse import urlparse

def delete_folder(folder_path):
    """
    delete's s3 folder
    """
    url_res = urlparse(folder_path)
    s3_res = boto3.resource('s3')
    bucket.objects.filter(Prefix=url_res.path.strip('/') + '/').delete()

