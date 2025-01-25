
from datetime import datetime
import io
import boto3
import numpy as np


s3 = boto3.resource("s3")

class MeasurementsBucket:

    def __init__(self, bucket_name: str):
        self.bucket = s3.Bucket(bucket_name)

    def _download_file(self, s3_key: str):
        try:
            file_stream = io.BytesIO()
            self.bucket.download_fileobj(s3_key, file_stream)
            file_stream.seek(0)
            data_array = np.load(file_stream)
            print(f"Loaded {s3_key} containing {data_array.shape}")
            return data_array
        except Exception as e:
            print(f'Failed to download or load {s3_key}: {e}')
            return None
    
    def download_day(self, device: str, date: datetime):
        s3_key = f'{device}/{date.strftime("%Y/%m/%d")}/data.npy'
        return self._download_file(s3_key)
    
    def download_month(self, device: str, date: datetime):
        s3_key = f'{device}/{date.strftime("%Y/%m")}/data.npy'
        return self._download_file(s3_key)

    def download_year(self, device: str, date: datetime):
        s3_key = f'{device}/{date.strftime("%Y")}/data.npy'
        return self._download_file(s3_key)
    
    def _upload_file(self, s3_key: str, data_array):
        try:
            file_stream = io.BytesIO()
            np.save(file_stream, data_array)
            file_stream.seek(0)
            
            self.bucket.upload_fileobj(file_stream, s3_key)
            print(f"Uploaded {s3_key} containing {data_array.shape}")
        except Exception as e:
            print(f'Failed to upload {s3_key}: {e}')
    
    def upload_day(self, device: str, date: datetime, data_array):
        s3_key = f'{device}/{date.strftime("%Y/%m/%d")}/data.npy'
        self._upload_file(s3_key, data_array)

    def upload_month(self, device: str, date: datetime, data_array):
        s3_key = f'{device}/{date.strftime("%Y/%m")}/data.npy'
        self._upload_file(s3_key, data_array)

    def upload_year(self, device: str, date: datetime, data_array):
        s3_key = f'{device}/{date.strftime("%Y")}/data.npy'
        self._upload_file(s3_key, data_array)