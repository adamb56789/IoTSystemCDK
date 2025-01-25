
from datetime import date, datetime
import io
import boto3
import numpy as np


def day_key(device: str, date: date):
    return f'{device}/{date.strftime("%Y/%m/%d")}/data.npy'


def month_key(device: str, date: date):
    return f'{device}/{date.strftime("%Y/%m")}/data.npy'


def year_key(device: str, date: date):
    return f'{device}/{date.strftime("%Y")}/data.npy'


s3 = boto3.resource("s3")

class MeasurementsBucket:

    def __init__(self, bucket_name: str):
        self.bucket = s3.Bucket(bucket_name) # type: ignore

    def _download_file(self, s3_key: str):
        try:
            file_stream = io.BytesIO()
            self.bucket.download_fileobj(s3_key, file_stream)
            file_stream.seek(0)
            data_array = np.load(file_stream)
            print(f"Downloaded {s3_key} containing {data_array.shape}")
            return data_array
        except Exception as e:
            print(f'Failed to download or load {s3_key}: {e}')
            return None
    
    def download_day(self, device: str, date: date):
        return self._download_file(day_key(device, date))
    
    def download_month(self, device: str, date: date):
        return self._download_file(month_key(device, date))

    def download_year(self, device: str, date: date):
        return self._download_file(year_key(device, date))
    
    def _upload_file(self, s3_key: str, data_array):
        try:
            file_stream = io.BytesIO()
            np.save(file_stream, data_array)
            file_stream.seek(0)
            
            self.bucket.upload_fileobj(file_stream, s3_key)
            print(f"Uploaded {s3_key} containing {data_array.shape}")
        except Exception as e:
            print(f'Failed to upload {s3_key}: {e}')
    
    def upload_day(self, device: str, date: date, data_array):
        self._upload_file(day_key(device, date), data_array)

    def upload_month(self, device: str, date: date, data_array):
        self._upload_file(month_key(device, date), data_array)

    def upload_year(self, device: str, date: date, data_array):
        self._upload_file(year_key(device, date), data_array)
