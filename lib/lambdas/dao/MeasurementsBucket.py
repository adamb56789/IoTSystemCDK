
from datetime import date, datetime, timedelta
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

    def _download_file(self, s3_key: str) -> np.ndarray | None:
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
    
    def download_day(self, device: str, date: date) -> np.ndarray | None:
        return self._download_file(day_key(device, date))
    
    def download_days_in_range(self, device: str, year: int, month: int, start_day: int, end_day: int) -> np.ndarray | None:
        day_arrays = []

        for day in range(start_day, end_day + 1):
            day_array = self.download_day(device, date(year, month, day))
            if day_array is None:
                print(f"Could not find {day}")
            elif day_array.shape[0] != 0 and day_array.shape[1] == 3:
                day_arrays.append(day_array)
            else:
                print(f"Shape of day {day} is incorrect, is {day_array.shape}")

        if day_arrays:
            return np.concatenate(day_arrays, axis=0)
    
    def download_month(self, device: str, date: date) -> np.ndarray | None:
        return self._download_file(month_key(device, date))

    def download_months_in_range(self, device: str, year: int, start_month: int, end_month: int) -> np.ndarray | None:
        year = year
        month_arrays = []

        for month in range(start_month, end_month + 1):
            month_datetime = date(year, month, 1)
            
            month_array = self.download_month(device, month_datetime)

            if month_array is None:
                print(f"Could not find {month_datetime}")
            elif month_array.shape[0] != 0 and month_array.shape[1] == 3:
                month_arrays.append(month_array)
            else:
                print(f"Shape of month {month_datetime} is incorrect, is {month_array.shape}")

        if month_arrays:
            return np.concatenate(month_arrays, axis=0)

    def download_year(self, device: str, date: date) -> np.ndarray | None:
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
    
    def upload_day(self, device: str, date: date, data_array: np.ndarray):
        self._upload_file(day_key(device, date), data_array)

    def upload_month(self, device: str, date: date, data_array: np.ndarray):
        self._upload_file(month_key(device, date), data_array)

    def upload_year(self, device: str, date: date, data_array: np.ndarray):
        self._upload_file(year_key(device, date), data_array)
