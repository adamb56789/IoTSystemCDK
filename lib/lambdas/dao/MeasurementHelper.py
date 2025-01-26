
from datetime import date, datetime, time, timedelta

import numpy as np
from dao.MeasurementsBucket import MeasurementsBucket
from dao.MeasurementsTable import MeasurementsTable

# If the number of days we would need to download exceeds this value, download
# the entire month instead.
MAXIMUM_DAY_COUNT = 5
MAXIMUM_MONTH_COUNT = 5

def filter_by_date_sorted(array: np.ndarray, start_date: datetime, end_date: datetime) -> np.ndarray:
    start_millis = start_date.timestamp() * 1000
    end_millis = end_date.timestamp() * 1000

    start_idx = np.searchsorted(array[:, 0], start_millis, side='left')
    end_idx = np.searchsorted(array[:, 0], end_millis, side='right')

    return array[start_idx:end_idx]


def is_today(date: datetime):
    return date.date() == datetime.today().date()


def is_this_month(date: datetime):
    today = datetime.today().date()
    return date.year == today.year and date.month == today.month


class MeasurementHelper:

    def __init__(self, table: MeasurementsTable, bucket: MeasurementsBucket):
        self.table = table
        self.bucket = bucket
    
    def _get_data_in_month(self, device: str, start: datetime, end: datetime) -> np.ndarray:
        if end.day == 1 and end.time() == time(0, 0, 0, 0):
            end -= timedelta(microseconds=1)
        print(f"Getting data in month {start} to {end}")

        days_covered = end.day - start.day + 1
        print(f"Covering {days_covered} days")

        if days_covered > MAXIMUM_DAY_COUNT:
            print(f"{days_covered} is greater than the threshold {MAXIMUM_DAY_COUNT} so downloading the month instead")
            month_array = self.bucket.download_month(device, start)
            if month_array is None:
                raise Exception(f"Unable to find data for month {start}")
        else:
            month_array = self.bucket.download_days_in_range(device, start.year, start.month, start.day, end.day - (1 if is_today(end) else 0))
            if month_array is None:
                raise Exception(f"Unable to find data for days {start.day} to {end.day}")
    
        # If the end includes today then we need to get the latest data from the table
        if is_today(end):
            print("Getting today's data from the table")
            today_array = self.table.get_sensor_data(device, datetime(end.year, end.month, end.day), end)
            print(f"Got {today_array.shape} from today")
            month_array = np.append(month_array, today_array, axis=0)

        return month_array


    def _get_data_in_year(self, device: str, start: datetime, end: datetime) -> np.ndarray:
        if end.month == 1 and end.day == 1 and end.time() == time(0, 0, 0, 0):
            end -= timedelta(microseconds=1)
        print(f"Getting data in year {start} to {end}")

        months_covered = end.month - start.month + 1

        if months_covered == 1:
            print("Covering just one month so going directly to month")
            data = self._get_data_in_month(device, start, end)
        elif months_covered > MAXIMUM_MONTH_COUNT:
            print(f"{months_covered} is greater than the threshold {MAXIMUM_MONTH_COUNT} so downloading the year instead")
            data = self.bucket.download_year(device, start)
            if data is None:
                raise Exception(f"Unable to find data for year {start.year}")
            
            if is_this_month(end):
                print("End month is this month so need to get this months data separately")
                this_month = self._get_data_in_month(device, datetime(end.year, end.month, 1), end)
                data = np.append(data, this_month, axis=0)
        else:
            first_month = self._get_data_in_month(device, start, datetime(start.year, start.month + 1, 1))
            month_arrays = [first_month]
            month_number = start.month + 1
            while month_number < end.month:
                middle_month_array = self.bucket.download_month(device, date(start.year, month_number, 1))
                if middle_month_array is None:
                    raise Exception(f"Unable to find data for month {start.year}-{month_number}")
                month_arrays.append(middle_month_array)
                month_number += 1
            last_month = self._get_data_in_month(device, datetime(end.year, end.month, 1), end)
            month_arrays.append(last_month)
            data = np.concatenate(month_arrays, axis=0)

        return data

    
    def get_data_in_range(self, device: str, start: datetime, end: datetime) -> np.ndarray:

        if is_today(start) and is_today(end):
            return self.table.get_sensor_data(device, start, end)
        else:
            years_covered = end.year - start.year + 1

            if years_covered == 1:
                print("Covering just one year so going directly to year")
                data = self._get_data_in_year(device, start, end)
            else:
                first_year = self._get_data_in_year(device, start, datetime(start.year + 1, 1, 1))
                year_arrays = [first_year]
                year_number = start.year + 1
                while year_number < end.year:
                    middle_year_array = self.bucket.download_year(device, date(year_number, 1, 1))
                    if middle_year_array is None:
                        raise Exception(f"Unable to find data for year {start.year}")
                    year_arrays.append(middle_year_array)
                    year_number += 1
                final_year = self._get_data_in_year(device, datetime(end.year, 1, 1), end)
                year_arrays.append(final_year)
                data = np.concatenate(year_arrays, axis=0)
        
        data = filter_by_date_sorted(data, start, end)
        return data
