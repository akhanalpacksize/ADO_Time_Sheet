import csv
import os

from  pydomo import Domo, Column, ColumnType, Schema
from pydomo import DataSetRequest
from config.env import CLIENT_ID, CLIENT_SECRET
from utils import send_email_error


def datasets(domo, file_path):
    ds = domo.datasets
    dsr = DataSetRequest()
    dsr.name = os.path.basename(file_path)

    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)

    columns = [Column(ColumnType.STRING, col) for col in headers]
    dsr.schema = Schema(columns)
    dataset = ds.create(dsr)
    print("Created Dataset ", dataset['id'])


if __name__ == "__main__":
    try:
        csv_files = [
           'ADO_timelogs_all.csv'
        ]
        domo = Domo(CLIENT_ID, CLIENT_SECRET, api_host='api.domo.com')

        for file_path in csv_files:
            datasets(domo, file_path)
    except Exception as exc:
        send_email_error(exc)
