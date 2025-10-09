import csv
import logging

from pydomo import Domo, Column, ColumnType, Schema
from pydomo import DataSetRequest

from commons import ADO_Time_log
from config.env import CLIENT_ID, CLIENT_SECRET,ADO_time_log_ID
from utils import send_email_error
from logger_config import setup_logging

# Setup logging
setup_logging(module_name="Upload Cost Data")
logger = logging.getLogger(__name__)

date_time_ADO_timesheet = {'date', 'createdOn', 'updatedOn', 'deletedOn', }



def get_new_column(file_path):
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)



def get_column_type(column_name, date_time_columns):
    if column_name in date_time_columns:
        return ColumnType.DATETIME
    else:
        return ColumnType.STRING




def upload_dataset(domo, dataset_id, file_path, date_time_columns, dataset_name, dataset_description):
    # Update a DataSet's metadata
    ds = domo.datasets
    update = DataSetRequest()

    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)

    # update.schema = Schema([Column(get_column_type(col, date_time_columns), col) for col in headers])
    update.schema = Schema([Column(get_column_type(col, date_time_columns), col) for col in headers])
    update.name = dataset_name
    update.description = dataset_description

    ds.update(dataset_id, update)
    ds.data_import_from_file(dataset_id, file_path)
    logger.info("Uploaded data from a file to Dataset {}".format(dataset_id, dataset_name))


def upload_csv_to_domo_daily():
    try:
        datasets_info = [

            (ADO_time_log_ID,ADO_Time_log,date_time_ADO_timesheet,
             'ADO Time Sheet', 'Contains Time Sheet data from ADO'),

        ]
        domo = Domo(CLIENT_ID, CLIENT_SECRET, api_host='api.domo.com')

        for dataset_id, filepath, date_time_columns, dataset_name, dataset_description in datasets_info:
            upload_dataset(domo, dataset_id, filepath, date_time_columns, dataset_name,
                           dataset_description)
    except Exception as exc:
        send_email_error(exc)


if __name__ == "__main__":
    upload_csv_to_domo_daily()