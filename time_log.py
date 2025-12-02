import requests
import datetime
import csv
import calendar
from config.env import timelog_key
from utils import safe_request
from commons import ADO_Time_log

# -------------------------------------------------------------------
# Fetch time logs for a work item
# -------------------------------------------------------------------
def get_time_logs(work_item_id, api_key, year=None):
    root_url = "https://boznet-timelogapi.azurewebsites.net/api/"

    if year is None:
        year = datetime.datetime.now().year

    from_date = f"{year}-01-01T00:00:00"

    url = (
        f"{root_url}29784d26-5ac9-4146-909f-c142590bc417/"
        f"timelog/query?createdOnFromDate={from_date}&workitemId={work_item_id}"
    )

    headers = {"x-functions-key": timelog_key}

    # Use safe_request instead of raw requests.get
    resp = safe_request("GET", url, headers=headers)

    # 404 means: no logs for this work item → return empty list
    if resp.status_code == 404:
        return []

    logs = resp.json()
    return logs


# -------------------------------------------------------------------
# Load work items from work_items.csv
# -------------------------------------------------------------------
def load_work_items(file_path):
    items = []
    with open(file_path, 'r', newline='', encoding='utf-8') as wf:
        reader = csv.DictReader(wf)
        fieldnames = reader.fieldnames or []
        for row in reader:
            items.append(row)
    return items, fieldnames


# -------------------------------------------------------------------
# Process + Merge Work Items with Time Logs
# -------------------------------------------------------------------
def merge_work_items_with_logs(work_items_file):
    BOZNET_FUNCTION_KEY = "29784d26-5ac9-4146-909f-c142590bc417"
    work_items, work_item_fieldnames = load_work_items(work_items_file)

    current_year = datetime.datetime.now().year
    # Fields from time log API
    time_log_fields = [
        'month',
        'comment',
        'timeTypeDescription',
        'minutes',
        'date',
        'workItemId',
        'createdOn',
        'createdBy'
    ]

    # Final CSV columns (merge the two sets)
    merged_fields = list(work_item_fieldnames) + [
        f for f in time_log_fields if f not in work_item_fieldnames
    ]


    with open(ADO_Time_log, 'w', newline='', encoding='utf-8') as out_f:
        writer = csv.DictWriter(out_f, fieldnames=merged_fields)
        writer.writeheader()

        for wi in work_items:

            wid = (
                    wi.get('ID') or
                    wi.get('Id') or
                    wi.get('workItemId')
            )

            if not wid:
                continue

            wid_str = str(wid).strip()

            # ---- Get time logs ----
            logs = get_time_logs(wid_str, BOZNET_FUNCTION_KEY, year=current_year)

            # ProductType fallback
            product_type = wi.get('ProductType', '')

            # ---- Merge time logs ----
            if logs:
                for log in logs:
                    row = dict(wi)

                    for fld in time_log_fields:
                        if fld == 'month':
                            date_val = log.get('date', '')
                            month_name = ""
                            if date_val and len(date_val) >= 7:
                                try:
                                    m = int(date_val[5:7])
                                    month_name = calendar.month_name[m]
                                except Exception:
                                    pass
                            row['month'] = month_name
                        else:
                            row[fld] = log.get(fld, "")

                    row['workItemId'] = wid_str

                    if not row.get('ProductType'):
                        row['ProductType'] = product_type

                    writer.writerow(row)

            else:
                # No logs → write item with empty log fields
                row = dict(wi)
                for fld in time_log_fields:
                    row[fld] = ""

                row['workItemId'] = wid_str
                if not row.get('ProductType'):
                    row['ProductType'] = product_type

                writer.writerow(row)

    print(f"\nMerged time logs written to: {ADO_Time_log}")
    return ADO_Time_log


# -------------------------------------------------------------------
# Run script
# -------------------------------------------------------------------
# merge_work_items_with_logs("work_items.csv")
