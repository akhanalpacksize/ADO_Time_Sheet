import requests
import datetime
import csv
import base64
import calendar
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any
from commons import ADO_Time_log
from config.env import Devops_token, timelog_key

# --- Configuration ---
ORGANIZATION = 'packsize'
PROJECT = 'Packsize Product Development'
TEAM_AREA_PATHS = {
    'Team_NABU_RnD_HW_Sustaining': 'Team_NABU_RnD_HW_Sustaining',
    'Team_RnD_Design': 'Team_RnD_Design',
    'Team_PLC': 'Team_PLC',
    'Team_PLC_TechDebt': 'Team_PLC_TechDebt',
    'Team_PLC_X5N': 'Team_PLC_X5N'
}

AZURE_DEVOPS_URL = "https://dev.azure.com"
BOZNET_API_ROOT = "https://boznet-timelogapi.azurewebsites.net/api"
BOZNET_FUNCTION_KEY = "29784d26-5ac9-4146-909f-c142590bc417"

CSV_FIELDNAMES = [
    'workItemId', 'ProductType', 'comment', 'week', 'timeTypeDescription',
    'minutes', 'date', 'month', 'userName', 'createdOn', 'createdBy',
    'updatedOn', 'updatedBy', 'deletedOn', 'deletedBy', 'timesheet_state'
]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_auth_headers(token: str) -> Dict[str, str]:
    credentials = base64.b64encode(f':{token}'.encode()).decode()
    return {
        'Authorization': f'Basic {credentials}',
        'Content-Type': 'application/json'
    }


def get_all_work_item_ids(organization: str, project: str, team_area_paths: Dict[str, str], headers: Dict[str, str]) -> List[int]:
    all_ids = []
    wiql_url = f'{AZURE_DEVOPS_URL}/{organization}/{project}/_apis/wit/wiql?api-version=7.0'
    for area_path in team_area_paths.values():
        wiql_query = {
            "query": f"SELECT [System.Id] FROM WorkItems "
                     f"WHERE [System.TeamProject] = '{project}' "
                     f"AND [System.AreaPath] UNDER '{project}\\\\{area_path}'"
        }
        response = requests.post(wiql_url, headers=headers, json=wiql_query)
        response.raise_for_status()
        ids = [item['id'] for item in response.json()['workItems']]
        all_ids.extend(ids)
    return all_ids


def get_work_items_batch(ids: List[int], organization: str, project: str, headers: Dict[str, str]) -> List[Dict]:
    url = f'{AZURE_DEVOPS_URL}/{organization}/{project}/_apis/wit/workitemsbatch?api-version=7.0'
    payload = {
        "ids": ids,
        "fields": ["System.Id", "Custom.ProductType", "System.State"]  # <-- Added System.State
    }
    resp = requests.post(url, headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json()['value']


def extract_month_name(date_str: str) -> str:
    if not date_str or len(date_str) < 7:
        return ''
    try:
        month_num = int(date_str[5:7])
        return calendar.month_name[month_num]
    except (ValueError, IndexError):
        return ''


def fetch_time_logs_for_work_item(args) -> tuple[int, List[Dict]]:
    wid, api_key, year = args
    url = f"{BOZNET_API_ROOT}/{BOZNET_FUNCTION_KEY}/timelog/query"
    params = {
        'createdOnFromDate': f"{year}-01-01T00:00:00",
        'workitemId': wid
    }
    headers = {"x-functions-key": api_key}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        if resp.status_code == 404:
            return wid, []
        resp.raise_for_status()
        return wid, resp.json()
    except Exception as e:
        logger.error(f"Failed to fetch logs for work item {wid}: {e}")
        return wid, []


def main():
    devops_token = Devops_token
    time_log_key = timelog_key
    headers = get_auth_headers(devops_token)

    current_year = datetime.datetime.now().year

    logger.info("Fetching all work item IDs...")
    all_ids = get_all_work_item_ids(ORGANIZATION, PROJECT, TEAM_AREA_PATHS, headers)
    logger.info(f"Total work items: {len(all_ids)}")

    # Fetch work item details in batches
    work_items = []
    for i in range(0, len(all_ids), 200):
        batch = all_ids[i:i+200]
        work_items.extend(get_work_items_batch(batch, ORGANIZATION, PROJECT, headers))
    logger.info(f"Fetched details for {len(work_items)} work items.")

    # Map work item ID to ProductType and State
    id_to_info = {}
    for item in work_items:
        fields = item['fields']
        wid = fields['System.Id']
        product_type = fields.get('Custom.ProductType', '')
        state = fields.get('System.State', '')
        id_to_info[wid] = {'ProductType': product_type, 'State': state}

    csv_file = ADO_Time_log
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()

        # Prepare tasks for concurrent time log fetching
        tasks = [(wid, time_log_key, current_year) for wid in all_ids]
        results = {}

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_wid = {executor.submit(fetch_time_logs_for_work_item, task): task[0] for task in tasks}
            for future in as_completed(future_to_wid):
                wid, logs = future.result()
                results[wid] = logs

        # Write all rows — including those with no logs
        for wid in all_ids:
            logs = results.get(wid, [])
            info = id_to_info.get(wid, {'ProductType': '', 'State': ''})
            product_type = info['ProductType']
            state = info['State']

            if logs:
                for log in logs:
                    row = {k: log.get(k, '') for k in CSV_FIELDNAMES if k not in ('workItemId', 'ProductType', 'state', 'month')}
                    row['workItemId'] = wid
                    row['ProductType'] = product_type
                    row['timesheet_state'] = state  # <-- From ADO work item
                    row['month'] = extract_month_name(log.get('date', ''))
                    writer.writerow(row)
            else:
                # Placeholder row for work items with no time logs
                empty_row = {k: '' for k in CSV_FIELDNAMES}
                empty_row['workItemId'] = wid
                empty_row['ProductType'] = product_type
                empty_row['timesheet_state'] = state  # <-- Still include state
                writer.writerow(empty_row)

    logger.info(f"✅ Output written to {csv_file}")


if __name__ == "__main__":
    main()