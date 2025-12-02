from fetch_tickets import export_work_items_to_csv
from time_log import merge_work_items_with_logs
from upload_csv_to_domo_daily import upload_csv_to_domo_daily

if __name__ == "__main__":
    try:
        export_work_items_to_csv("work_items.csv")
        merge_work_items_with_logs("work_items.csv")
        upload_csv_to_domo_daily()
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        pass
