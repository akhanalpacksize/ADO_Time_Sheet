from time_log import  main
from upload_csv_to_domo_daily import upload_csv_to_domo_daily

if __name__ == "__main__":
    try:
        main()
        upload_csv_to_domo_daily()
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        pass
