from src.driver import DataSyncClient
from pprint import pprint
import base64
from src.config import FinancialQueries


if __name__ == "__main__":
    test = DataSyncClient(".env", "config.json")
    

    # test.lark_client.delete_table("影票销售明细")


    test.download_data(FinancialQueries("C01", 'day', '2025-07-26'))
    # test._upload_most_recent_data("C01", "影票销售明细")
    # list_of_id = test.lark_client.get_table_records_id_before_some_day("影票销售明细", some_day=13, time_stamp_column_name="销售时间")
    # test.lark_client.delete_records("影票销售明细", list_of_id)
    # with open("output.txt", "w", encoding="utf-8") as f:
    #     pprint(data, stream=f)


    # b64_string = 'cGFnZVRva2VuOjM2'
    # decoded_bytes = base64.b64decode(b64_string)
    # decoded_str = decoded_bytes.decode('utf-8')

    # print(decoded_str)

    # test.sync_most_recent_data('C01', '影票销售明细')

