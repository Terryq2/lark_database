from fetcher import YKYRequester
from feishu_client import FeishuClient
from driver import DataSyncClient
from utility import helpers
from config import FinancialQueries

if __name__ == "__main__":
    test = DataSyncClient(".env", "config.json")
    # test.sync_current_year_data('C01', 'TEST')

    # test.lark_client.delete_table("影票销售明细")
    test.sync_most_recent_data('C01', "影票销售明细")

    # test.uploader.get_wiki_all_table_info(test.uploader.get_wiki_app_token("IDNYwwMHti1AKyki7ArcokTInve"))
    # test.get_financial_data([('C04', 'month', f'2025-02')])
    # # print(test.get_cloud_file_token("IDNYwwMHti1AKyki7ArcokTInve"))
    # for i in range(1, 7):
    #     test.get_financial_data([('C05', 'month', f'2025-0{i}')])
    #     test.post_csv_data_to_feishu("D:\Repositories\lark_database\商品订单数据\商品订单数据.csv", "Jwsbb2LmTaie2xsqpa2cJMjQnAb", "tblxjgcJfxRfNDkD")



    # for day in helpers.get_past_days_this_month():
    #     test.get_financial_data([('C04', 'day', day)])
    #     test.post_csv_data_to_feishu("D:\Repositories\lark_database\卡充值数据\卡充值数据.csv", "Jwsbb2LmTaie2xsqpa2cJMjQnAb", "tblJeCKuBjNu44aG")


    # print(test.create_new_table("Jwsbb2LmTaie2xsqpa2cJMjQnAb", "TEST_2", "C02"))

    
    # print(test.get_tenant_access_token_from_feishu())
    
    

