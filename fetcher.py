"""This is a class that helps fetching data from the yuekeyun website"""
from utility import exceptions
from utility import FINANCIAL_DATA_TYPE_MAP
from utility.helpers import get_signature, download_urls, query_data, combine_data_files, order_by_time, get_timestamp, merge_csv_files
from utility import sha1prng
import config

class YKYRequester:
    """用于从悦刻云(Yuekeyun)网站获取数据的请求类。

    Attributes:
        base_url (str): 所有 API 请求的基础 URL。
        keys (dict): 从 .env 文件中读取的密钥字典。
        financial_categories (list): 可用的财政数据类型代码列表（例如 'C01' 到 'C23'）。
    """
    def __init__(self, in_config: config.ConfigManager):
        self.config = in_config
        self.base_url: str = "https://gw.open.yuekeyun.com/openapi/param2/1/alibaba.dme.lark"

    def get_financial_data(self, entries: tuple[str, str, str]):
        """用于获取财政数据。

        数据类型编码与名称对照表：

        Args:
            entries (tuple[str, str, str]):
                financial_category (str): 调用哪一类数据，例如 'C13' 表示会员卡激活数据。
                timespan (str): 数据的时间跨度，例如 'month' 表示返回一个月的数据。
                search_date (str): 查询的日期，格式必须为 'YYYY-MM-DD'，若月份和日期不足两位需补齐。
        
        If an error occurs in the fetching of data. All already fetched files are removed.

        """

        financial_category = entries[0]
        timespan = entries[1]
        search_date = entries[2]

        if financial_category not in FINANCIAL_DATA_TYPE_MAP:
            raise exceptions.InvalidFinancialCategoryException()

        if timespan not in ('month', 'day'):
            raise exceptions.InvalidTimespanException()

        api_name: str = "dme.lark.data.finance.getFinancialData"

        query_parameters = {"leaseCode": f"{self.config.get("LEASE_CODE")}",
                    "cinemaLinkId": f"{self.config.get("CINEMA_LINK_ID")}",
                    "_aop_timestamp": f"{get_timestamp()}",
                    "channelCode": f"{self.config.get("CHANNEL_CODE")}",
                    "dataType": f"{financial_category}",
                    "searchDateType": f"{timespan}",
                    "searchDate": f"{search_date}"}

        aop_signature = get_signature(api_name,
                                    query_parameters,
                                    self.config.get("APP_KEY"),
                                    self.config.get("SECRET_KEY"))

        query_parameters["_aop_signature"] = f"{aop_signature}"

        response = query_data(api_name, query_parameters, self.config.get("APP_KEY"))

        # print(response.json())
        download_url_list = response.json()['data']['bizData']['downloadUrlList']

        decrypter = sha1prng.Decrypter(self.config.get("LEASE_CODE"))
        file_name_stack = download_urls(download_url_list, financial_category, decrypter, search_date)
        output_csv = combine_data_files(file_name_stack, financial_category, search_date, True)

        order_by_time(output_csv, self.config.get_timestamp_column(financial_category))
        
        return output_csv
        

    




