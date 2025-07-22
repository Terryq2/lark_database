"""This is a class that helps fetching data from the yuekeyun website"""
import os
import time
import dotenv

import httpx
from utility import exceptions
from utility import FINANCIAL_DATA_TYPE_MAP
from utility.helpers import get_signature, download_urls, query_data, combine_data_files, read_csv, order_by_time
from utility import sha1prng

class YKYRequester:
    """用于从悦刻云(Yuekeyun)网站获取数据的请求类。

    Attributes:
        base_url (str): 所有 API 请求的基础 URL。
        keys (dict): 从 .env 文件中读取的密钥字典。
        financial_categories (list): 可用的财政数据类型代码列表（例如 'C01' 到 'C23'）。
    """
    def __init__(self):
        self.keys: dict[str, str] = dotenv.dotenv_values()
        self.base_url: str = "https://gw.open.yuekeyun.com/openapi/param2/1/alibaba.dme.lark"


    def get_timestamp(self) -> int:
        """返回当前毫秒级 Unix 时间戳。
        Returns:
            int: 当前的 Unix 时间戳（毫秒级）。
        """
        return int(time.time() * 1000)

    def get_financial_data(self, entries: list[tuple[str, str, str]]):
        """用于获取财政数据。

        数据类型编码与名称对照表：

        Args:
            entries (list[tuple[str, str, str]]):
                financial_category (str): 调用哪一类数据，例如 'C13' 表示会员卡激活数据。
                timespan (str): 数据的时间跨度，例如 'month' 表示返回一个月的数据。
                search_date (str): 查询的日期，格式必须为 'YYYY-MM-DD'，若月份和日期不足两位需补齐。
        
        If an error occurs in the fetching of data. All already fetched files are removed.

        """
        for financial_category, timespan, search_date in entries:
            if financial_category not in FINANCIAL_DATA_TYPE_MAP:
                raise exceptions.InvalidFinancialCategoryException()

            if timespan not in ('month', 'day'):
                raise exceptions.InvalidTimespanException()

            api_name: str = "dme.lark.data.finance.getFinancialData"

            query_parameters = {"leaseCode": f"{self.keys["LEASE_CODE"]}",
                        "cinemaLinkId": f"{self.keys["CINEMA_LINK_ID"]}",
                        "_aop_timestamp": f"{self.get_timestamp()}",
                        "channelCode": f"{self.keys["CHANNEL_CODE"]}",
                        "dataType": f"{financial_category}",
                        "searchDateType": f"{timespan}",
                        "searchDate": f"{search_date}"}

            aop_signature = get_signature(api_name, 
                                        query_parameters, 
                                        self.keys["APP_KEY"], 
                                        self.keys["SECRET_KEY"])

            query_parameters["_aop_signature"] = f"{aop_signature}"

            response = query_data(api_name, query_parameters, self.keys["APP_KEY"])

            print(response.json())
            download_url_list = response.json()['data']['bizData']['downloadUrlList']

            decrypter = sha1prng.Decrypter(self.keys["LEASE_CODE"])
            file_name_stack = download_urls(download_url_list, financial_category, decrypter)
            output_csv = combine_data_files(file_name_stack, financial_category, False)
            order_by_time(output_csv)


    def get_tenant_access_token_from_feishu(self):
        """
        获取飞书租户访问令牌。

        此方法通过向飞书开放平台的认证接口发送POST请求，获取租户访问令牌（tenant_access_token）。
        该令牌用于后续调用飞书开放平台的相关API。

        Args:
            无(使用self.keys中的FEISHU_APP_KEY和FEISHU_APP_SECRET)。

        Returns:
            str: 飞书租户访问令牌(tenant_access_token)。

        Raises:
            httpx.HTTPStatusError: 如果请求失败或返回状态码非200。
            KeyError: 如果响应中缺少'tenant_access_token'字段。
            JSONDecodeError: 如果响应内容无法解析为JSON。

        Example:
            >>> instance = SomeClass()
            >>> token = instance.get_tenant_access_token_from_feishu()
            >>> print(token)
            't-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'

        Note:
            - 确保self.keys中已正确配置'FEISHU_APP_KEY'和'FEISHU_APP_SECRET'。
            - 该方法使用httpx.Client进行HTTP请求, 确保网络连接正常。
            - 返回的令牌有有效期，需根据飞书文档处理令牌过期情况。
        """

        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        headers = {
            "Content-Type": "application/json"
        }

        request_body = {
            "app_id": self.keys["FEISHU_APP_KEY"],
            "app_secret": self.keys["FEISHU_APP_SECRET"]
        }


        with httpx.Client() as client:
            response = client.post(url, headers=headers, json=request_body)
            print(response.json())
            return response.json()['tenant_access_token']

    def get_cloud_file_token(self, node_token: str):
        """
        获取飞书云文档(wiki)的 obj_token。

        该方法调用飞书开放平台接口 `/wiki/v2/spaces/get_node`，根据传入的 wiki 节点 token（node_token），
        获取对应文档的元数据，并返回可用于后续访问或操作该文档的 obj_token。

        参数:
            node_token (str): 表示 wiki 节点的 token，来自飞书文档空间。

        返回:
            str: 与该节点关联的 obj_token。

        异常:
            httpx.HTTPStatusError: 当 API 请求失败或返回非 200 状态码时抛出。
            KeyError: 当返回数据中缺少预期字段（如 'data' 或 'node'）时抛出。
        """
        # https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node

        url = "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node"
        headers = {
            "Authorization": f"Bearer {self.get_tenant_access_token_from_feishu()}",
            "Content-Type": "application/json"
        }

        request_body = {
            "token": f"{node_token}",
            "obj_type": "wiki"
        }

        with httpx.Client() as client:
            response = client.get(url, headers=headers, params=request_body)
            print(response.content)
            return response.json()['data']['node']['obj_token']

    def post_csv_data_to_feishu(self, path: str, app_token: str, table_id: str):
        """
        读取 CSV 文件并将数据以 POST 请求发送到飞书多维表格。

        参数:
            path (str): CSV 文件的路径。

        返回:
            dict: 飞书 API 的响应内容。
        """
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"

        headers = {
            "Authorization": f"Bearer {self.get_tenant_access_token_from_feishu()}",
            "Content-Type": "application/json"
        }

        request_bodies = read_csv(path)
        
        for request_body in request_bodies:
            with httpx.Client() as client:
                response = client.post(url, headers=headers, json={"records": request_body})
                print(response.content)
                
        os.remove(path)
        return
        
        
        
            
