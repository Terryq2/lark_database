import os
import httpx
from utility import FINANCIAL_DATA_TYPE_MAP
from utility.helpers import read_csv, find_matching_table
import config
from tqdm import tqdm

class FeishuClient:
    """飞书客户端，用于与飞书开放平台API交互。

    该类封装了与飞书开放平台API的交互功能，包括获取租户访问令牌、操作云文档（wiki）、
    以及管理多维表格（如创建表格和上传CSV数据）。通过配置管理器初始化，依赖有效的飞书
    应用凭证（如app_id和app_secret）。

    Args:
        config_in (config.ConfigManager): 包含飞书API凭证和配置的配置管理器实例。

    Attributes:
        config (config.ConfigManager): 存储传入的配置管理器，用于访问飞书API密钥和其他配置。

    Raises:
        KeyError: 如果配置管理器中缺少必要的飞书API凭证（如FEISHU_APP_KEY或FEISHU_APP_SECRET）。
    """
    def __init__(self, config_in: config.ConfigManager):
        self.config = config_in

    def get_wiki_obj_token(self, node_token: str):
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
            response.raise_for_status()
            return response.json()['data']['node']['obj_token']
    

    def post_csv_data_to_feishu(self, path: str, wiki_obj_token: str, table_name: str, financial_category: str):
        """
        读取 CSV 文件并将数据以 POST 请求发送到飞书多维表格。

        参数:
            path (str): CSV 文件的路径。

        返回:
            dict: 飞书 API 的响应内容。
        """

        table_infos = self.get_wiki_all_table_info(wiki_obj_token)

        found_table_id = find_matching_table(table_infos, table_name)

        if found_table_id is not None:
            table_id = found_table_id
        else:
            table_id = self.create_new_table(wiki_obj_token, table_name, financial_category)

        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{wiki_obj_token}/tables/{table_id}/records/batch_create"

        headers = {
            "Authorization": f"Bearer {self.get_tenant_access_token_from_feishu()}",
            "Content-Type": "application/json"
        }

        request_bodies = read_csv(path)

        print("Uploading: ", path)
        for request_body in tqdm(request_bodies, ncols=50, unit='chunk'):
            with httpx.Client() as client:
                response = client.post(url, headers=headers, json={"records": request_body}, timeout=30.0)
                response.raise_for_status()

                if response.json().get('msg') != 'success':
                    print(response.content)
                    raise Exception(f"Post failed")


                print("......")
                # print(response.content)
        print("Done")
        os.remove(path)

    def create_new_table(self, wiki_obj_token: str, file_name: str, financial_category: str):
        """
        在由app_token指定的位置生成一个名字为file_name的表格。
        表格字段由financial_category决定
        """
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{wiki_obj_token}/tables"


        headers = {
            "Authorization": f"Bearer {self.get_tenant_access_token_from_feishu()}",
            "Content-Type": "application/json"
        }

        def format_headers(headers: list[str]):
            return [{"field_name": header, "type": 1} for header in headers]
        
        column_names = self.config.get_columns(financial_category)
        request_body = {
            "table": {
                "name": file_name,
                "fields": format_headers(column_names)
            }
        }
        with httpx.Client() as client:
            response = client.post(url, headers=headers, json=request_body)
            response.raise_for_status()
            if response.json().get('msg') != 'success':
                raise Exception(f"Request failed: Check table naming?")
            return response.json()['data']['table_id']

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
            "app_id": self.config.get("FEISHU_APP_KEY"),
            "app_secret": self.config.get("FEISHU_APP_SECRET")
        }

        with httpx.Client() as client:
            response = client.post(url, headers=headers, json=request_body)
            response.raise_for_status()
            return response.json()['tenant_access_token']
    
    def get_wiki_all_table_info(self, wiki_obj_token: str):
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{wiki_obj_token}/tables"
        headers = {
            "Authorization": f"Bearer {self.get_tenant_access_token_from_feishu()}",
            "Content-Type": "application/json"
        }

        request_body = {}

        with httpx.Client() as client:
            response = client.get(url, headers=headers, params=request_body)
            response.json()
            return response.json()
    
    def delete_table(self, table_name: str, wiki_obj_token: str = None):
        if wiki_obj_token is None:
            wiki_obj_token = self.get_wiki_obj_token(self.config.get("WIKI_APP_TOKEN"))


        json_data = self.get_wiki_all_table_info(wiki_obj_token)
        table_id = find_matching_table(json_data, table_name)
        if table_id is None:
            raise Exception("Non existent table")
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{wiki_obj_token}/tables/{table_id}"


        headers = {
            "Authorization": f"Bearer {self.get_tenant_access_token_from_feishu()}",
            "Content-Type": "application/json"
        }


        with httpx.Client() as client:
            response = client.delete(url, headers=headers)
            response.json()
            return response.json()