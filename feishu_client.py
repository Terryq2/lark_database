import os
import httpx
from utility import FINANCIAL_DATA_TYPE_MAP
from utility.helpers import read_csv
import config

class FeishuClient:
    def __init__(self, config_in: config.ConfigManager):
        self.config = config_in

    def get_wiki_app_token(self, node_token: str):
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

        print("Uploading: ", path)
        for request_body in request_bodies:
            with httpx.Client() as client:
                response = client.post(url, headers=headers, json={"records": request_body}, timeout=30.0)
                response.raise_for_status()
                print("......")
                # print(response.content)
        print("Done")
        os.remove(path)

    def create_new_table(self, app_token: str, file_name: str, financial_category: str):
        """
        在由app_token指定的位置生成一个名字为file_name的表格。
        表格字段由financial_category决定
        """
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"


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
            return response.json()

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