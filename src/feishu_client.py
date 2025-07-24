import os
import logging
from typing import Optional, Dict, Any, List
import httpx
from tqdm import tqdm

from utility.helpers import read_csv, find_matching_table
import src.config as config

logger = logging.getLogger(__name__)


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

    BASE_URL = "https://open.feishu.cn/open-apis"
    DEFAULT_TIMEOUT = 30.0

    def __init__(self, config_in: config.ConfigManager):
        """初始化飞书客户端"""
        self.config = config_in
        logger.info("FeishuClient initialized")

    def _get_headers(self, require_auth: bool = True) -> Dict[str, str]:
        """获取标准请求头"""
        headers = {"Content-Type": "application/json"}
        if require_auth:
            headers["Authorization"] = f"Bearer {self.get_tenant_access_token_from_feishu()}"
        return headers

    def _make_request(
        self, 
        method: str, 
        url: str, 
        headers: Optional[Dict[str, str]] = None,
        json_data: Optional[Dict] = None,
        params: Optional[Dict] = None,
        timeout: float = DEFAULT_TIMEOUT
    ) -> httpx.Response:
        """统一的HTTP请求方法"""
        with httpx.Client() as client:
            response = client.request(
                method=method,
                url=url,
                headers=headers,
                json=json_data,
                params=params,
                timeout=timeout
            )
            response.raise_for_status()
            return response

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
        logger.info(f"Fetching wiki obj_token for node: {node_token}")
        
        url = f"{self.BASE_URL}/wiki/v2/spaces/get_node"
        headers = self._get_headers()
        params = {
            "token": node_token,
            "obj_type": "wiki"
        }

        try:
            response = self._make_request("GET", url, headers=headers, params=params)
            obj_token = response.json()['data']['node']['obj_token']
            logger.info(f"Successfully obtained obj_token: {obj_token}")
            return obj_token
        except KeyError as e:
            logger.error(f"Invalid response format: missing {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to get wiki obj_token: {e}")
            raise

    def post_csv_data_to_feishu(self, path: str, wiki_obj_token: str, table_name: str, financial_category: str):
        """
        读取 CSV 文件并将数据以 POST 请求发送到飞书多维表格。

        参数:
            path (str): CSV 文件的路径。
            wiki_obj_token (str): wiki对象token。
            table_name (str): 表格名称。
            financial_category (str): 财务数据类别。

        返回:
            dict: 飞书 API 的响应内容。
        """
        logger.info(f"Starting CSV upload from '{path}' to table '{table_name}'")

        try:
            # 获取或创建表格
            table_infos = self.get_wiki_all_table_info(wiki_obj_token)
            found_table_id = find_matching_table(table_infos, table_name)

            if found_table_id is not None:
                table_id = found_table_id
                logger.info(f"Using existing table with ID: {table_id}")
            else:
                table_id = self.create_new_table(wiki_obj_token, table_name, financial_category)
                logger.info(f"Created new table with ID: {table_id}")

            # 准备上传
            url = f"{self.BASE_URL}/bitable/v1/apps/{wiki_obj_token}/tables/{table_id}/records/batch_create"
            headers = self._get_headers()
            request_bodies = read_csv(path)

            if not request_bodies:
                logger.warning("No data found in CSV file")
                return

            # 上传数据
            print("Uploading: ", path)
            for i, request_body in enumerate(tqdm(request_bodies, ncols=70, unit='chunk')):
                try:
                    response = self._make_request(
                        "POST", 
                        url, 
                        headers=headers, 
                        json_data={"records": request_body}
                    )
                    
                    response_data = response.json()
                    if response_data.get('msg') != 'success':
                        print(response.content)
                        raise httpx.HTTPError(f"Post failed")

                    print("......")
                    logger.debug(f"Successfully uploaded chunk {i+1}/{len(request_bodies)}")
                    
                except Exception as e:
                    logger.error(f"Failed to upload chunk {i+1}: {e}")
                    raise

            print("Done")
            logger.info(f"Successfully uploaded all data from '{path}'")
            
        except Exception as e:
            logger.error(f"CSV upload failed: {e}")
            raise
        finally:
            # 清理文件
            try:
                if os.path.exists(path):
                    os.remove(path)
                    logger.info(f"Cleaned up CSV file: {path}")
            except OSError as cleanup_error:
                logger.warning(f"Failed to remove CSV file '{path}': {cleanup_error}")

    def create_new_table(self, wiki_obj_token: str, file_name: str, financial_category: str):
        """
        在由app_token指定的位置生成一个名字为file_name的表格。
        表格字段由financial_category决定

        参数:
            wiki_obj_token (str): wiki对象token。
            file_name (str): 表格名称。
            financial_category (str): 财务数据类别。

        返回:
            str: 创建的表格ID。

        异常:
            Exception: 当表格创建失败时抛出。
        """
        logger.info(f"Creating new table '{file_name}' for category '{financial_category}'")
        
        url = f"{self.BASE_URL}/bitable/v1/apps/{wiki_obj_token}/tables"
        headers = self._get_headers()

        def format_headers(header_list: List[str]) -> List[Dict[str, Any]]:
            """格式化表格字段"""
            return [{"field_name": header, "type": 1} for header in header_list]

        try:
            column_names = self.config.get_columns(financial_category)
            request_body = {
                "table": {
                    "name": file_name,
                    "fields": format_headers(column_names)
                }
            }
            
            response = self._make_request("POST", url, headers=headers, json_data=request_body)
            response_data = response.json()
            
            if response_data.get('msg') != 'success':
                logger.error(f"Table creation failed: {response_data.get('msg', 'Unknown error')}")
                raise httpx.HTTPError("Request failed: Check table naming?")           
            table_id = response_data['data']['table_id']
            logger.info(f"Successfully created table '{file_name}' with ID: {table_id}")
            return table_id
            
        except Exception as e:
            logger.error(f"Failed to create table '{file_name}': {e}")
            raise

    def get_tenant_access_token_from_feishu(self):
        """
        获取飞书租户访问令牌。

        此方法通过向飞书开放平台的认证接口发送POST请求，获取租户访问令牌（tenant_access_token）。
        该令牌用于后续调用飞书开放平台的相关API。

        Args:
            无(使用self.config中的FEISHU_APP_KEY和FEISHU_APP_SECRET)。

        Returns:
            str: 飞书租户访问令牌(tenant_access_token)。

        Raises:
            httpx.HTTPStatusError: 如果请求失败或返回状态码非200。
            KeyError: 如果响应中缺少'tenant_access_token'字段。
            JSONDecodeError: 如果响应内容无法解析为JSON。

        Example:
            >>> client = FeishuClient(config_manager)
            >>> token = client.get_tenant_access_token_from_feishu()
            >>> print(token)
            't-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'

        Note:
            - 确保self.config中已正确配置'FEISHU_APP_KEY'和'FEISHU_APP_SECRET'。
            - 该方法使用httpx.Client进行HTTP请求, 确保网络连接正常。
            - 返回的令牌有有效期，需根据飞书文档处理令牌过期情况。
        """
        logger.debug("Fetching tenant access token")
        
        url = f"{self.BASE_URL}/auth/v3/tenant_access_token/internal"
        headers = {"Content-Type": "application/json"}
        request_body = {
            "app_id": self.config.get("FEISHU_APP_KEY"),
            "app_secret": self.config.get("FEISHU_APP_SECRET")
        }

        try:
            response = self._make_request("POST", url, headers=headers, json_data=request_body)
            token = response.json()['tenant_access_token']
            logger.debug("Successfully obtained tenant access token")
            return token
            
        except KeyError as e:
            logger.error(f"Invalid token response format: missing {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to get tenant access token: {e}")
            raise

    def get_wiki_all_table_info(self, wiki_obj_token: str):
        """
        获取wiki中所有表格信息。

        参数:
            wiki_obj_token (str): wiki对象token。

        返回:
            dict: 表格信息字典。

        异常:
            httpx.HTTPStatusError: 当API请求失败时抛出。
        """
        logger.debug(f"Fetching table info for wiki: {wiki_obj_token}")
        
        url = f"{self.BASE_URL}/bitable/v1/apps/{wiki_obj_token}/tables"
        headers = self._get_headers()

        try:
            response = self._make_request("GET", url, headers=headers, params={})
            table_info = response.json()
            logger.debug(f"Found {len(table_info.get('data', {}).get('items', []))} tables")
            return table_info
            
        except Exception as e:
            logger.error(f"Failed to get table info: {e}")
            raise

    def delete_table(self, table_name: str, wiki_obj_token: Optional[str] = None):
        """
        删除指定的表格。

        参数:
            table_name (str): 表格名称。
            wiki_obj_token (Optional[str]): wiki对象token，如果为None则使用默认值。

        返回:
            dict: 删除操作的响应数据。

        异常:
            Exception: 当表格不存在或删除失败时抛出。
        """
        logger.info(f"Deleting table '{table_name}'")
        
        try:
            if wiki_obj_token is None:
                wiki_obj_token = self.get_wiki_obj_token(self.config.get("WIKI_APP_TOKEN"))

            # 查找表格
            json_data = self.get_wiki_all_table_info(wiki_obj_token)
            table_id = find_matching_table(json_data, table_name)
            
            if table_id is None:
                logger.error(f"Table '{table_name}' not found")
                raise Exception("Non existent table")

            # 删除表格
            url = f"{self.BASE_URL}/bitable/v1/apps/{wiki_obj_token}/tables/{table_id}"
            headers = self._get_headers()

            response = self._make_request("DELETE", url, headers=headers)
            result = response.json()
            
            logger.info(f"Successfully deleted table '{table_name}'")
            return result
            
        except Exception as e:
            logger.error(f"Failed to delete table '{table_name}': {e}")
            raise