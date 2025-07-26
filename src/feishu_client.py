import os
import logging
from datetime import datetime, timedelta, date, time
from typing import Optional, Dict, Any, List
import httpx
from tqdm import tqdm



from utility.helpers import read_csv, find_matching_table
import src.config as config


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
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

    def _initialize_request(self, table_name: str, wiki_obj_token: str):
        if wiki_obj_token is None:
                wiki_obj_token = self.get_wiki_obj_token(self.config.get("WIKI_APP_TOKEN"))

        json_data = self.get_wiki_all_table_info(wiki_obj_token)
        table_id = find_matching_table(json_data, table_name)
        return table_id, wiki_obj_token


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
            # logger.info(response.content)
            response.raise_for_status()
            return response

    def get_wiki_obj_token(self, node_token: str):
        """
        获取飞书云文档(wiki)的 obj_token。

        该方法调用飞书开放平台接口 `/wiki/v2/spaces/get_node`，根据传入的 wiki 节点 token（node_token），
        获取对应文档的元数据，并返回可用于后续访问或操作该文档的 obj_token。

        Args:
            node_token (str): 表示 wiki 节点的 token，来自飞书文档空间。

        Returns:
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

        Args:
            path (str): CSV 文件的路径。
            wiki_obj_token (str): wiki对象token。
            table_name (str): 表格名称。
            financial_category (str): 财务数据类别。

        Returns:
            dict: 飞书 API 的响应内容。
        """
        logger.info(f"Starting CSV upload from '{path}' to table '{table_name}'")

        try:
            table_id, wiki_obj_token = self._initialize_request(table_name, wiki_obj_token)

            if table_id is not None:
                logger.info(f"Using existing table with ID: {table_id}")
            else:
                table_id = self.create_new_table(wiki_obj_token, table_name, financial_category)
                logger.info(f"Created new table with ID: {table_id}")

            url = f"{self.BASE_URL}/bitable/v1/apps/{wiki_obj_token}/tables/{table_id}/records/batch_create"
            headers = self._get_headers()
            request_bodies = read_csv(path)

            if not request_bodies:
                logger.warning("No data found in CSV file")
                return

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
                    if response_data['code'] != 0:
                        raise Exception('Something bad happened (Check table naming?)')

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

        Args:
            wiki_obj_token (str): wiki对象token。
            file_name (str): 表格名称。
            financial_category (str): 财务数据类别。

        Returns:
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

            if response_data['code'] != 0:
                raise Exception('Something bad happened (Check table naming?)')
                   
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
        headers = self._get_headers(False)
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

        Args:
            wiki_obj_token (str): wiki对象token。

        Returns:
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

            if table_info['code'] != 0:
                raise Exception('Something bad happened')


            logger.debug(f"Found {len(table_info.get('data', {}).get('items', []))} tables")
            return table_info
 
        except Exception as e:
            logger.error(f"Failed to get table info: {e}")
            raise

    def delete_table(self, table_name: str, wiki_obj_token: Optional[str] = None):
        """
        删除指定的表格。

        Args:
            table_name (str): 表格名称。
            wiki_obj_token (Optional[str]): wiki对象token，如果为None则使用默认值。

        Returns:
            dict: 删除操作的响应数据。

        异常:
            Exception: 当表格不存在或删除失败时抛出。
        """
        logger.info(f"Deleting table '{table_name}'")

        try:
            table_id, wiki_obj_token = self._initialize_request(table_name, wiki_obj_token)

            url = f"{self.BASE_URL}/bitable/v1/apps/{wiki_obj_token}/tables/{table_id}"
            headers = self._get_headers()

            response = self._make_request("DELETE", url, headers=headers)
            result = response.json()

            if response['code'] != 0:
                raise Exception('Something bad happened')
   
            logger.info(f"Successfully deleted table '{table_name}'")
            return result

        except Exception as e:
            logger.error(f"Failed to delete table '{table_name}': {e}")
            raise


    def get_all_column_ids(self,
                           table_name: str, 
                           wiki_obj_token: Optional[str] = None):
        """
        获取指定表格的所有列ID信息。
        
        Args:
            table_name (str): 表格名称。
            wiki_obj_token (Optional[str]): wiki对象token，如果为None则使用默认值。
            
        Returns:
            dict: 包含所有列信息的响应数据。
            
        Raises:
            Exception: 当获取列信息失败时抛出。
        """
        try:
            table_id, wiki_obj_token = self._initialize_request(table_name, wiki_obj_token)

            url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{wiki_obj_token}/tables/{table_id}/fields"


            headers = self._get_headers()

            request_body = {
                "page_size": "100"
            }

            response = self._make_request("POST", url, headers=headers, json_data=request_body)
            result = response.json()

            if response['code'] != 0:
                raise Exception('Something bad happened')

            logger.info(f"Successfully fetched column ids from '{table_name}'")
    
        except Exception as e:
            logger.error(f"Failed to fetch table '{table_name}': {e}")
            raise
    
        return result
    
    def delete_records_by_id(self,
                       table_name: str,
                       ids_to_delete: list[str],
                       wiki_obj_token: Optional[str] = None
                       ):
        """
        根据记录ID批量删除表格中的记录。
        
        该方法将待删除的记录ID列表分块处理，每次最多删除500条记录，
        以符合飞书API的限制要求。
        
        Args:
            table_name (str): 表格名称。
            ids_to_delete (list[str]): 待删除的记录ID列表。
            wiki_obj_token (Optional[str]): wiki对象token，如果为None则使用默认值。
            
        Returns:
            dict: 最后一批删除操作的响应数据。
            
        Raises:
            Exception: 当删除操作失败时抛出。
        """
        try:
            table_id, wiki_obj_token = self._initialize_request(table_name, wiki_obj_token)

            url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{wiki_obj_token}/tables/{table_id}/records/batch_delete"

            headers = self._get_headers()

            MAX_CHUNK_SIZE = 500
            
            already_sent = 0

            while already_sent < len(ids_to_delete):
                if len(ids_to_delete) - already_sent < MAX_CHUNK_SIZE:
                    can_send = len(ids_to_delete) - already_sent
                else:
                    can_send = MAX_CHUNK_SIZE

                request_body = {
                    "records": [ids_to_delete[i] for i in range(already_sent, already_sent + can_send)] # Use max allowed for efficiency
                }
                response = self._make_request("POST", url, headers=headers, json_data=request_body)
                result = response.json()
                if result['code'] != 0:
                    raise Exception('Something bad happened')
                
                already_sent += can_send

            print(result)
            logger.info(f"Successfully deleted records from '{table_name}'")

            return result
        except Exception as e:
            logger.error(f"Failed to delete records from table '{table_name}': {e}")
            raise

    def get_table_records(self, 
                          table_name: str, 
                          page_size: int = 500,
                          page_token: Optional[str] = None, 
                          wiki_obj_token: Optional[str] = None):
        """
        获取表格记录数据，支持分页查询。
        
        Args:
            table_name (str): 表格名称。
            page_size (int): 每页记录数，默认为500（最大值）。
            page_token (Optional[str]): 分页标记，用于获取下一页数据。
            wiki_obj_token (Optional[str]): wiki对象token，如果为None则使用默认值。
            
        Returns:
            dict: 包含记录数据的响应信息，包括items、total、page_token等字段。
            
        Raises:
            Exception: 当获取记录失败时抛出。
        """
        try:
            table_id, wiki_obj_token = self._initialize_request(table_name, wiki_obj_token)
            
            url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{wiki_obj_token}/tables/{table_id}/records/search"
            
            headers = self._get_headers()
            
            # Prepare request body (not URL params)
            request_body = {
                "page_size": page_size # Use max allowed for efficiency
            }
            
            if page_token is not None:
                request_body["page_token"] = page_token
            
            # Pass request_body as JSON data, not params
            response = self._make_request("POST", url, headers=headers, params=request_body, json_data={})
            result = response.json()
            if result['code'] != 0:
                raise Exception('Something bad happened')
            
            logger.info(f"Successfully fetched records from '{table_name}': {len(result.get('data', {}).get('items', []))} records")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to fetch table '{table_name}': {e}")
            raise

    def get_table_records_id_at_date(self, 
                                    table_name: str,
                                    day: str,
                                    time_stamp_column_name: str = None, 
                                    wiki_obj_token: Optional[str] = None) -> list[str]:
        """
        获取指定日期（精确到日）对应的记录ID。

        假设表中的记录按时间升序排序，一旦遇到日期大于目标日期的记录，即可提前终止遍历。

        Args:
            table_name (str): 飞书多维表的名称。
            day (str): 指定的日期，格式为 'YYYY-M-D'（如 '2025-7-14'）。
            time_stamp_column_name (str, optional): 时间戳列名。
            wiki_obj_token (Optional[str], optional): Wiki 表对象 token。

        Returns:
            list[str]: 所有时间戳字段的日期等于 `day` 的记录ID列表。
        """
        try:
            table_id, wiki_obj_token = self._initialize_request(table_name, wiki_obj_token)

            target_date = datetime.strptime(day, "%Y-%m-%d").date()
            list_of_id = []
            already_read = 0

            response = self.get_table_records(table_name, wiki_obj_token=wiki_obj_token)

            total_records = response['data']['total']

            while already_read < total_records:
                for field in response['data']['items']:
                    dt_obj = datetime.strptime(field["fields"][time_stamp_column_name][0]['text'], "%Y-%m-%d %H:%M:%S")
                    record_date = dt_obj.date()

                    if record_date > target_date:
                        # Early termination — all subsequent records will be after target_date
                        logger.info(f"Early termination at {record_date}, no more records for {target_date}")
                        return list_of_id
                    
                    if record_date == target_date:
                        list_of_id.append(field['record_id'])

                already_read += len(response['data']['items'])

                if already_read >= total_records or 'page_token' not in response['data']:
                    break

                response = self.get_table_records(
                    table_name,
                    page_token=response['data']['page_token'],
                    wiki_obj_token=wiki_obj_token
                )
                
            logger.info(f"Successfully fetched {len(list_of_id)} record(s) from '{table_name}' for date {target_date}")
            return list_of_id

        except Exception as e:
            logger.error(f"Failed to fetch table '{table_name}': {e}")
            raise

    
    def get_table_records_id_at_head_date(
        self,
        table_name: str,
        time_stamp_column_name: str = None,
        wiki_obj_token: Optional[str] = None
    ) -> list[str]:
        """
        获取飞书表格中最早日期(表头日期)对应的所有记录ID。

        假设表格记录按时间戳升序排序(最早的在最前),一旦出现比首个记录日期大的记录,即可停止遍历。

        Args:
            table_name (str): 飞书多维表的名称。
            time_stamp_column_name (str, optional): 时间戳列的名称。
            wiki_obj_token (Optional[str], optional): Wiki 表对象 token。

        Returns:
            list[str]: 所有记录时间戳等于表头最早日期的记录ID列表。
        """
        try:
            table_id, wiki_obj_token = self._initialize_request(table_name, wiki_obj_token)
            list_of_id = []

            response = self.get_table_records(table_name, wiki_obj_token=wiki_obj_token)
            
            # Handle empty response
            if not response.get('data', {}).get('items'):
                logger.info(f"No records found in table '{table_name}'")
                return []

            items = response['data']['items']
            
            # Get the first record's date to establish the head date
            first_record = items[0]
            timestamp_field = first_record.get("fields", {}).get(time_stamp_column_name)
                
            try:
                first_date = datetime.strptime(timestamp_field[0]['text'], "%Y-%m-%d %H:%M:%S").date()
            except ValueError as ve:
                logger.error(f"Invalid datetime format in first record: {ve}")
                return []

            logger.info(f"Head date determined as: {first_date}")
            
            # Process first page
            for field in items:
                timestamp_field = field['fields'][time_stamp_column_name]
                if not timestamp_field or not timestamp_field[0].get('text'):
                    logger.warning(f"Missing timestamp field in record {field.get('record_id', 'unknown')}")
                    continue
                    
                try:
                    dt_obj = datetime.strptime(timestamp_field[0]['text'], "%Y-%m-%d %H:%M:%S")
                    record_date = dt_obj.date()
                except ValueError as ve:
                    logger.warning(f"Invalid datetime format in record {field.get('record_id', 'unknown')}: {ve}")
                    continue

                # If we encounter a date different from the first date, stop processing
                if record_date != first_date:
                    logger.info(f"Encountered different date {record_date}, stopping pagination")
                    return list_of_id

                list_of_id.append(field['record_id'])

            # Continue with subsequent pages if needed
            already_read = len(items)
            total_records = response['data']['total']
            
            while already_read < total_records:
                page_token = response['data']['page_token']
            
                response = self.get_table_records(
                    table_name, 
                    page_token=page_token, 
                    wiki_obj_token=wiki_obj_token
                )
                    
                items = response['data']['items']
                already_read += len(items)
                
                for field in items:
                    timestamp_field = field['fields'][time_stamp_column_name]
                    if not timestamp_field or not timestamp_field[0].get('text'):
                        logger.warning(f"Missing timestamp field in record {field.get('record_id', 'unknown')}")
                        continue
                        
                    try:
                        dt_obj = datetime.strptime(timestamp_field[0]['text'], "%Y-%m-%d %H:%M:%S")
                        record_date = dt_obj.date()
                    except ValueError as ve:
                        logger.warning(f"Invalid datetime format in record {field.get('record_id', 'unknown')}: {ve}")
                        continue

                    if record_date != first_date:
                        logger.info(f"Encountered different date {record_date}, stopping pagination")
                        return list_of_id

                    list_of_id.append(field['record_id'])

            logger.info(f"Fetched {len(list_of_id)} record(s) at head date {first_date}")
            return list_of_id

        except Exception as e:
            logger.error(f"Failed to fetch head-date records from '{table_name}': {e}")
            raise
