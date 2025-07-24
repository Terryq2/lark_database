from datetime import datetime, timedelta
from config import FinancialQueries

from config import ConfigManager
from fetcher import YKYRequester
from feishu_client import FeishuClient
from utility.helpers import merge_csv_files, FINANCIAL_DATA_TYPE_MAP

class DataSyncClient:
    """驱动Yuekeyun API和飞书客户端的数据获取与上传流程。

    该类负责协调配置管理、Yuekeyun API数据请求和飞书数据上传，处理财务数据的获取和同步。

    属性:
        config (ConfigManager): 配置管理器，存储API相关配置。
        requester (YKYRequester): Yuekeyun API请求客户端。
        uploader (FeishuClient): 飞书数据上传客户端。
    """

    def __init__(self, env_file_path: str, config_file_path: str):
        """初始化DataSyncClient，设置配置管理器和客户端。

        参数:
            env_file_path (str): 环境文件路径（例如，'.env'）。
            config_file_path (str): 数据schema配置文件路径（例如，'schemas.json'）。

        示例:
            >>> client = DataSyncClient('.env', 'schemas.json')
            >>> client.config.get('BASE_URL')
            'https://gw.open.yuekeyun.com/openapi/param2/1/alibaba.dme.lark'
        """
        self.config = ConfigManager(env_file_path, config_file_path)
        self.cinema_client = YKYRequester(self.config)
        self.lark_client = FeishuClient(self.config)

    def download_financial_data(self, queries: FinancialQueries) -> None:
        """从Yuekeyun API获取财务数据。不上传数据。

        参数:
            entries (List[Tuple[str, str, str]]): 财务数据查询条目列表，每个条目包含
                (数据类型, 时间范围, 查询日期)。

        示例:
            >>> client = DataSyncClient('.env', 'schemas.json')
            >>> entries = [('C01', 'day', '2023-01-01'), ('C02', 'month', '2023-01')]
            >>> client.get_financial_data(entries)
        """
        for query in queries.to_tuple():
            self.cinema_client.get_financial_data(query)

    def sync_financial_data(self, query: FinancialQueries, table_name: str, wiki_obj_token: str = None) -> None:
        """从Yuekeyun API获取财务数据并上传至飞书。

        参数:
            entries (List[Tuple[str, str, str]]): 财务数据查询条目列表，每个条目包含
                (数据类型, 时间范围, 查询日期)。
            app_token (str): 飞书应用的令牌。
            table_id (str): 飞书目标数据表的ID。

        返回:
            None: 该方法不返回任何值，但会将数据上传至飞书。

        示例:
            >>> client = DataSyncClient('.env', 'schemas.json')
            >>> entries = [('C01', 'day', '2023-01-01')]
            >>> client.get_and_upload_financial_data(entries, 'app_token_123', 'table_id_456')
        """
        if wiki_obj_token is None:
            wiki_obj_token = self.lark_client.get_wiki_obj_token(self.config.get("WIKI_APP_TOKEN"))
        queries = query.to_tuple()

        output_csv = []
        for entry in queries:
            output_csv.append(self.cinema_client.get_financial_data(entry))

        output_path = f"{FINANCIAL_DATA_TYPE_MAP[query.category]}/{FINANCIAL_DATA_TYPE_MAP[query.category]}.csv"
        
        df = merge_csv_files(f"{FINANCIAL_DATA_TYPE_MAP[query.category]}")
        df.write_csv(output_path)
        
        self.lark_client.post_csv_data_to_feishu(output_path, wiki_obj_token, table_name, entry[0])

    def sync_current_year_data(self, financial_category: str, table_name: str, wiki_obj_token: str = None) -> None:

        if wiki_obj_token is None:
            wiki_obj_token = self.lark_client.get_wiki_obj_token(self.config.get("WIKI_APP_TOKEN"))
        current_time = datetime.now()

        queries = FinancialQueries(financial_category, "month", f"{current_time.year}-{1:02d}")
        for month in range(2, current_time.month):
            queries.add_new_query("month", f"{current_time.year}-{month:02d}")
        for day in range(1, current_time.day):
            queries.add_new_query("day", f"{current_time.year}-{current_time.month:02d}-{day:02d}")  
        self.sync_financial_data(queries, table_name, wiki_obj_token)

    def sync_most_recent_data(self, financial_category: str, table_name: str, looking_back: int = 14, wiki_obj_token: str = None) -> None:
        if wiki_obj_token is None:
            wiki_obj_token = self.lark_client.get_wiki_obj_token(self.config.get("WIKI_APP_TOKEN"))
            
        current_time = datetime.now()
        queries = FinancialQueries(financial_category, "day", (current_time - timedelta(days=looking_back)).strftime("%Y-%m-%d"))
        for i in range(looking_back, 0, -1):
            date = (current_time - timedelta(days=i)).strftime("%Y-%m-%d")
            queries.add_new_query('day', date)

        self.sync_financial_data(queries, table_name, wiki_obj_token)