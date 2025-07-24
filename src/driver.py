from datetime import datetime, timedelta
from typing import Optional
import logging
from pathlib import Path

from src.config import FinancialQueries, ConfigManager
from src.cinema_client import YKYRequester
from src.feishu_client import FeishuClient
from utility.helpers import merge_csv_files, FINANCIAL_DATA_TYPE_MAP

logger = logging.getLogger(__name__)


class DataSyncClient:
    """驱动Yuekeyun API和飞书客户端的数据获取与上传流程。

    该类负责协调配置管理、Yuekeyun API数据请求和飞书数据上传，处理财务数据的获取和同步。

    属性:
        config (ConfigManager): 配置管理器，存储API相关配置。
        cinema_client (YKYRequester): Yuekeyun API请求客户端。
        lark_client (FeishuClient): 飞书数据上传客户端。
    """

    def __init__(self, env_file_path: str, config_file_path: str):
        """初始化DataSyncClient，设置配置管理器和客户端。

        参数:
            env_file_path (str): 环境文件路径（例如，'.env'）。
            config_file_path (str): 数据schema配置文件路径（例如，'schemas.json'）。

        Raises:
            FileNotFoundError: 当配置文件不存在时抛出。
            ValueError: 当配置文件格式无效时抛出。

        示例:
            >>> client = DataSyncClient('.env', 'schemas.json')
            >>> client.config.get('BASE_URL')
            'https://gw.open.yuekeyun.com/openapi/param2/1/alibaba.dme.lark'
        """
        try:
            self.config = ConfigManager(env_file_path, config_file_path)
            self.cinema_client = YKYRequester(self.config)
            self.lark_client = FeishuClient(self.config)
            logger.info("DataSyncClient initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize DataSyncClient: {e}")
            raise

    def download_financial_data(self, queries: FinancialQueries) -> None:
        """从Yuekeyun API获取财务数据。不上传数据。

        参数:
            queries (FinancialQueries): 财务数据查询对象。

        Raises:
            ValueError: 当queries为空时抛出。
            APIError: 当API请求失败时抛出。

        示例:
            >>> client = DataSyncClient('.env', 'schemas.json')
            >>> queries = FinancialQueries('C01', 'day', '2023-01-01')
            >>> client.download_financial_data(queries)
        """
        if not queries or not queries.to_tuple():
            raise ValueError("Queries cannot be empty")
            
        logger.info(f"Starting download of {len(queries.to_tuple())} financial data queries")
        
        try:
            for query in queries.to_tuple():
                self.cinema_client.get_financial_data(query)
            logger.info("Financial data download completed successfully")
        except Exception as e:
            logger.error(f"Failed to download financial data: {e}")
            raise

    def sync_financial_data(
        self, 
        queries: FinancialQueries, 
        table_name: str, 
        wiki_obj_token: Optional[str] = None
    ) -> None:
        """从Yuekeyun API获取财务数据并上传至飞书。

        参数:
            queries (FinancialQueries): 财务数据查询对象。
            table_name (str): 飞书目标数据表名称。
            wiki_obj_token (Optional[str]): 飞书wiki对象令牌，如果为None则自动获取。

        Raises:
            ValueError: 当参数无效时抛出。
            APIError: 当API请求失败时抛出。
            FileNotFoundError: 当生成的CSV文件不存在时抛出。

        示例:
            >>> client = DataSyncClient('.env', 'schemas.json')
            >>> queries = FinancialQueries('C01', 'day', '2023-01-01')
            >>> client.sync_financial_data(queries, 'financial_table')
        """
        if not queries or not queries.to_tuple():
            raise ValueError("Queries cannot be empty")
        if not table_name.strip():
            raise ValueError("Table name cannot be empty")

        logger.info(f"Starting sync of financial data to table '{table_name}'")

        try:
            # Get wiki token if not provided
            if wiki_obj_token is None:
                wiki_obj_token = self.lark_client.get_wiki_obj_token(
                    self.config.get("WIKI_APP_TOKEN")
                )

            # Download data
            query_tuples = queries.to_tuple()
            output_csv_paths = []
            
            for query in query_tuples:
                csv_path = self.cinema_client.get_financial_data(query)
                output_csv_paths.append(csv_path)

            # Merge and save data
            category_folder = FINANCIAL_DATA_TYPE_MAP[queries.category]
            output_path = Path(category_folder) / f"{category_folder}.csv"
            
            # Ensure directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            df = merge_csv_files(category_folder)
            df.write_csv(str(output_path))

            # Upload to Feishu
            # Note: Using first query's category for upload
            first_query_category = query_tuples[0][0] if query_tuples else queries.category
            self.lark_client.post_csv_data_to_feishu(
                str(output_path),
                wiki_obj_token,
                table_name,
                first_query_category
            )
            
            logger.info(f"Successfully synced financial data to table '{table_name}'")
            
        except Exception as e:
            logger.error(f"Failed to sync financial data: {e}")
            raise

    def sync_current_year_data(
        self,
        financial_category: str,
        table_name: str,
        wiki_obj_token: Optional[str] = None
    ) -> None:
        """同步当前年度的财务数据。

        参数:
            financial_category (str): 财务数据类别。
            table_name (str): 飞书目标数据表名称。
            wiki_obj_token (Optional[str]): 飞书wiki对象令牌，如果为None则自动获取。

        Raises:
            ValueError: 当参数无效时抛出。
        """
        if not financial_category.strip():
            raise ValueError("Financial category cannot be empty")
        if not table_name.strip():
            raise ValueError("Table name cannot be empty")

        logger.info(f"Starting sync of current year data for category '{financial_category}'")

        try:
            if wiki_obj_token is None:
                wiki_obj_token = self.lark_client.get_wiki_obj_token(
                    self.config.get("WIKI_APP_TOKEN")
                )

            current_time = datetime.now()
            
            # Create queries for all months from January to current month (exclusive)
            queries = FinancialQueries(
                financial_category, 
                "month", 
                f"{current_time.year}-01"  # Start from January
            )
            
            # Add remaining months
            for month in range(2, current_time.month):
                queries.add_new_query("month", f"{current_time.year}-{month:02d}")
            
            # Add days of current month
            for day in range(1, current_time.day):
                date_str = f"{current_time.year}-{current_time.month:02d}-{day:02d}"
                queries.add_new_query("day", date_str)

            self.sync_financial_data(queries, table_name, wiki_obj_token)
            
        except Exception as e:
            logger.error(f"Failed to sync current year data: {e}")
            raise

    def sync_most_recent_data(
        self,
        financial_category: str,
        table_name: str,
        looking_back: int = 14,
        wiki_obj_token: Optional[str] = None
    ) -> None:
        """同步最近指定天数的财务数据。

        参数:
            financial_category (str): 财务数据类别。
            table_name (str): 飞书目标数据表名称。
            looking_back (int): 回溯天数，默认为14天。
            wiki_obj_token (Optional[str]): 飞书wiki对象令牌，如果为None则自动获取。

        Raises:
            ValueError: 当参数无效时抛出。
        """
        if not financial_category.strip():
            raise ValueError("Financial category cannot be empty")
        if not table_name.strip():
            raise ValueError("Table name cannot be empty")
        if looking_back <= 0:
            raise ValueError("Looking back days must be positive")

        logger.info(f"Starting sync of most recent {looking_back} days for category '{financial_category}'")

        try:
            if wiki_obj_token is None:
                wiki_obj_token = self.lark_client.get_wiki_obj_token(
                    self.config.get("WIKI_APP_TOKEN")
                )

            current_time = datetime.now()
            start_date = current_time - timedelta(days=looking_back)
            
            queries = FinancialQueries(
                financial_category, 
                "day", 
                start_date.strftime("%Y-%m-%d")
            )

            # Add remaining days (note: corrected the range logic)
            for i in range(looking_back - 1, 0, -1):
                date = (current_time - timedelta(days=i)).strftime("%Y-%m-%d")
                queries.add_new_query('day', date)

            self.sync_financial_data(queries, table_name, wiki_obj_token)
            
        except Exception as e:
            logger.error(f"Failed to sync most recent data: {e}")
            raise