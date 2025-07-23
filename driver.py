from config import ConfigManager
from fetcher import YKYRequester
from feishu_client import FeishuClient


from typing import List, Tuple

class Driver:
    """驱动Yuekeyun API和飞书客户端的数据获取与上传流程。

    该类负责协调配置管理、Yuekeyun API数据请求和飞书数据上传，处理财务数据的获取和同步。

    属性:
        config (ConfigManager): 配置管理器，存储API相关配置。
        requester (YKYRequester): Yuekeyun API请求客户端。
        uploader (FeishuClient): 飞书数据上传客户端。
    """

    def __init__(self, env_file_path: str, config_file_path: str):
        """初始化Driver，设置配置管理器和客户端。

        参数:
            env_file_path (str): 环境文件路径（例如，'.env'）。
            config_file_path (str): 数据schema配置文件路径（例如，'schemas.json'）。

        示例:
            >>> driver = Driver('.env', 'schemas.json')
            >>> driver.config.get('BASE_URL')
            'https://gw.open.yuekeyun.com/openapi/param2/1/alibaba.dme.lark'
        """
        self.config = ConfigManager(env_file_path, config_file_path)
        self.requester = YKYRequester(self.config)
        self.uploader = FeishuClient(self.config)

    def get_financial_data(self, entries: List[Tuple[str, str, str]]):
        """从Yuekeyun API获取财务数据。

        参数:
            entries (List[Tuple[str, str, str]]): 财务数据查询条目列表，每个条目包含
                (数据类型, 时间范围, 查询日期)。

        示例:
            >>> driver = Driver('.env', 'schemas.json')
            >>> entries = [('C01', 'day', '2023-01-01'), ('C02', 'month', '2023-01')]
            >>> driver.get_financial_data(entries)
        """
        self.requester.get_financial_data(entries)

    def get_and_upload_financial_data(self, entries: List[Tuple[str, str, str]], app_token: str, table_id: str):
        """从Yuekeyun API获取财务数据并上传至飞书。

        参数:
            entries (List[Tuple[str, str, str]]): 财务数据查询条目列表，每个条目包含
                (数据类型, 时间范围, 查询日期)。
            app_token (str): 飞书应用的令牌。
            table_id (str): 飞书目标数据表的ID。

        返回:
            None: 该方法不返回任何值，但会将数据上传至飞书。

        示例:
            >>> driver = Driver('.env', 'schemas.json')
            >>> entries = [('C01', 'day', '2023-01-01')]
            >>> driver.get_and_upload_financial_data(entries, 'app_token_123', 'table_id_456')
        """
        output_path = self.requester.get_financial_data(entries)
        self.uploader.post_csv_data_to_feishu(output_path, app_token, table_id)