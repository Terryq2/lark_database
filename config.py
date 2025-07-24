import json
from datetime import datetime
import regex as re

from dataclasses import dataclass
import dotenv


@dataclass
class Config:
    """存储Yuekeyun和飞书API的配置信息。

    属性:
        BASE_URL (str): Yuekeyun API的基础URL。
        APP_KEY (str): Yuekeyun API的应用密钥。
        SECRET_KEY (str): Yuekeyun API的秘密密钥。
        LEASE_CODE (str): Yuekeyun的租户代码。
        CINEMA_LINK_ID (str): 影院链接ID。
        CHANNEL_CODE (str): 渠道代码。
        FEISHU_APP_KEY (str): 飞书API的应用密钥。
        FEISHU_APP_SECRET (str): 飞书API的秘密密钥。
    """
    BASE_URL: str
    APP_KEY: str
    SECRET_KEY: str
    LEASE_CODE: str
    CINEMA_LINK_ID: str
    CHANNEL_CODE: str
    FEISHU_APP_KEY: str
    FEISHU_APP_SECRET: str
    WIKI_APP_TOKEN: str

class ConfigManager:
    """管理从环境文件加载的配置信息。

    该类负责从指定的.env文件加载配置，并将其转换为Config对象。

    属性:
        config (Config): 加载的配置对象。
    """

    def __init__(self, env_file: str = ".env", config_file: str = "config.json"):
        """初始化ConfigManager，加载环境文件中的配置。

        参数:
            env_file (str): 环境文件的路径，默认为".env"。

        示例:
            >>> config_manager = ConfigManager(".env")
            >>> config = config_manager.config
            >>> print(config.base_url)
            'https://gw.open.yuekeyun.com/openapi/param2/1/alibaba.dme.lark'
        """
        self.config = self._load_config(env_file)
        with open(config_file, 'r', encoding='utf-8') as f:
            self.schemas = json.load(f)

    def _load_config(self, env_file: str) -> Config:
        """从环境文件中加载配置并返回Config对象。

        参数:
            env_file (str): 环境文件的路径。

        返回:
            Config: 包含所有配置字段的Config对象。

        异常:
            KeyError: 如果环境文件中缺少所需的配置键。
        """
        keys = dotenv.dotenv_values(env_file)
        required_keys = [
            "APP_KEY", "SECRET_KEY", "LEASE_CODE", "CINEMA_LINK_ID",
            "CHANNEL_CODE", "FEISHU_APP_KEY", "FEISHU_APP_SECRET", "WIKI_APP_TOKEN"
        ]
        for key in required_keys:
            if key not in keys:
                raise KeyError(f"缺少必需的配置键: {key}")
        
        return Config(
            BASE_URL="https://gw.open.yuekeyun.com/openapi/param2/1/alibaba.dme.lark",
            APP_KEY=keys["APP_KEY"],
            SECRET_KEY=keys["SECRET_KEY"],
            LEASE_CODE=keys["LEASE_CODE"],
            CINEMA_LINK_ID=keys["CINEMA_LINK_ID"],
            CHANNEL_CODE=keys["CHANNEL_CODE"],
            FEISHU_APP_KEY=keys["FEISHU_APP_KEY"],
            FEISHU_APP_SECRET=keys["FEISHU_APP_SECRET"],
            WIKI_APP_TOKEN=keys["WIKI_APP_TOKEN"]
        )
    
    def get(self, key: str) -> str:
        """获取指定配置字段的值。

        参数:
            key (str): 要获取的配置字段名称（如"base_url"）。

        返回:
            str: 指定字段的配置值。

        异常:
            AttributeError: 如果指定的配置字段不存在。
        """
        return getattr(self.config, key)
    
    def get_columns(self, category: str) -> list[str]:
        return self.schemas[category]["columns"]

    def get_timestamp_column(self, category: str) -> int:
        return self.schemas[category].get("timestamp_column", 0)

class FinancialQueries:
    def __init__(self, financial_category: str, time_range: str, query_date: str):
        
        self.category = financial_category
        self.time_spans = [time_range]
        self.querie_dates = [query_date]
        self.size = 1


    def add_new_query(self, time_span: str, query_date: str):
        assert(time_span in ("month", "day"))
        assert(self.is_real_and_valid_date(time_span, query_date))

        self.time_spans.append(time_span)
        self.querie_dates.append(query_date)
        self.size += 1
            
    def is_real_and_valid_date(self, time_span: str, input_date: str):
        try:
            if time_span == "month":
                datetime.strptime(input_date, "%Y-%m")
            else:
                datetime.strptime(input_date, "%Y-%m-%d")
            return True
        except ValueError:
            return False


    def to_tuple(self) -> list[tuple[str, str, str]]:
        """将查询条目转换为元组格式。

        返回:
            Tuple[str, str, str]: 包含(data_type, time_range, query_date)的元组。

        示例:
            >>> query = FinancialQuery('C01', 'day', '2023-01-01')
            >>> query.to_tuple()
            ('C01', 'day', '2023-01-01')
        """
        return [(self.category, self.time_spans[i], self.querie_dates[i]) for i in range(self.size)]
        