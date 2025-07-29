"""
配置管理与财务查询模块。

该模块包括以下内容：
1. Config 数据类：用于保存从环境变量中读取的配置信息。
2. ConfigManager 类：负责加载 .env 和 JSON 配置文件，并提供配置信息访问方法。
3. FinancialQueries 类：用于构建财务数据的查询条件，包括时间粒度与查询日期。

该模块依赖：
- python-dotenv：用于解析 `.env` 文件。
- config.json：包含各类财务数据结构与字段定义。
- datetime：用于时间格式验证与操作。
- dataclasses：用于定义轻量级数据容器。
- typing：用于类型注解，增强可读性与 IDE 支持。

使用示例：

    config = ConfigManager(".env", "config.json")
    app_key = config.get("APP_KEY")

    fq = FinancialQueries("票房收入", "day", "2025-07-14")
    fq.add_new_query("month", "2025-07")
    query_list = fq.to_tuple()

适用于需要从多个数据源读取配置并动态构建查询参数的场景。
"""

import json
from datetime import datetime
from dataclasses import dataclass
from typing import List, Tuple
import dotenv


@dataclass
class Config:
    """用于存储从环境变量加载的配置信息的数据类。"""
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
    """配置管理类，用于加载环境变量和 JSON 配置文件中的字段信息。"""

    def __init__(self, env_file: str = ".env", config_file: str = "config.json"):
        """
        初始化配置管理器，加载环境变量与 schema 配置。

        Args:
            env_file (str): 环境变量文件路径，默认为 ".env"。
            config_file (str): JSON 格式的 schema 配置文件路径，默认为 "config.json"。
        """
        self.config = self._load_config(env_file)
        with open(config_file, 'r', encoding='utf-8') as f:
            self.schemas = json.load(f)

    def _load_config(self, env_file: str) -> Config:
        """
        从 .env 文件中加载配置，并校验必需字段。

        Args:
            env_file (str): .env 文件路径。

        Returns:
            Config: 加载并校验后的配置对象。

        Raises:
            KeyError: 如果缺少必要的字段。
        """
        keys = dotenv.dotenv_values(env_file)
        required_keys = [
            "APP_KEY", "SECRET_KEY", "LEASE_CODE", "CINEMA_LINK_ID",
            "CHANNEL_CODE", "FEISHU_APP_KEY", "FEISHU_APP_SECRET", "WIKI_APP_TOKEN"
        ]

        missing_keys = [key for key in required_keys if key not in keys]
        if missing_keys:
            raise KeyError(f"缺少必需的配置键: {', '.join(missing_keys)}")

        return Config(
            BASE_URL="https://gw.open.yuekeyun.com/openapi/param2/1/alibaba.dme.lark",
            APP_KEY=keys["APP_KEY"], # type: ignore
            SECRET_KEY=keys["SECRET_KEY"], # type: ignore
            LEASE_CODE=keys["LEASE_CODE"], # type: ignore
            CINEMA_LINK_ID=keys["CINEMA_LINK_ID"], # type: ignore
            CHANNEL_CODE=keys["CHANNEL_CODE"], # type: ignore
            FEISHU_APP_KEY=keys["FEISHU_APP_KEY"], # type: ignore
            FEISHU_APP_SECRET=keys["FEISHU_APP_SECRET"], # type: ignore
            WIKI_APP_TOKEN=keys["WIKI_APP_TOKEN"] # type: ignore
        )

    def get(self, key: str) -> str:
        """
        根据键名获取配置值。

        Args:
            key (str): 配置项的字段名。

        Returns:
            str: 对应的配置值。
        """
        return getattr(self.config, key)

    def get_columns(self, category: str) -> List[str]:
        """
        获取指定类别下的字段名列表。

        Args:
            category (str): 财务类别名称。

        Returns:
            List[str]: 字段名组成的列表。
        """
        return self.schemas[category]["columns"]
    
    def get_name(self, category: str) -> str:
        """
        返回财务类型的名字，例如C01 对应 影票销售明细。
        """
        return self.schemas[category]['name']

    def get_timestamp_column(self, category: str) -> int:
        """
        获取指定类别下的时间戳字段索引，默认为 0。

        Args:
            category (str): 财务类别名称。

        Returns:
            int: 时间戳列的索引。
        """
        return self.schemas[category].get("timestamp_column", 0)


class FinancialQueries:
    """用于构建和验证财务查询条件的类。"""

    VALID_TIME_SPANS = {"month", "day"}
    DATE_FORMATS = {
        "month": "%Y-%m",
        "day": "%Y-%m-%d"
    }

    def __init__(self, financial_category: str, time_range: str, query_date: str):
        """
        初始化一个财务查询任务。

        Args:
            financial_category (str): 财务数据类别。
            time_range (str): 时间跨度类型，必须是 "month" 或 "day"。
            query_date (str): 查询的日期字符串，格式取决于时间跨度。
        """
        self.category = financial_category
        self.time_spans = [time_range]
        self.query_dates = [query_date]
        self.size = 1

    def add_new_query(self, time_span: str, query_date: str):
        """
        添加新的时间跨度和查询日期。

        Args:
            time_span (str): 时间跨度，"month" 或 "day"。
            query_date (str): 查询日期字符串。

        Raises:
            ValueError: 如果时间跨度不合法，或日期格式错误。
        """
        if time_span not in self.VALID_TIME_SPANS:
            raise ValueError(f"Invalid time_span. Must be one of: {self.VALID_TIME_SPANS}")

        if not self._is_real_and_valid_date(time_span, query_date):
            raise ValueError(f"Invalid date format for time_span '{time_span}': {query_date}")

        self.time_spans.append(time_span)
        self.query_dates.append(query_date)
        self.size += 1

    def _is_real_and_valid_date(self, time_span: str, input_date: str) -> bool:
        """
        检查输入日期是否符合指定时间跨度的格式。

        Args:
            time_span (str): 时间跨度类型。
            input_date (str): 输入的日期字符串。

        Returns:
            bool: 是否为合法日期格式。
        """
        try:
            date_format = self.DATE_FORMATS.get(time_span)
            if not date_format:
                return False
            datetime.strptime(input_date, date_format)
            return True
        except ValueError:
            return False

    def to_tuple(self) -> List[Tuple[str, str, str]]:
        """
        将查询条件转换为三元组列表。

        Returns:
            List[Tuple[str, str, str]]: 每项为 (category, time_span, query_date)。
        """
        return [(self.category, self.time_spans[i], self.query_dates[i]) for i in range(self.size)]
