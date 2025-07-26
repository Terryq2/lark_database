import json
from datetime import datetime
from dataclasses import dataclass
from typing import List, Tuple
import dotenv


@dataclass
class Config:
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
    def __init__(self, env_file: str = ".env", config_file: str = "config.json"):
        self.config = self._load_config(env_file)
        with open(config_file, 'r', encoding='utf-8') as f:
            self.schemas = json.load(f)

    def _load_config(self, env_file: str) -> Config:
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
        return getattr(self.config, key)
    
    def get_columns(self, category: str) -> List[str]:
        return self.schemas[category]["columns"]

    def get_timestamp_column(self, category: str) -> int:
        return self.schemas[category].get("timestamp_column", 0)


class FinancialQueries:
    VALID_TIME_SPANS = {"month", "day"}
    DATE_FORMATS = {
        "month": "%Y-%m",
        "day": "%Y-%m-%d"
    }

    def __init__(self, financial_category: str, time_range: str, query_date: str):
        self.category = financial_category
        self.time_spans = [time_range]
        self.query_dates = [query_date]
        self.size = 1

    def add_new_query(self, time_span: str, query_date: str):
        if time_span not in self.VALID_TIME_SPANS:
            raise ValueError(f"Invalid time_span. Must be one of: {self.VALID_TIME_SPANS}")
        
        if not self._is_real_and_valid_date(time_span, query_date):
            raise ValueError(f"Invalid date format for time_span '{time_span}': {query_date}")

        self.time_spans.append(time_span)
        self.query_dates.append(query_date)
        self.size += 1
            
    def _is_real_and_valid_date(self, time_span: str, input_date: str) -> bool:
        try:
            date_format = self.DATE_FORMATS.get(time_span)
            if not date_format:
                return False
            datetime.strptime(input_date, date_format)
            return True
        except ValueError:
            return False

    def to_tuple(self) -> List[Tuple[str, str, str]]:
        return [(self.category, self.time_spans[i], self.query_dates[i]) for i in range(self.size)]