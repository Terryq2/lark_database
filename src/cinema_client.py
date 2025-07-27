"""This is a class that helps fetching data from the yuekeyun website"""
import logging
import os
import datetime

from utility import exceptions
from utility import FINANCIAL_DATA_TYPE_MAP

from utility.helpers import (
    get_signature,
    download_urls,
    combine_data_files,
    order_by_time,
    get_timestamp,
    make_request
)
from utility import sha1prng
from src import config


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class YKYRequester:
    """用于从悦刻云(Yuekeyun)网站获取数据的请求类。

    该类封装了与悦刻云API的交互功能，包括获取财务数据、处理签名认证、
    下载和解密数据文件等操作。

    Attributes:
        config (config.ConfigManager): 配置管理器实例，包含API密钥和其他配置。
        base_url (str): 所有 API 请求的基础 URL。

    Example:
        >>> config_manager = config.ConfigManager('.env', 'config.json')
        >>> requester = YKYRequester(config_manager)
        >>> data = requester.get_financial_data(('C01', 'day', '2023-01-01'))
    """

    # API相关常量
    BASE_URL = "https://gw.open.yuekeyun.com/openapi/param2/1/alibaba.dme.lark"
    API_NAME = "dme.lark.data.finance.getFinancialData"
    VALID_TIMESPANS = ('month', 'day')

    def __init__(self, in_config: config.ConfigManager):
        """初始化YKYRequester实例。

        Args:
            in_config (config.ConfigManager): 配置管理器实例。

        Raises:
            ValueError: 当配置管理器无效时抛出。
        """
        if not in_config:
            raise ValueError("Configuration manager cannot be None")

        self.config = in_config
        self.base_url = self.BASE_URL

        logger.info("YKYRequester initialized successfully")

    def _validate_inputs(self, financial_category: str, timespan: str, search_date: str) -> None:
        """验证输入参数的有效性。

        Args:
            financial_category (str): 财务数据类别代码。
            timespan (str): 时间跨度。
            search_date (str): 查询日期。

        Raises:
            exceptions.InvalidFinancialCategoryException: 当财务类别无效时抛出。
            exceptions.InvalidTimespanException: 当时间跨度无效时抛出。
        """
        if financial_category not in FINANCIAL_DATA_TYPE_MAP:
            logger.error(f"Invalid financial category: {financial_category}")
            raise exceptions.InvalidFinancialCategoryException()

        if timespan not in self.VALID_TIMESPANS:
            logger.error(f"Invalid timespan: {timespan}. Must be one of {self.VALID_TIMESPANS}")
            raise exceptions.InvalidTimespanException()

        datetime.datetime.strptime(search_date, "%Y-%m-%d")

        logger.debug(f"Input validation passed for {financial_category}, and {timespan}")

    def _build_query_parameters(self,
                                financial_category: str,
                                timespan: str,
                                search_date: str) -> dict[str, str]:
        """构建API查询参数。

        Args:
            financial_category (str): 财务数据类别代码。
            timespan (str): 时间跨度。
            search_date (str): 查询日期。

        Returns:
            Dict[str, str]: 构建好的查询参数字典。
        """
        query_parameters = {
            "leaseCode": self.config.get("LEASE_CODE"),
            "cinemaLinkId": self.config.get("CINEMA_LINK_ID"),
            "_aop_timestamp": get_timestamp(),
            "channelCode": self.config.get("CHANNEL_CODE"),
            "dataType": financial_category,
            "searchDateType": timespan,
            "searchDate": search_date
        }

        logger.debug(f"Built query parameters for {financial_category} on {search_date}")
        return query_parameters

    def _generate_signature(self, query_parameters: dict[str, str]) -> str:
        """生成API请求签名。

        Args:
            query_parameters (Dict[str, str]): 查询参数字典。

        Returns:
            str: 生成的签名字符串。
        """
        try:
            signature = get_signature(
                self.API_NAME,
                query_parameters,
                self.config.get("APP_KEY"),
                self.config.get("SECRET_KEY")
            )
            logger.debug("API signature generated successfully")
            return signature
        except Exception as e:
            logger.error(f"Failed to generate signature: {e}")
            raise

    def _fetch_download_urls(self, query_parameters: dict[str, str]) -> list[str]:
        """获取数据下载URL列表。

        Args:
            query_parameters (Dict[str, str]): 包含签名的完整查询参数。

        Returns:
            List[str]: 下载URL列表。

        Raises:
            Exception: 当API请求失败时抛出。
        """
        try:
            url = ("https://gw.open.yuekeyun.com/openapi/"
                   "param2/1/alibaba.dme.lark/dme.lark.data.finance.getFinancialData/"
                  f"{self.config.get("APP_KEY")}")

            response = make_request('GET', url, params=query_parameters)

            response_data = response.json()
            download_url_list = response_data['data']['bizData']['downloadUrlList']

            if not download_url_list:
                logger.warning("No download URLs found in API response")
                return []

            logger.info(f"Retrieved {len(download_url_list)} download URLs")
            return download_url_list

        except Exception as e:
            logger.error(f"Failed to fetch download URLs: {e}")
            raise

    def _process_downloaded_data(
        self,
        download_url_list: list[str],
        financial_category: str,
        search_date: str
    ) -> str | None:
        """处理下载的数据文件。

        Args:
            download_url_list (List[str]): 下载URL列表。
            financial_category (str): 财务数据类别。
            search_date (str): 查询日期。

        Returns:
            str: 处理后的CSV文件路径。

        Raises:
            Exception: 当数据处理失败时抛出。
        """
        file_name_stack = []
        try:
            decrypter = sha1prng.Decrypter(self.config.get("LEASE_CODE"))
            logger.debug("Decrypter initialized")

            logger.info(f"Starting download of {len(download_url_list)} files")

            file_name_stack = download_urls(download_url_list,
                                            financial_category,
                                            decrypter,
                                            search_date)

            if not file_name_stack:
                logger.warning("No files were downloaded successfully")
                return ""

            logger.info(f"Downloaded {len(file_name_stack)} files successfully")

            logger.info("Combining data files")
            output_csv = combine_data_files(file_name_stack, financial_category, search_date, True)

            logger.info("Ordering data by timestamp")
            timestamp_column = self.config.get_timestamp_column(financial_category)
            if output_csv is None:
                raise exceptions.DataProcessException("Data file not generated")
            order_by_time(output_csv, timestamp_column)

            logger.info(f"Data processing completed successfully: {output_csv}")
            return output_csv

        except Exception as e:
            logger.error(f"Failed to process downloaded data: {e}")
            self._cleanup_on_error(file_name_stack)
            raise

    def _cleanup_on_error(self, file_list: list[str]) -> None:
        """错误时清理已下载的文件。

        Args:
            file_list (List[str]): 需要清理的文件列表。
        """
        if not file_list:
            return

        logger.info(f"Cleaning up {len(file_list)} files due to error")

        for file_path in file_list:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.debug(f"Removed file: {file_path}")
            except OSError as cleanup_error:
                logger.warning(f"Failed to cleanup file {file_path}: {cleanup_error}")

    def get_financial_data(self, entries: tuple[str, str, str]) -> str | None:
        """用于获取财政数据。

        从悦刻云API获取指定类型和日期的财务数据，包括下载、解密、合并和排序等完整流程。

        数据类型编码与名称对照表：
        - 详细映射请参考 FINANCIAL_DATA_TYPE_MAP

        Args:
            entries (Tuple[str, str, str]): 包含以下三个元素的元组：
                - financial_category (str): 调用哪一类数据，例如 'C13' 表示会员卡激活数据。
                - timespan (str): 数据的时间跨度，例如 'month' 表示返回一个月的数据。
                - search_date (str): 查询的日期，格式必须为 'YYYY-MM-DD'，若月份和日期不足两位需补齐。

        Returns:
            str: 处理完成的CSV文件路径。

        Raises:
            exceptions.InvalidFinancialCategoryException: 当财务类别代码无效时抛出。
            exceptions.InvalidTimespanException: 当时间跨度参数无效时抛出。
            Exception: 当数据获取或处理过程中发生错误时抛出。

        Note:
            如果数据获取过程中发生错误，所有已下载的文件都会被自动清理。

        Example:
            >>> requester = YKYRequester(config_manager)
            >>> csv_file = requester.get_financial_data(('C01', 'day', '2023-01-01'))
            >>> print(f"Data saved to: {csv_file}")
        """
        financial_category, timespan, search_date = entries

        logger.info(f"Starting financial data retrieval: category={financial_category}, "
                   f"timespan={timespan}, date={search_date}")

        self._validate_inputs(financial_category, timespan, search_date)

        query_parameters = self._build_query_parameters(financial_category,
                                                        timespan,
                                                        search_date)

        aop_signature = self._generate_signature(query_parameters)
        query_parameters["_aop_signature"] = aop_signature

        download_url_list = self._fetch_download_urls(query_parameters)

        if not download_url_list:
            logger.warning("No download URLs available, returning empty result")
            return ""

        output_csv = self._process_downloaded_data(download_url_list,
                                                    financial_category,
                                                    search_date)

        logger.info(f"Financial data retrieval completed successfully: {output_csv}")
        return output_csv
