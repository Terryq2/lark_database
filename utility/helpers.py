""""
Helper functions
"""
from datetime import datetime
import hashlib
import hmac
import os
import time
from typing import Any

import httpx
import polars
from tqdm import tqdm

from . import FINANCIAL_DATA_TYPE_MAP
from . import exceptions
from . import sha1prng



def download_urls(encrypted_urls: list[str],
                  financial_category: str,
                  decrypter: sha1prng.Decrypter, search_date: str):
    """
    解密并下载一组加密的 URL 数据。

    该方法接收一组加密的 URL, 解密后从这些 URL 下载数据，并将下载的数据保存为 CSV 文件。下载的文件将根据财务类别存储在相应的文件夹中。

    如果在下载过程中发生任何错误，将清理已下载的文件并抛出异常。

    Args:
        encrypted_urls (list[str]): 包含加密 URL 的字符串列表，需要解密并下载这些 URL 指向的数据。
        financial_category (str): 财务数据类型的编码，用于指定下载的文件存储目录。

        
    Returns
        file_path_stack (list[str]): 包含所有csv文件的路径列表。
        directory_path (str): 文件夹路径

    Exceptions:
        DataFetchException: 如果在获取数据过程中发生任何错误，将抛出此异常。
        httpx.HTTPError: 如果请求过程中发生 HTTP 错误。
        OSError: 如果文件保存过程中发生操作系统相关错误（如权限问题等）。

    Example:
        该方法是 `get_financial_data` 的辅助方法，用于实际下载解密后的数据文件。

    """
    file_id = 0
    file_path_stack: list[str] = []
    os.makedirs(f"{FINANCIAL_DATA_TYPE_MAP[financial_category]}", exist_ok = True)
    for secret_url in tqdm(encrypted_urls, ncols=70, unit="url"):
        try:
            data_response = httpx.get(decrypter.decode(secret_url))
        except httpx.HTTPError as htttp_error:
            clear_files(file_path_stack)
            raise exceptions.DataFetchException() from htttp_error

        utf8_content = data_response.content.decode('gbk').encode('utf-8')

        filename = f"""{FINANCIAL_DATA_TYPE_MAP[financial_category]}/
                       {FINANCIAL_DATA_TYPE_MAP[financial_category]}
                       _({search_date})_part{file_id}.csv"""

        try:
            with open(filename, "wb") as f:
                f.write(utf8_content)
                file_path_stack.append(filename)
        except OSError:
            clear_files(file_path_stack)
            raise exceptions.DataFetchException() from OSError
        file_id += 1
    return file_path_stack


def clear_files(files_to_clear: list[str]):
    """
    删除指定的文件列表中的所有文件。

    该方法循环遍历文件列表，并依次删除每个文件。如果文件存在且没有被占用，则会成功删除。

    Args:
        files_to_clear (list[str]): 包含待删除文件路径的字符串列表。

    Exceptions:
        FileNotFoundError: 如果文件不存在，将抛出此异常。
        PermissionError: 如果没有权限删除文件，将抛出此异常。
        OSError: 其他与文件操作相关的错误。
    """
    while files_to_clear:
        file_name = files_to_clear.pop()
        os.remove(file_name)

def get_signature(api_name: str,
                  paramaters: dict[str, str],
                  app_key: str,
                  secret_key: str) -> str:
    """用入参组合签名。

    使用 secret_key 作为密钥，对 parameters 使用 HMAC-SHA1 算法生成签名。

    Args:
        parameters (dict): API 请求的所有参数。
        api_name (str): API 接口名称，例如 'dme.lark.item.goods.getCategoryList'。

    Returns:
        str: 本次请求的 HMAC-SHA1 签名。
    """

    signature_factor_url = f"param2/1/alibaba.dme.lark/{api_name}/{app_key}"
    signature_factor_params = ""

    for key, value in sorted(paramaters.items(), key=lambda item: item[0]):
        signature_factor_params += f"{key}{value}"

    signature_string = signature_factor_url + signature_factor_params

    return hmac.new(
        secret_key.encode('utf-8'),
        signature_string.encode('utf-8'),
        hashlib.sha1
    ).hexdigest().upper()


def combine_data_files(file_paths: list[str],
                       financial_category: str,
                       search_date: str,
                       remove_files_after_finish: bool) -> str | None:
    """
    将多个财务数据 CSV 文件合并为一个输出文件，跳过注释行和重复的表头。
    
    输出文件将命名为 "<数据类型名称>.csv"，并保存在以对应数据类型名称命名的文件夹中。
    该名称由 `FINANCIAL_DATA_TYPE_MAP` 字典通过 `financial_category` 键确定。

    Args:
        file_paths (list[str]): 输入 CSV 文件路径列表。
        financial_category (str): 财务数据类型编码，需存在于 `FINANCIAL_DATA_TYPE_MAP` 中。
        remove_files_after_finish (bool): 如果为真则再合并结束后，删除所有输入数据

    异常:
        ValueError: 如果财务类型未知，或第一个文件格式无效。
        OSError: 如果文件写入时发生 I/O 错误。

    示例:
        >>> combine_data_files(["文件1.csv", "文件2.csv"], "C02")
        # 输出文件将写入 商品订单数据/商品订单数据.csv
    """
    if not file_paths:
        return None

    output_path = (f"{FINANCIAL_DATA_TYPE_MAP[financial_category]}/"
                      f"{FINANCIAL_DATA_TYPE_MAP[financial_category]}"
                      f"_({search_date}).csv")

    def get_filtered_lines(path):
        with open(path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
            return [line for line in lines if not line.startswith("#")]

    with open(output_path, "wb") as out_file:
        # Process the first file
        first_lines = get_filtered_lines(file_paths[0])
        if len(first_lines) < 2:
            raise ValueError(f"File {file_paths[0]} does not contain enough data.")

        header = first_lines[1]
        out_file.write(header.encode("utf-8") + b"\n")

        for path in file_paths:
            lines = get_filtered_lines(path)
            for line in lines[2:]:  # Skip header lines
                out_file.write(line.encode("utf-8") + b"\n")

        if remove_files_after_finish:
            for path in file_paths:
                os.remove(path)

    return output_path

def read_csv(path: str):
    """
    读取汇总后的 CSV 文件并返回一个用于向飞书 POST 数据的结构，以分块方式处理。

    将 CSV 文件数据转换为包含多个记录块的列表，每个块包含最多 1000 条记录。
    每条记录包含一个 'fields' 键，其值为对应 CSV 行的字段字典，键和值均为字符串。
    例如：
    [
        [
            {
                "fields": {
                    "充值/续费日期": "2025-06-03 15:22:46",
                    "充值/续费影院名称": "联调测试影院1",
                    "充值/续费影院编码": "99990007",
                    "充值/续费影院所属区域": "青龙",
                    "订单号": "250603001002X900073171",
                    "卡号": "20004998868X"
                }
            },
            ...
        ],
        ...
    ]
    Args:
        path (str): CSV 文件的路径。

    Returns:
        list: 包含多个记录块的列表，每个块是一个字典列表，字典包含 'fields' 键，对应 CSV 行的字段字典。

    异常:
        polars.exceptions.ComputeError: 如果 CSV 文件格式错误或无法读取。
        FileNotFoundError: 如果指定的路径不存在。

    示例:
        >>> data = read_csv("data.csv")
        >>> print(data[0][0]["fields"])
        {'充值/续费日期': '2025-06-03 15:22:46', '充值/续费影院名称': '联调测试影院1', ...}

    注意:
        - 每块记录最多包含 1000 条数据，以适应飞书 API 的限制。
        - 使用 polars 库读取 CSV 文件，确保 truncate_ragged_lines=True 和 infer_schema=False 以处理不规则数据。
    """
    max_rows_per_post = 1000
    df = polars.read_csv(path, truncate_ragged_lines=True, infer_schema=False)
    data_dict = df.to_dicts()
    total_rows = len(data_dict)

    current_entry_ptr = 0
    list_of_records = []
    while current_entry_ptr < total_rows:
        if total_rows - current_entry_ptr > max_rows_per_post:
            chunk_size = max_rows_per_post
        else:
            chunk_size = total_rows - current_entry_ptr
        records = [
            {"fields": data_dict[current_entry_ptr: current_entry_ptr + chunk_size]}
        ]
        list_of_records.append(records)
        current_entry_ptr += max_rows_per_post

    return list_of_records


def merge_csv_files(folder_path: str) -> polars.DataFrame:
    """合并指定文件夹中的所有 CSV 文件到一个 Polars DataFrame，并清空原文件夹。

    遍历文件夹中的所有 CSV 文件，将它们读取为 Polars DataFrame 并进行垂直拼接（纵向合并）。
    合并完成后，删除原文件夹中的所有文件（包括非 CSV 文件）。

    Args:
        folder_path (str): 包含 CSV 文件的文件夹路径。路径需存在且可读。

    Returns:
        polars.DataFrame: 合并后的 DataFrame，包含所有 CSV 文件的数据。
            如果文件夹中没有 CSV 文件，返回一个空的 DataFrame。

    Raises:
        FileNotFoundError: 如果 `folder_path` 不存在或不可访问。
        polars.PolarsError: 如果 CSV 文件读取或合并失败（如格式不一致）。

    Notes:
        1. 合并时会自动截断不规则行（`truncate_ragged_lines=True`）。
        2. 不自动推断模式（`infer_schema=False`），以提高读取速度。
        3. 原文件夹中的所有文件（包括非 CSV）均会被删除，请谨慎使用！

    Example:
        >>> # 假设文件夹 `/data` 包含 a.csv 和 b.csv
        >>> df = merge_csv_files("/data")
        >>> df.shape  # 输出合并后的行数和列数
        (200, 5)
        >>> # 原文件夹 `/data` 将被清空
    """
    dfs = []
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            filepath = os.path.join(folder_path, filename)
            df = polars.read_csv(filepath, truncate_ragged_lines=True, infer_schema=False)
            dfs.append(df)
    for filename in os.listdir(folder_path):
        os.remove(os.path.join(folder_path, filename))
    return polars.concat(dfs, how="vertical")

def order_by_time(path: str, timestamp_col: int = 0):
    """
    读取 CSV 文件，按时间列排序并覆盖原文件。

    该函数读取指定路径的 CSV 文件，将第一列解析为日期时间格式，按升序排序后，
    将日期时间格式转换回字符串，并覆盖原文件保存排序后的数据。

    Args:
        path (str): CSV 文件的路径。

    Returns:
        None

    异常:
        polars.exceptions.ComputeError: 如果 CSV 文件格式错误或无法读取。
        FileNotFoundError: 如果指定的路径不存在。
        ValueError: 如果第一列的日期时间格式解析失败。
        OSError: 如果文件删除或写入操作失败。

    示例:
        >>> order_by_time("data.csv")
        # 假设 data.csv 的第一列为日期时间格式，文件将被排序并覆盖保存

    注意:
        - 第一列必须包含格式为 "%Y-%m-%d %H:%M:%S" 的日期时间字符串。
        - 使用 strict=False 处理非严格的日期时间解析，可能导致无效数据被解析为 null。
        - 原文件将被删除并覆盖，建议在操作前备份文件。
        - 使用 polars 库读取和处理 CSV 文件，确保 truncate_ragged_lines=True 和 infer_schema=False 以处理不规则数据。
    """

    df = polars.read_csv(path, truncate_ragged_lines=True, infer_schema=False)
    df = df.with_columns([
        polars.col(df.columns[timestamp_col]).str.strptime(polars.Datetime,
                                                           "%Y-%m-%d %H:%M:%S", 
                                                           strict=False)
    ])
    df = df.sort(df.columns[timestamp_col], descending = False)
    df = df.with_columns([
        polars.col(df.columns[timestamp_col]).dt.strftime("%Y-%m-%d %H:%M:%S")
    ])
    os.remove(path)
    df.write_csv(path, quote_style="always")


def get_past_days_this_month() -> list[str]:
    """
    返回当月中已经过去的所有日期（不包括今天），格式为 'YYYY-MM-DD' 的字符串列表。

    例如，如果今天是 2025 年 7 月 23 日，函数将返回从 '2025-07-01' 到 '2025-07-22' 的所有日期。

    Returns值：
        list[str]: 当前月份中从1号到昨天的所有日期字符串列表。
    """
    today = datetime.now()
    year = today.year
    month = today.month
    day = today.day
    return [f"{year}-{month:02d}-{d:02d}" for d in range(1, day)]


def get_timestamp() -> int:
    """返回当前毫秒级 Unix 时间戳。
    Returns:
        int: 当前的 Unix 时间戳（毫秒级）。
    """
    return int(time.time() * 1000)

def find_matching_table(json_data: dict[Any, Any] , table_name: str | None) -> str | None:
    """根据表名在嵌套的 JSON 数据中查找并返回对应的 `table_id`。

    在嵌套字典结构（假设表数据位于 `data["items"]` 下）中搜索与指定 `table_name` 匹配的表，
    若找到则返回其 `table_id`。

    Args:
        json_data (dict[Any, Any]): 包含表数据的嵌套字典。预期结构为：
            {
                "data": {
                    "items": [
                        {"name": str, "table_id": str},
                        ...
                    ]
                }
            }
            如果结构无效（如缺失关键字段），则返回 `None`。
        table_name (Optional[str]): 要查找的表名。若为 `None`，则直接返回 `None`。

    Returns:
        Optional[str]: 匹配到的 `table_id`（字符串），以下情况返回 `None`：
            - `table_name` 为 `None`。
            - 未找到匹配的表。
            - `json_data` 结构无效（如缺少 `data` 或 `items`）。

    Raises:
        TypeError: 如果 `json_data["data"]["items"]` 不是列表（尽管防御性编程已避免此情况）。

    Example:
        >>> json_data = {
        ...     "data": {
        ...         "items": [
        ...             {"name": "users", "table_id": "t1"},
        ...             {"name": "orders", "table_id": "t2"}
        ...         ]
        ...     }
        ... }
        >>> find_matching_table(json_data, "users")
        't1'
        >>> find_matching_table(json_data, "products")
        None
    """
    if table_name is None:
        return None

    try:
        for table in json_data['data']['items']:
            if table.get("name") == table_name:
                return table.get("table_id")
    except KeyError as e:
        raise KeyError("Json data is malformed or does not contain a data field") from e

def make_request(
    method: str,
    url: str,
    headers: dict[str, str] | None = None,
    json_data: dict[Any, Any] | None = None,
    params: dict[Any, Any] | None = None,
    timeout: float = 30.0
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
