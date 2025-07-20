""""
Helper functions
"""

import hashlib
import hmac

import os
import httpx

from utility import exceptions
from utility import FINANCIAL_DATA_TYPE_MAP
from utility import sha1prng

def download_urls(encrypted_urls: list[str],
                  financial_category: str,
                  decrypter: sha1prng.Decrypter):
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
    for secret_url in encrypted_urls:
        try:
            data_response = httpx.get(decrypter.decode(secret_url))
        except httpx.HTTPError:
            clear_files(file_path_stack)
            raise exceptions.DataFetchException() from httpx.HTTPError

        utf8_content = data_response.content.decode('gbk').encode('utf-8')

        filename = f"{FINANCIAL_DATA_TYPE_MAP[financial_category]}/data_part{file_id}.csv"

        try:
            with open(filename, "wb") as f:
                f.write(utf8_content)
                file_path_stack.append(filename)
        except OSError:
            clear_files(file_path_stack)
            raise exceptions.DataFetchException() from OSError

        print(f"Downloaded: {filename}")
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

def query_data(api_name: str, query_parameters: dict[str, str], app_key: str) -> str:
    """
    使用 HTTP GET 请求从指定 API 查询数据。

    通过构造请求 URL 和参数，使用 httpx 客户端发送请求，并返回响应内容。

    Args:
        api_name (str): API 的名称，用于拼接请求 URL。
        query_parameters (dict[str, str]): 请求的查询参数字典。

    Returns:
        str: API 返回的响应内容（通常是字符串格式）。
    """
    base_url: str = "https://gw.open.yuekeyun.com/openapi/param2/1/alibaba.dme.lark"
    with httpx.Client() as client:
        response = client.get(f"{base_url}/{api_name}/{app_key}?",
                            params=query_parameters)
    return response

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


def combine_data_files(file_paths: list[str], financial_category: str):
    """
    将多个财务数据 CSV 文件合并为一个输出文件，跳过注释行和重复的表头。
    
    输出文件将命名为 "<数据类型名称>.csv"，并保存在以对应数据类型名称命名的文件夹中。
    该名称由 `FINANCIAL_DATA_TYPE_MAP` 字典通过 `financial_category` 键确定。

    参数:
        file_paths (list[str]): 输入 CSV 文件路径列表。
        financial_category (str): 财务数据类型编码，需存在于 `FINANCIAL_DATA_TYPE_MAP` 中。

    异常:
        ValueError: 如果财务类型未知，或第一个文件格式无效。
        OSError: 如果文件写入时发生 I/O 错误。

    示例:
        >>> combine_data_files(["文件1.csv", "文件2.csv"], "C02")
        # 输出文件将写入 商品订单数据/商品订单数据.csv
    """
    if not file_paths:
        return

    output_path = f"{FINANCIAL_DATA_TYPE_MAP[financial_category]}/{FINANCIAL_DATA_TYPE_MAP[financial_category]}.csv"

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

        for path in file_paths[1:]:
            lines = get_filtered_lines(path)
            for line in lines[2:]:  # Skip header lines
                out_file.write(line.encode("utf-8") + b"\n")


