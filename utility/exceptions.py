"""Stores the exceptions used"""

class InvalidFinancialCategoryException(Exception):
    """表示无效财务类别编码的异常。

    当用户输入的财务类别编码不在有效范围（C01 到 C23）内时引发此异常。

    示例：
        用户输入 'C99' 时，会抛出此异常。

    属性：
        message (str): 异常提示信息，默认为 "财务类别编码无效"。
    """

    def __init__(self, message="财务类别编码无效"):
        self.message = message
        super().__init__(message)


class InvalidTimespanException(Exception):
    """表示无效时间跨度参数的异常。

    当用户输入的时间跨度不是 'month' 或 'day' 时引发此异常。

    示例：
        用户输入 'year' 时，会抛出此异常。

    属性：
        message (str): 异常提示信息，默认为 "时间跨度无效，只能为 'month' 或 'day'"。
    """

    def __init__(self, message="时间跨度无效，只能为 'month' 或 'day'"):
        self.message = message
        super().__init__(message)

class DataFetchException(Exception):
    """表示获取数据时发生错误的异常。

    当程序在获取数据过程中遇到错误时抛出此异常。

    示例：
        在请求接口失败或数据解析错误时，可以抛出此异常。

    属性：
        message (str): 异常提示信息，默认为 "获取数据失败"。
    """

    def __init__(self, message="获取数据失败"):
        self.message = message
        super().__init__(message)

class DataProcessException(Exception):
    """表示处理数据时发生错误的异常。

    当程序在处理数据过程中遇到错误时抛出此异常。

    示例：
        在请求接口失败或数据解析错误时，可以抛出此异常。

    属性：
        message (str): 异常提示信息，默认为 "获取数据失败"。
    """

    def __init__(self, message="处理数据失败"):
        self.message = message
        super().__init__(message)