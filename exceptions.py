"""Stores the exceptions used"""

class InvalidFinancialCategoryException(Exception):
    """
    自定义异常类：用于表示财务类别编码无效。

    当用户输入的财务类别编码不在有效范围(C01 到 C23)内时引发此异常。

    示例：
        用户输入 'C99'，程序将抛出 InvalidFinancialCategoryException 异常。

    参数:
        code (str): 用户输入的无效编码。
        message (str): 异常提示信息，默认提示范围为 C01 到 C23。
    """
    def __init__(self, message="财务类别编码无效"):
        self.message = message
        super().__init__(f"{message}")

class InvalidTimespanException(Exception):
    """
    自定义异常类：用于表示时间跨度参数无效。

    当用户输入的时间跨度不是 'month' 或 'day' 时引发此异常。

    示例：
        用户输入 'year'，程序将抛出 InvalidTimespanException 异常。

    参数:
        message (str): 异常提示信息，默认提示只能为 'month' 或 'day'。
    """
    def __init__(self, message="时间跨度无效，只能为 'month' 或 'day'"):
        self.message = message
        super().__init__(f"{message}")