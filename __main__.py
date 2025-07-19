"""This is a class that helps fetching data from the yuekeyun website"""
import time
import hmac
import hashlib
import urllib.parse


import dotenv
import httpx

import exceptions
from sha1prng import sha1prng


class YKYRequester:
    """
    属性：
        base_url: 所有api请求都以这个url开始
        keys: 存储写在.env文件里的密钥
        
    """
    def __init__(self):
        self.keys: dict[str, str] = dotenv.dotenv_values()
        self.base_url: str = "https://gw.open.yuekeyun.com/openapi/param2/1/alibaba.dme.lark"
        self.financial_categories = [f"C{str(i).zfill(2)}" for i in range(1, 24)]

    def get_timestamp(self) -> int:
        """返回当前毫秒级unix timestamp
        出参:
            unix timestamp.
        """
        current_timestamp = int(time.time() * 1000)
        return current_timestamp

    def get_signature(self, api_name: str, paramaters: dict[str, str]) -> str:
        """用入参组合签名

        用secret_key作为密钥对parameters使用hmac_sha1

        参数：
            parameters: api请求的所有参数
            api_name: api端口的名字 例如：dme.lark.item.goods.getCategoryList

        出参:
            本次请求的hmac_sha1签名

        """
        signature_factor_url = f"param2/1/alibaba.dme.lark/{api_name}/{self.keys["APP_KEY"]}"
        signature_factor_params = ""

        for key, value in sorted(paramaters.items(), key=lambda item: item[0]):
            signature_factor_params += f"{key}{value}"

        signature_string = signature_factor_url + signature_factor_params
        print(signature_string)
        signature_hmac = hmac.new(
            self.keys["SECRET_KEY"].encode('utf-8'),
            signature_string.encode('utf-8'),
            hashlib.sha1
        ).hexdigest()

        return signature_hmac.upper()

    def get_financial_data(self, financial_category: str, timespan: str, search_date: str):
        """用于获取财政数据
            数据类型编码与名称对照表：

            | 编码   | 数据类型名称                     |
            |--------|----------------------------------|
            | C01    | 影票订单数据                     |
            | C02    | 商品订单数据                     |
            | C03    | 发卡数据                         |
            | C04    | 卡充值数据                       |
            | C05    | 卡消费数据                       |
            | C06    | 券回兑数据                       |
            | C07    | 商品进销存数据                   |
            | C08    | 商品出入库数据                   |
            | C09    | 销售消耗原材料数据               |
            | C10    | 销售消耗品项数据                 |
            | C11    | 会员卡续费数据                   |
            | C12    | 会员卡退卡数据                   |
            | C13    | 会员卡激活数据                   |
            | C14    | 会员卡补卡换卡数据               |
            | C15    | 货品操作明细数据                 |
            | C16    | 利润毛利数据（移动加权平均）     |
            | C17    | 利润毛利数据（月末加权平均）     |
            | C18    | 场次放映明细数据                 |
            | C19    | 优惠券销售明细数据               |
            | C20    | 影票订单数据（放映日期）         |
            | C21    | 欠款核销明细查询                 |
            | C22    | 计次卡明细数据                   |
            | C23    | 联名卡订单数据                   |

        参数：
            financial_category: 调用哪一类数据 如 C13是会员卡激活数据 \n
            timespan: 调用数据的时间跨度 如month 会返回一个月的数据 \n
            search_date: 日期 如 2025-07-19 格式必须是 年份-月-日， 如果月和日不足两位数则补满两位.

        """

        if financial_category not in self.financial_categories:
            raise exceptions.InvalidFinancialCategoryException()

        if timespan not in ('month', 'day'):
            raise exceptions.InvalidTimespanException()

        api_name: str = "dme.lark.data.finance.getFinancialData"
        time_stamp: int = self.get_timestamp()


        #Composing the request
        query_parameters = {"leaseCode": f"{self.keys["LEASE_CODE"]}",
                    "cinemaLinkId": f"{self.keys["CINEMA_LINK_ID"]}",
                    "_aop_timestamp": f"{time_stamp}",
                    "channelCode": f"{self.keys["CHANNEL_CODE"]}",
                    "dataType": f"{financial_category}",
                    "searchDateType": f"{timespan}",
                    "searchDate": f"{search_date}"}

        aop_signature = self.get_signature(api_name, query_parameters)
        query_parameters["_aop_signature"] = f"{aop_signature}"

        with httpx.Client() as client:
            response = client.get(f"{self.base_url}/{api_name}/{self.keys["APP_KEY"]}?", 
                                  params=query_parameters)

        response.raise_for_status()
        decrypter = sha1prng.Decrypter(self.keys['LEASE_CODE'])
        json_data = response.json()['data']['bizData']

        file_id = 0

        for secret_url in json_data['downloadUrlList']:
            decoded_url = decrypter.decode(secret_url)
            data_response = httpx.get(decoded_url)
            data_response.raise_for_status()


            gbk_content = data_response.content
            utf8_content = gbk_content.decode('gbk').encode('utf-8')

            filename = f"data_part{file_id}.csv"
            with open(filename, "wb") as f:
                f.write(utf8_content)
            print(f"Downloaded: {filename}")     
            file_id += 1

        # print(json.dumps(json_data, indent=4, ensure_ascii=False))

if __name__ == "__main__":
    test = YKYRequester()
    test.get_financial_data("C04", "month", "2025-06")
