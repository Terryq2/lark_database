"""Main loop"""
import time
import hmac
import hashlib
import json

import dotenv
import httpx

class YKYRequester:
    """
    属性：
        base_url: 所有api请求都以这个url开始
        keys: 存储写在.env文件里的密钥
        
    """
    def __init__(self):
        self.keys: dict[str, str] = dotenv.dotenv_values()
        self.base_url: str = "https://gw.open.yuekeyun.com/openapi/param2/1/alibaba.dme.lark"

    def get_timestamp(self) -> int:
        """返回当前毫秒级unix timestamp
        出参:
            unix timestamp
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

    def get_category_list(self):
        """获取所有商品大类的列表"""
        api_name: str = "dme.lark.item.goods.getCategoryList"
        time_stamp: int = self.get_timestamp()

        parameters_for_signature = {"leaseCode": f"{self.keys["LEASE_CODE"]}",
                    "cinemaLinkId": f"{self.keys["CINEMA_LINK_ID"]}",
                    "_aop_timestamp": f"{time_stamp}",
                    "channelCode": f"{self.keys["CHANNEL_CODE"]}"}

        aop_signature = self.get_signature(api_name, parameters_for_signature)

        with httpx.Client() as client:
            print(f"{self.base_url}/{api_name}/{self.keys["APP_KEY"]}?")
            response = client.get(f"{self.base_url}/{api_name}/{self.keys["APP_KEY"]}?", params={
                    "leaseCode": f"{self.keys["LEASE_CODE"]}",
                    "cinemaLinkId": f"{self.keys["CINEMA_LINK_ID"]}",
                    "_aop_timestamp": f"{time_stamp}",
                    "channelCode": f"{self.keys["CHANNEL_CODE"]}",
                    "_aop_signature": f"{aop_signature}"
                })
        response.raise_for_status()
        json_data = response.json()
        print(json.dumps(json_data, indent=4, ensure_ascii=False))

if __name__ == "__main__":
    test = YKYRequester()
