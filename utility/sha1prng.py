

import hashlib
import base64
from Crypto.Cipher import AES


class Decrypter:
    """
    解密"dme.lark.data.finance.getFinancialData" 返回的密文'

    属性：
        sha1prng_key: 用key作为种子生成的伪随机数。阿里用这个加密了返回的密文

    WARNING: 
    THIS IS HACKY
    THIS DOES NOT FULLY CAPTURE THE COMPLEXITY OF SHA1PRNG IMPLEMENTED BY SUN
    THIS ONLY WORKS FOR THE CASE OF AES 128

    https://stackoverflow.com/questions/64786678/javascript-equivalent-to-java-sha1prng?answertab=modifieddesc#tab-top

    """
    def __init__(self, key: str):
        self.sha1prng_key = self.get_sha1prng_key(key)

    def decode(self, value: str) -> str:
        """ 
        AES/ECB/NoPadding decrypt
        假设value 是base64 encoded string
        """
        cryptor = AES.new(bytes.fromhex(self.sha1prng_key), AES.MODE_ECB)
        ciphertext = cryptor.decrypt(base64.b64decode(value))
        return bytes.decode(ciphertext, "ASCII").rstrip("\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f")
    
    def get_sha1prng_key(self, key: str):
        """Returns the result of SHA1PRNG with $key$ as a seed"""
        signature = hashlib.sha1(key.encode()).digest()
        signature = hashlib.sha1(signature).digest()
        return signature.hex().upper()[:32]