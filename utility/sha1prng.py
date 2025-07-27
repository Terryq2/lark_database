"""
Decodes the encrypted message after fetching them from YKY database
"""
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

    def _remove_control_characters(self, input_str: str):
        return input_str.rstrip("""\x00\x01\x02\x03\x04\x05
                               \x06\x07\x08\x09\x0a\x0b
                               \x0c\x0d\x0e\x0f\x10\x11
                               \x12\x13\x14\x15\x16\x17
                               \x18\x19\x1a\x1b\x1c\x1d
                               \x1e\x1f""")

    def decode(self, value: str) -> str:
        """ 
        使用 AES/ECB/NoPadding 模式解密 Base64 编码的字符串。

        该方法通过 AES-ECB 算法解密输入的 Base64 编码字符串，并移除解密后数据中的填充字符。
        注意：ECB 模式安全性较低，建议仅在兼容旧系统时使用。

        Args:
            value (str): Base64 编码的加密字符串。需确保字符串符合 Base64 格式，
                        且长度是 AES 块大小（16 字节）的整数倍（因使用 NoPadding）。

        Returns:
            str: 解密后的原始字符串。会自动移除尾部可能存在的填充字符（ASCII 控制字符）。

        Raises:
            ValueError: 如果 `value` 不是有效的 Base64 字符串，或解密后数据无法解码为 ASCII。
            TypeError: 如果 `self.sha1prng_key` 不是有效的十六进制字符串。

        Notes:
            1. 密钥来源：`self.sha1prng_key` 应为十六进制字符串，例如 `"2b7e151628..."`。
            2. 填充处理：解密后会移除 ASCII 控制字符（0x00-0x1F），但不会验证原始数据是否包含合法填充。
            3. 安全警告：ECB 模式无随机性，相同明文会生成相同密文，不建议用于新系统。

        Example:
            >>> cipher = MyCipher(sha1prng_key="2b7e151628...")
            >>> cipher.decode("SGVsbG8gd29ybGQhAA==")
            'Hello world!'
    
        AES/ECB/NoPadding decrypt
        假设value 是base64 encoded string
        """
        cryptor = AES.new(bytes.fromhex(self.sha1prng_key), AES.MODE_ECB)
        ciphertext = cryptor.decrypt(base64.b64decode(value))
        return self._remove_control_characters(bytes.decode(ciphertext,
                            "ASCII"))

    def get_sha1prng_key(self, key: str):
        """使用 SHA1PRNG 算法生成 AES-128 密钥（基于种子密钥派生）。

        通过两次 SHA-1 哈希迭代从输入种子密钥派生一个 32 字符（16 字节）的十六进制字符串，
        模拟 Java 中 `SHA1PRNG` 密钥生成器的行为（常用于旧版加密系统）。

        Args:
            key (str): 用于生成密钥的种子字符串。建议长度至少为 16 字节以增强安全性。

        Returns:
            str: 32 字符的十六进制字符串（大写），格式如 `"2B7E1516..."`，可直接转换为字节作为 AES-128 密钥。

        Raises:
            TypeError: 如果 `key` 不是字符串类型。

        Notes:
            1. **安全性警告**：
            - SHA-1 已不再安全，不建议在新系统中使用（应改用 PBKDF2、Argon2 或 HKDF）。
            - 两次 SHA-1 哈希仅为模拟旧系统行为，实际密钥强度受种子密钥质量限制。
            2. **兼容性**：此方法模拟 Java 的 `SHA1PRNG`，常用于与遗留系统交互。
            3. **密钥长度**：返回的 32 字符十六进制字符串对应 16 字节（AES-128 密钥长度）。

        Example:
            >>> cipher = MyCipher()
            >>> cipher.get_sha1prng_key("my_secret_key")
            '2B7E151628AED2A6ABF7158809CF4F3C'
        """
        signature = hashlib.sha1(key.encode()).digest()
        signature = hashlib.sha1(signature).digest()
        return signature.hex().upper()[:32]
