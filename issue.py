import uuid
import binascii

import logging

# Configure logging to write to a file named "my_app.log"
logging.basicConfig(filename="my_app.log", level=logging.DEBUG)

# Create a logger for your application
logger = logging.getLogger(__name__)

# Log messages at different levels
logger.debug("This is a debug message")
logger.info("This is an informational message")
logger.warning("This is a warning message")
logger.error("This is an error message")
logger.critical("This is a critical message")


def convert_to_big_endian(bytes):
    """
    将 16 个字节转换为大端序
    :param bytes: 16 个字节的数组
    :return: 大端序字节对象
    """
    assert len(bytes) == 16, "输入字节数必须为 16"
    big_endian_bytes = bytearray()  # Use bytearray for bytes object
    for i in range(16):
        big_endian_bytes.append(bytes[15 - i])
    return big_endian_bytes

def send_uuid(uuid_str):
    # Convert string to UUID object
    uuid_instance = uuid.UUID(uuid_str)

    # Get UUID bytes (native order)
    uuid_bytes_a = uuid_instance.bytes

    # Python 和 Go 的字节序不同。
    # 在 Python 代码中将 UUID 字节转换为大端序，以便 Go 代码可以正确解析。
    uuid_bytes_b = convert_to_big_endian(uuid_bytes_a)

    logger.info(f"{uuid_bytes_a} {binascii.hexlify(uuid_bytes_b)}")

    try:
        3/0
    except Exception as e:                    
      logger.error("response e-error:{}".format(str(e)))

    logger.debug("---------------2-----------------")

if __name__ == "__main__":
    # Replace with your UUID string
    logger.debug("---------------1-----------------")
    uuid_str = "d79632b7-ce79-433b-9842-0ac39f413b9c"
    send_uuid(uuid_str)
    logger.debug("---------------3-----------------")



# package main

# import (
#     "fmt"
#     "bytes"
#     "uuid"
# )

# func main() {
#     // 接收 UUID 字节
#     // ... 接收逻辑 ...
#     uuid_bytes := make([]byte, 16)

#     // 将字节转换为 UUID 对象
#     id := uuid.FromBytesOrNil(uuid_bytes)

#     // 检查 UUID 是否有效
#     if id == uuid.Nil {
#         fmt.Println("无效的 UUID")
#         return
#     }

#     // 使用 UUID
#     fmt.Println("UUID:", id)
# }
