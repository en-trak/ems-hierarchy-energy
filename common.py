import uuid
import numpy as np

def zeroUUID():
  """
  生成一个全是 0 的 UUID。

  Returns:
    一个全是 0 的 UUID。
  """

  # 创建一个长度为16的bytes对象，每个字节都是0
  zero_bytes = b'\x00' * 16

  # 将bytes对象转换为UUID对象
  zero_uuid = uuid.UUID(bytes=zero_bytes)

  # print(zero_uuid)  # 输出: 00000000-0000-0000-0000-000000000000

  return zero_uuid

def is_none_or_nan(value):
  """
  判断值是否为 NaN 或 None。

  Args:
    value: 要判断的值。

  Returns:
    如果值为 NaN 或 None，则返回 True；否则返回 False。
  """

  return value is None or np.isnan(value)

