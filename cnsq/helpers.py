bytes_types = (bytes, bytearray, memoryview)


def to_bytes(x, encoding='utf-8', errors='strict'):
    if isinstance(x, bytes_types):
        return bytes(x)
    if isinstance(x, str):
        return x.encode(encoding, errors)
    raise TypeError('expected bytes or a string, not %r' % type(x))


def to_str(x, encoding='utf-8', errors='strict'):
    if isinstance(x, bytes_types):
        return x.decode(encoding, errors)
    if isinstance(x, str):
        return x
    raise TypeError('expected bytes or a string, not %r' % type(x))
