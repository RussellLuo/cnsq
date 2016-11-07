import logging
import struct
import re

import ujson

from .helpers import bytes_types, to_bytes


logger = logging.getLogger(__name__)


class FrameType:

    response = 0
    error = 1
    message = 2


class Frame:

    size_len = 4

    heartbeat = b'_heartbeat_'

    @staticmethod
    def unpack_size(data):
        return struct.unpack('>l', data)[0]

    @staticmethod
    def unpack_frame(data):
        frame_type = struct.unpack('>l', data[:4])[0]
        return frame_type, data[4:]

    @staticmethod
    def unpack_message(data):
        timestamp = struct.unpack('>q', data[:8])[0]
        attempts = struct.unpack('>h', data[8:10])[0]
        id_ = data[10:26]
        body = data[26:]
        return id_, body, timestamp, attempts


class Command:

    valid_name_max_len = 64

    valid_name_pattern = re.compile(r'^[\.a-zA-Z0-9_-]+(#ephemeral)?$')

    magic_identifier = b'  V2'

    @classmethod
    def _pack_size(cls, size):
        return struct.pack('>l', size)

    @classmethod
    def _pack_data(cls, data):
        if data is None:
            return b''
        return cls._pack_size(len(data)) + to_bytes(data)

    @classmethod
    def _is_valid_name(cls, name):
        if not 0 < len(name) <= cls.valid_name_max_len:
            return False
        return bool(cls.valid_name_pattern.match(name))

    @classmethod
    def _command(cls, cmd, data, *params):
        logger.info('%s' % cmd)
        params = tuple(to_bytes(param) for param in params)
        return b' '.join((cmd,) + params) + b'\n' + cls._pack_data(data)

    @classmethod
    def subscribe(cls, topic, channel):
        assert cls._is_valid_name(topic)
        assert cls._is_valid_name(channel)
        return cls._command(b'SUB', None, topic, channel)

    @classmethod
    def identify(cls, data):
        return cls._command(b'IDENTIFY', to_bytes(ujson.dumps(data)))

    @classmethod
    def auth(cls, data):
        return cls._command(b'AUTH', to_bytes(data))

    @classmethod
    def ready(cls, count):
        assert isinstance(count, int), 'ready count must be an integer'
        assert count >= 0, 'ready count cannot be negative'
        return cls._command(b'RDY', None, str(count))

    @classmethod
    def finish(cls, id_):
        return cls._command(b'FIN', None, id_)

    @classmethod
    def requeue(cls, id_, time_ms=0):
        assert isinstance(time_ms, int), 'requeue time_ms must be an integer'
        return cls._command(b'REQ', None, id_, str(time_ms))

    @classmethod
    def touch(cls, id_):
        return cls._command(b'TOUCH', None, id_)

    @classmethod
    def nop(cls):
        return cls._command(b'NOP', None)

    @classmethod
    def pub(cls, topic, data):
        return cls._command(b'PUB', data, topic)

    @classmethod
    def mpub(cls, topic, data):
        assert isinstance(data, (set, list))
        body = cls._pack_size(len(data))
        for d in data:
            msg = 'message bodies must be bytestrings'
            assert isinstance(d, bytes_types), msg
            body += cls._pack_size(len(d)) + to_bytes(d)
        return cls._command(b'MPUB', body, topic)

    @classmethod
    def dpub(cls, topic, delay_ms, data):
        assert isinstance(delay_ms, int), 'dpub delay_ms must be an integer'
        return cls._command(b'DPUB', data, topic, str(delay_ms))
