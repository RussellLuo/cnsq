import logging
import socket

import curio

from .message import Message
from .protocol import Command, Frame, FrameType


logger = logging.getLogger(__name__)


class Consumer:

    def __init__(self, topic, channel, message_handler, *,
                 name=None, nsqd_tcp_addresses=None,
                 max_tries=5, max_in_flight=1, heartbeat_interval=30):
        self.topic = topic
        self.channel = channel
        self.message_handler = message_handler
        self.name = name or (topic + ':' + channel)
        self.nsqd_tcp_addresses = nsqd_tcp_addresses
        self.max_tries = max_tries
        self.max_in_flight = max_in_flight
        self.hostname = socket.gethostname()
        self.short_hostname = self.hostname.split('.')[0]
        self.heartbeat_interval = heartbeat_interval * 1000

    def run(self):
        curio.run(self.connect())

    async def connect(self):
        for tcp_address in self.nsqd_tcp_addresses:
            address, port = tcp_address.split(':')
            await curio.spawn(self.connect_to_nsqd(address, int(port)))

    async def connect_to_nsqd(self, address, port):
        sock = await curio.open_connection(address, port)
        stream = sock.as_stream()
        await stream.write(Command.magic_identifier)

        identify_data = {
            'client_id': self.short_hostname,
            'hostname': self.hostname,
            'heartbeat_interval': self.heartbeat_interval,
            'feature_negotiation': True,
        }
        await stream.write(Command.identify(identify_data))
        await self.handle_frame(stream)

        await stream.write(Command.subscribe(self.topic, self.channel))
        await self.handle_frame(stream)

        await stream.write(Command.ready(1))

        while True:
            await self.handle_frame(stream)

    async def read_frame(self, stream):
        size_bytes = await stream.read(Frame.size_len)
        size = Frame.unpack_size(size_bytes)
        frame_bytes = await stream.read(size)
        frame_type, data = Frame.unpack_frame(frame_bytes)
        logger.info('%r, %r' % (frame_type, data))
        return frame_type, data

    async def handle_frame(self, stream):
        frame_type, data = await self.read_frame(stream)
        if frame_type == FrameType.message:
            message = Message.from_frame(data)
            if message.attempts > self.max_tries:
                await self.discard_message(message)
            else:
                await self.handle_message(message)
            if message.reply_cmd:
                await stream.write(message.reply_cmd)
        elif frame_type == FrameType.response:
            if data == Frame.heartbeat:
                await stream.write(Command.nop())
            else:
                pass
        elif frame_type == FrameType.error:
            await self.handle_error(data)

    async def discard_message(self, message):
        message.finish()

    async def handle_message(self, message):
        await self.message_handler(message)

    async def handle_error(self, data):
        pass
