from .protocol import Command, Frame


class Message:

    def __init__(self, id_, body, timestamp, attempts):
        self.id = id_
        self.body = body
        self.timestamp = timestamp
        self.attempts = attempts
        self.reply_cmd = None

    @classmethod
    def from_frame(cls, data):
        return cls(*Frame.unpack_message(data))

    def finish(self):
        self.reply_cmd = Command.finish(self.id)

    def requeue(self, delay=0):
        self.reply_cmd = Command.requeue(self.id, delay * 1000)

    def touch(self):
        self.reply_cmd = Command.touch(self.id)
