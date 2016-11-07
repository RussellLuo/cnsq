import logging

from cnsq import Consumer

logging.basicConfig(level=logging.INFO, format='')


async def handler(message):
    print(message.id, message.body, message.timestamp, message.attempts)
    message.requeue(5)


if __name__ == '__main__':
    consumer = Consumer(topic='test', channel='test',
                        nsqd_tcp_addresses=['localhost:4150'],
                        message_handler=handler)
    consumer.run()
