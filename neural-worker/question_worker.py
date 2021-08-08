import asyncio
import copy
import os

import aioredis
import core
import json
import logging
import aio_pika


class QueueManager:
    def __init__(self, amqp_connection, amqp_queue: str, callback):
        self.connection = amqp_connection
        self.amqp_queue = amqp_queue
        self.callback = callback

    async def run(self):
        input_channel = await self.connection.channel()
        queue = await input_channel.declare_queue(self.amqp_queue)
        await queue.consume(self.callback, no_ack=True)


class QuestionProcessor:
    def __init__(self, redis_host: str, redis_port: int, amqp_url: str, amqp_input_queue: str,
                 amqp_operator_answer_queue: str):
        self.redis = aioredis.StrictRedis(host=redis_host, port=redis_port, db=0)
        self.connection = None
        self.amqp_input_queue = amqp_input_queue
        self.amqp_operator_answer_queue = amqp_operator_answer_queue
        self.amqp_url = amqp_url
        self.neural_answer_queue_manager = None
        self.operator_answer_queue_manager = None
        self.kafka_producer = None

    async def run(self, loop):
        if self.amqp_operator_answer_queue is None and self.amqp_input_queue is None:
            exit(0)

        try:
            self.connection = await aio_pika.connect(self.amqp_url, loop=loop)
        except Exception as e:
            logging.exception(e)
            exit(1)

        if self.amqp_operator_answer_queue != '' and self.amqp_operator_answer_queue is not None:
            channel = await self.connection.channel()
            queue = await channel.declare_queue(self.amqp_operator_answer_queue)
            await queue.consume(self.oa_callback)
        elif self.amqp_input_queue != '' and self.amqp_input_queue is not None:
            channel = await self.connection.channel()
            queue = await channel.declare_queue(self.amqp_input_queue)
            await queue.consume(self.na_callback)
        else:
            exit(0)

    async def na_callback(self, message: aio_pika.IncomingMessage):
        body2 = json.loads(message.body)
        logging.info(f'Question is received: {body2}')
        answer = await self.get_answer(body2['question'])
        data = {
            'answer': answer or '',
            'metadata': body2['metadata'],
            'operator': False,
            'question': body2['question']
        }

        output_channel = await self.connection.channel()
        await output_channel.default_exchange.publish(
            aio_pika.Message(bytes(json.dumps(data), encoding='utf-8')),
            routing_key=body2['queue'],
        )
        message.ack()

    async def oa_callback(self, message: aio_pika.IncomingMessage):
        body2 = json.loads(message.body.decode('utf-8'))
        logging.info(f'Operator answer is received: {body2}')
        question = ' '.join(c.lower() for c in body2['question'] if not c.isspace())
        answer = body2['answer']

        redis_value = core.wrap_to_redis_value(question, answer)
        await self.redis.set(question, redis_value)
        message.ack()

    async def get_answer(self, question) -> (str, bool):
        minq = None
        mindist = float("inf")
        async for key in self.redis.scan_iter("*"):
            val = await self.redis.get(key)
            key = key.decode('utf-8')
            val = val.decode('utf-8')
            q = core.wrap_to_answer(key, val)
            dist = core.getmetric_q(question, q)
            if dist < mindist:
                mindist = dist
                minq = q

        qdist = mindist
        if qdist < 1.3:
            return minq.a
        else:
            return None


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    event_loop = asyncio.get_event_loop()
    qp = QuestionProcessor(
        redis_host=os.environ.get('REDIS_HOST', 'localhost'),
        redis_port=os.environ.get('REDIS_PORT', '6379'),
        amqp_url=os.environ.get('AMQP_URL', 'amqp://guest:guest@localhost:5672/'),
        amqp_input_queue=os.environ.get('NEURAL_QUESTIONS_QUEUE'),
        amqp_operator_answer_queue=os.environ.get('TO_DATABASE_QUEUE')
    )
    event_loop.create_task(qp.run(event_loop))
    event_loop.run_forever()
