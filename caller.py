import asyncio
import random
import time


class QueueListenerBatchifyer:
    def __init__(self, node, process_callable):
        self.node = node
        self.process_callable = process_callable

    async def call_service(self):
        queue = self.node.connector.queue
        while True:
            out = []
            rest = queue.qsize()
            for i in range(min(self.node.batch_size, rest)):
                item = await queue.get()
                out.append(item)
                queue.task_done()

            if out:
                await asyncio.sleep(2 * random.random())  # transport protocol caller simulation
                for i in out:
                    response = f'processed on {time.time()} in batch with {str([i.user_id for i in out])}'
                    await self.process_callable(i.user_id, self.node, response)
            await asyncio.sleep(0.3)

    async def register_consumer(self):
        asyncio.ensure_future(self.call_service())
