import asyncio
import sys
import time
from pprint import pprint


class Connector():
    async def process(self, payload):
        pass

    async def join(self):
        pass


class AioQueueConnector(Connector):  # пример коннектора с асинхронной батчификацией
    def __init__(self, queue):
        self.queue = queue

    async def process(self, payload):
        return await self.queue.put(payload)

    async def join(self):
        await self.queue.join()


class SingleRequestConnector(Connector):  # пример коннектора без батчификации
    def __init__(self):
        pass

    async def process(self, payload):
        await asyncio.sleep(2)  # имитация бурной деятельности по отправке запроса
        return f'processed on {time.time()} by single request connector'


class CmdConnector(Connector):  # пример исходящего коннектора
    async def process(self, payload):
        pprint(payload.__dict__)
        return
