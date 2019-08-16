import asyncio
import sys
from pprint import pprint


class Connector():
    async def process(self, payload):
        pass

    async def join(self):
        pass


class AioQueueConnector(Connector):
    def __init__(self, queue):
        self.queue = queue

    async def process(self, payload):
        return await self.queue.put(payload)

    async def join(self):
        await self.queue.join()


class CmdConnector(Connector):
    async def process(self, payload):
        pprint(payload.__dict__)
        return
