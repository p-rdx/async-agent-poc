import asyncio
import random
import time

from pprint import pprint

from pipeline import Pipeline, Node
from caller import QueueListenerBatchifyer
from connector import AioQueueConnector, CmdConnector


class State:
    def __init__(self, user_id):
        self.user_id = user_id
        self.msg = None
        self.services = dict()

    def add_utterance(self, msg):
        self.msg = msg


class StateStorage:  # чё то типа монги
    def __init__(self):
        self.states = {}

    async def get_or_create(self, user_id):
        state = self.states.get(user_id, None)
        if not state:
            state = State(user_id)
            self.states[user_id] = state
        return state

    async def update_service_responses(self, user_id, service, response, state_named_group):
        state = await self.get_or_create(user_id)
        if not state.services.get(state_named_group):
            state.services[state_named_group] = dict()
        state.services[state_named_group][service] = response
        await asyncio.sleep(random.random())
        return state

    async def add_utterance(self, user_id, msg):
        state = await self.get_or_create(user_id)
        state.msg = msg
        return


class AsyncAgent:
    def __init__(self, pipeline, statestorage):
        self.statestorage = statestorage
        self.pipeline = pipeline

    async def register_msg(self, user_id, msg):
        state = await self.statestorage.get_or_create(user_id)
        await self.statestorage.add_utterance(state.user_id, msg)
        await self.process(state.user_id)

    async def process(self, user_id, node=None, service_response=None):
        if node:
            state = await self.statestorage.update_service_responses(user_id, node.name, service_response, node.state_named_group)
        else:
            state = await self.statestorage.get_or_create(user_id)
        service_response_groups = self.pipeline.get_state_named_groups()
        done_services = set()
        waiting_services = set()
        for group_name in service_response_groups:
            services = state.services.get(group_name, None)
            if not services:
                continue
            done_services.update({key for key, value in services.items() if value is not None})
            waiting_services.update({key for key, value in services.items() if value is None})
        next_services = self.pipeline.get_next_nodes(done=done_services, waiting=waiting_services)
        ns = [i for i in next_services.keys()]
        if ns:                                                                      # remove
            print(f'start processing {user_id}, sending to {ns}')                   # remove
        for service, node in next_services.items():
            await self.statestorage.update_service_responses(user_id, service, None, node.state_named_group)
            await node.connector.process(state)
            print(f'{user_id} send to {service}')


async def produce(agent, count):
    for i in range(count):
        user_id = f'user No {i}'
        message = f'message for {i}'
        print(f'producing {i}')
        await asyncio.sleep(random.random())
        await agent.register_msg(user_id, message)


async def run(count):
    testconfig = {
        'anno1': {'connector': AioQueueConnector(asyncio.Queue()), 'previous_nodes': set(), 'batch_size': 2, 'state_named_group': 'annotators'},
        'anno2': {'connector': AioQueueConnector(asyncio.Queue()), 'previous_nodes': set(), 'batch_size': 3, 'state_named_group': 'annotators'},
        'anno3': {'connector': AioQueueConnector(asyncio.Queue()), 'previous_nodes': set(), 'batch_size': 2, 'state_named_group': 'annotators'},
        'serv2.1': {
            'connector': AioQueueConnector(asyncio.Queue()),
            'previous_nodes': {'anno1', 'anno2'},
            'batch_size': 4,
            'state_named_group': 'skills'},
        'serv2.2': {
            'connector': AioQueueConnector(asyncio.Queue()),
            'previous_nodes': {'anno1', 'anno2', 'anno3'},
            'batch_size': 2,
            'state_named_group': 'skills'},
        'serv2.3': {
            'connector': AioQueueConnector(asyncio.Queue()),
            'previous_nodes': {'anno1', 'anno2', 'anno3'},
            'batch_size': 1,
            'state_named_group': 'skills'},
        'serv2.4': {
            'connector': AioQueueConnector(asyncio.Queue()),
            'previous_nodes': {'anno1', 'anno2', 'anno3'},
            'batch_size': 1,
            'state_named_group': 'skills'},
        'finally': {
            'connector': CmdConnector(),
            'previous_nodes': {'serv2.1', 'serv2.2', 'serv2.3', 'serv2.4'},
            'state_named_group': 'responder'}
    }

    testnodes = []
    for name, node in testconfig.items():
        testnodes.append(Node(name, node['connector'], node['previous_nodes'], node['state_named_group'], node.get('batch_size', 0)))

    pipeline = Pipeline(testnodes)

    state_storage = StateStorage()

    agent = AsyncAgent(pipeline, state_storage)
    consumers = []

    for node in pipeline.nodes.values():
        if node.name == 'finally':
            continue
        caller = QueueListenerBatchifyer(node, agent.process)
        consumers.append(await caller.register_consumer())
        await node.connector.join()

    await produce(agent, count)

loop = asyncio.get_event_loop()
loop.set_debug(True)
try:
    loop.run_until_complete(run(5))
finally:
    loop.stop()
    pending = asyncio.Task.all_tasks()
    loop.run_until_complete(asyncio.gather(*pending))

loop.close()