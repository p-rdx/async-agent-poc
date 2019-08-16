import asyncio
import random
import time

from pprint import pprint

from pipeline import Pipeline, Node
from caller import QueueListenerBatchifyer
from connector import AioQueueConnector, CmdConnector, SingleRequestConnector


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
    def __init__(self, pipeline, statestorage, cache):
        self.statestorage = statestorage
        self.pipeline = pipeline
        self.cache = cache

    async def register_msg(self, user_id, msg):
        state = await self.statestorage.get_or_create(user_id)
        await self.statestorage.add_utterance(state.user_id, msg)
        self.cache[user_id] = {'done': set(), 'waiting': set()}  # новое сообщение - зарегистрировали в кэше, который отслеживает прохождение по пайплайну (и другие вещи)
        await self.process(state.user_id)


    async def process(self, user_id, node=None, service_response=None):
        if node:       # if node and node.state_updater - это будет костыль, чтобы результат можно было положить в стэйт as is сейчас
            state = await self.statestorage.update_service_responses(user_id, node.name, service_response, node.state_named_group)
            self.cache[user_id]['waiting'].discard(node.name)  # убираем сервис из waiting и добавляем его в done
            self.cache[user_id]['done'].add(node.name)
        else:
            state = await self.statestorage.get_or_create(user_id)

        next_services = self.pipeline.get_next_nodes(**self.cache[user_id])
        ns = {i for i in next_services.keys()}
        if ns:
            print(ns)
        self.cache[user_id]['waiting'].update(ns)  # Докидываем все последующие сервисы в waiting перед отправкой
        for service, next_node in next_services.items():
            result = await next_node.connector.process(state)
            print(f'{user_id} send to {service}')
            if result is not None:  # на случай, если сервис без разрыва. Например хтмль запрос без батчификации
                await self.process(user_id, next_node, result)


async def produce(agent, count):
    for i in range(count):
        user_id = f'user No {i}'
        message = f'message for {i}'
        print(f'producing {i}')
        await agent.register_msg(user_id, message)


async def run(count):
    testconfig = {
        'anno1': {'connector': SingleRequestConnector(), 'previous_nodes': set(), 'state_named_group': 'annotators'},
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

    agent = AsyncAgent(pipeline, state_storage, dict())
    consumers = []

    for node in pipeline.nodes.values():
        if not isinstance(node.connector, AioQueueConnector):
            continue
        caller = QueueListenerBatchifyer(node, agent.process)
        consumers.append(await caller.register_consumer())
        await node.connector.join()

    await produce(agent, count)

loop = asyncio.get_event_loop()
loop.set_debug(True)

loop.run_until_complete(run(5))
