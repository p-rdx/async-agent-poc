from collections import defaultdict
from pprint import pprint


class Node:
    def __init__(self, name, connector, names_previous_nodes, state_named_group, batch_size=0, category=None):
        self.name = name
        self.batch_size = batch_size
        self.connector = connector
        self.category = None
        self.state_named_group = state_named_group
        self.names_previous_nodes = names_previous_nodes
        self.previous_nodes = set()
        self.next_nodes = set()


class Pipeline:
    def __init__(self, nodes):
        self.nodes = dict()
        for node in nodes:
            self.add_node(node)
        wrong_links = self.process_node_names()
        if wrong_links:
            print('wrong links in config were detected: ', dict(wrong_links))

    def add_node(self, node):
        if not self.nodes.get(node.name, None):
            self.nodes[node.name] = node
        else:
            raise ValueError(f'node {node} already existed')

    def process_node_names(self):
        wrong_names = defaultdict(list)
        for node in self.nodes.values():
            for name_prev_node in node.names_previous_nodes:
                prev_node = self.nodes.get(name_prev_node, None)
                if not prev_node:
                    wrong_names[node.name].append(name_prev_node)
                    continue
                node.previous_nodes.add(prev_node)
                prev_node.next_nodes.add(node)
        return wrong_names  # + проверка конфига на тему того, правильно ли были внесены все скиллы, можно обернуть в ошибку

    def get_next_nodes(self, done=None, waiting=None):
        if not done:
            done = set()
        if not waiting:
            waiting = set()
        removed_names = waiting | done
        for name, node in self.nodes.items():
            if not {i.name for i in node.previous_nodes} <= done:
                removed_names.add(name)

        return {name: node for name, node in self.nodes.items() if name not in removed_names}

    def get_state_named_groups(self):
        return [i.state_named_group for i in self.nodes.values()]
