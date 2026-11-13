from pathlib import Path
from mcap.reader import make_reader


def inspect_file(path):
    topics = {}
    schemas = {}
    with Path(path).open('rb') as stream:
        for schema, channel, message in make_reader(stream).iter_messages():
            topics[channel.topic] = topics.get(channel.topic, 0) + 1
            if schema:
                schemas[str(schema.id)] = {'name':schema.name, 'encoding':schema.encoding}
    return {'messages':sum(topics.values()), 'topics':topics, 'schemas':schemas}
