"""Binary SubControl/EventCast wire format; see docs/protocol.md."""

from dataclasses import dataclass
from enum import IntEnum

MAGIC = b"CC"
VERSION = 1
HEADER_SIZE = 16
MAX_DATAGRAM = 65507
MAX_BODY = MAX_DATAGRAM - HEADER_SIZE
MAX_ID = (1 << 64) - 1


class ProtocolError(ValueError):
    pass


class Kind(IntEnum):
    HELLO = 1
    WELCOME = 2
    SUBSCRIBE = 3
    UNSUBSCRIBE = 4
    OK = 5
    ERROR = 6
    BYE = 7
    PUBLISH = 16
    DELIVER = 17
    ACK = 18


class Role(IntEnum):
    PUBLISHER = 1
    SUBSCRIBER = 2
    BROKER = 3


def uint(value, size):
    if not isinstance(value, int) or not 0 <= value < 1 << (size * 8):
        raise ProtocolError("integer outside field range")
    return value.to_bytes(size, "big")


def valid_name(value):
    return bool(value) and not any(c.isspace() or ord(c) < 32 for c in value)


def string(value):
    data = value.encode("utf-8")
    return uint(len(data), 2) + data


class Reader:
    def __init__(self, data):
        self.data = data
        self.offset = 0

    def take(self, size):
        end = self.offset + size
        if end > len(self.data):
            raise ProtocolError("truncated body")
        value = self.data[self.offset:end]
        self.offset = end
        return value

    def number(self, size):
        return int.from_bytes(self.take(size), "big")

    def text(self):
        try:
            return self.take(self.number(2)).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ProtocolError("invalid UTF-8") from exc

    def finish(self):
        if self.offset != len(self.data):
            raise ProtocolError("unexpected trailing bytes")


@dataclass(frozen=True)
class Frame:
    kind: Kind
    transmission_id: int
    body: bytes


def encode_frame(kind, transmission_id, body=b""):
    if len(body) > MAX_BODY:
        raise ProtocolError("body too large")
    return (MAGIC + uint(VERSION, 1) + uint(kind, 1)
            + uint(len(body), 4) + uint(transmission_id, 8) + body)


def read_header(data):
    if len(data) < HEADER_SIZE:
        raise ProtocolError("truncated header")
    if data[:2] != MAGIC or data[2] != VERSION:
        raise ProtocolError("unsupported protocol")
    try:
        kind = Kind(data[3])
    except ValueError as exc:
        raise ProtocolError("unknown message type") from exc
    size = int.from_bytes(data[4:8], "big")
    if size > MAX_BODY:
        raise ProtocolError("body too large")
    return kind, size, int.from_bytes(data[8:16], "big")


def decode_datagram(data):
    kind, size, transmission_id = read_header(data)
    if len(data) != HEADER_SIZE + size:
        raise ProtocolError("datagram length mismatch")
    return Frame(kind, transmission_id, data[HEADER_SIZE:])


def pop_frame(buffer):
    """Consume one complete TCP frame, leaving partial/coalesced frames intact."""
    if len(buffer) < HEADER_SIZE:
        return None
    kind, size, transmission_id = read_header(buffer)
    end = HEADER_SIZE + size
    if len(buffer) < end:
        return None
    frame = Frame(kind, transmission_id, bytes(buffer[HEADER_SIZE:end]))
    del buffer[:end]
    return frame


def hello_body(role, data_port, client_id):
    return uint(role, 1) + uint(data_port, 2) + string(client_id)


def parse_hello(body):
    reader = Reader(body)
    try:
        role = Role(reader.number(1))
    except ValueError as exc:
        raise ProtocolError("unknown role") from exc
    port, client_id = reader.number(2), reader.text()
    reader.finish()
    if not port or not valid_name(client_id):
        raise ProtocolError("invalid UDP port or client ID")
    return role, port, client_id


def welcome_body(session_id, data_port, broker_id):
    return uint(session_id, 8) + uint(data_port, 2) + string(broker_id)


def parse_welcome(body):
    reader = Reader(body)
    result = reader.number(8), reader.number(2), reader.text()
    reader.finish()
    return result


def validate_topic(topic, is_filter=False):
    parts = topic.split("/")
    if not valid_name(topic) or any(not part for part in parts):
        raise ProtocolError("invalid topic")
    if is_filter and parts[-1] == "*":
        if len(parts) not in (2, 3) or any("*" in p for p in parts[:-1]):
            raise ProtocolError("invalid subscription filter")
    elif len(parts) != 3 or "*" in topic:
        raise ProtocolError("topic must have three levels and no wildcards")


def matches(topic, topic_filter):
    return topic.startswith(topic_filter[:-1]) if topic_filter.endswith("/*") else topic == topic_filter


def subscribe_body(topic_filter, qos):
    return uint(qos, 1) + string(topic_filter)


def parse_subscription(body):
    reader = Reader(body)
    qos, topic_filter = reader.number(1), reader.text()
    reader.finish()
    validate_topic(topic_filter, is_filter=True)
    if qos not in (1, 2):
        raise ProtocolError("QoS must be 1 or 2")
    return topic_filter, qos


def parse_unsubscribe(body):
    reader = Reader(body)
    topic_filter = reader.text()
    reader.finish()
    validate_topic(topic_filter, is_filter=True)
    return topic_filter


@dataclass(frozen=True)
class Publication:
    session_id: int
    qos: int
    message_id: int
    topic: str
    payload: bytes


def publication_body(session_id, qos, message_id, topic, payload):
    return (uint(session_id, 8) + uint(qos, 1) + uint(message_id, 8)
            + string(topic) + uint(len(payload), 4) + payload)


def parse_publication(body):
    reader = Reader(body)
    session_id, qos, message_id = reader.number(8), reader.number(1), reader.number(8)
    topic = reader.text()
    payload = reader.take(reader.number(4))
    reader.finish()
    validate_topic(topic)
    if qos not in (1, 2):
        raise ProtocolError("QoS must be 1 or 2")
    return Publication(session_id, qos, message_id, topic, payload)


def parse_ack(body):
    reader = Reader(body)
    session_id = reader.number(8)
    reader.finish()
    return session_id
