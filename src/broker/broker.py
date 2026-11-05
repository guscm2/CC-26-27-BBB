"""Single-broker routing with independent reliable UDP delivery per subscriber."""

from dataclasses import dataclass, field
import secrets
import selectors
import socket
import time

from config import BrokerConfig
from protocol import (
    Kind, Role, MAX_DATAGRAM, MAX_ID, ProtocolError, decode_datagram,
    encode_frame, matches, parse_ack, parse_hello, parse_publication,
    parse_subscription, parse_unsubscribe, pop_frame, publication_body,
    string, uint, welcome_body,
)

RETRY_INTERVAL = 0.25
MAX_CONTROL_BUFFER = 1024 * 1024
IO_BATCH = 64


@dataclass
class Pending:
    packet: bytes
    topic: str
    sent_at: float


@dataclass
class Client:
    sock: socket.socket
    address: tuple
    incoming: bytearray = field(default_factory=bytearray)
    outgoing: bytearray = field(default_factory=bytearray)
    role: Role | None = None
    client_id: str = ""
    session_id: int = 0
    endpoint: tuple | None = None
    ready: bool = False
    closing: bool = False
    subscriptions: dict = field(default_factory=dict)
    pending: dict = field(default_factory=dict)
    seen: set = field(default_factory=set)
    waiting_since: float | None = None
    next_transmission_id: int = 1


class Broker:
    def __init__(self, broker_id: str, config: BrokerConfig):
        if config.peers:
            raise ValueError("peer connections belong to phase 4 and are not implemented")
        self.broker_id = broker_id
        self.config = config
        self.selector = selectors.DefaultSelector()
        self.clients = {}
        self.sessions = {}
        self.running = False
        self.listener = None
        self.udp = None

    def run(self):
        deadline = time.monotonic() + self.config.duration
        self.running = True
        try:
            self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.listener.bind((self.config.bind, self.config.control_port))
            self.listener.listen()
            self.listener.setblocking(False)
            self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp.bind((self.config.bind, self.config.data_port))
            self.udp.setblocking(False)
            self.selector.register(self.listener, selectors.EVENT_READ, "listener")
            self.selector.register(self.udp, selectors.EVENT_READ, "udp")
            while self.running:
                now = time.monotonic()
                if now >= deadline:
                    break
                self._timers(now)
                # Bounded reads prevent a busy client starving other sessions.
                for key, mask in self.selector.select(min(0.05, deadline - now)):
                    if key.data == "listener":
                        self._accept()
                    elif key.data == "udp":
                        self._receive_datagrams()
                    else:
                        self._control_io(key.data, mask)
        finally:
            self.close()

    def stop(self):
        self.running = False

    def close(self):
        self.running = False
        for client in list(self.clients.values()):
            self._disconnect(client)
        self.selector.close()
        for sock in (self.listener, self.udp):
            if sock is not None:
                sock.close()

    def _accept(self):
        for _ in range(IO_BATCH):
            try:
                sock, address = self.listener.accept()
            except BlockingIOError:
                break
            sock.setblocking(False)
            client = Client(sock, address)
            self.clients[sock] = client
            self.selector.register(sock, selectors.EVENT_READ, client)

    def _disconnect(self, client):
        if client.sock not in self.clients:
            return
        self.selector.unregister(client.sock)
        del self.clients[client.sock]
        self.sessions.pop(client.session_id, None)
        client.pending.clear()
        client.seen.clear()
        client.subscriptions.clear()
        client.sock.close()

    def _interest(self, client):
        if client.sock in self.clients:
            mask = selectors.EVENT_READ
            if client.outgoing:
                mask |= selectors.EVENT_WRITE
            self.selector.modify(client.sock, mask, client)

    def _reply(self, client, kind, transmission_id, body=b""):
        client.outgoing.extend(encode_frame(kind, transmission_id, body))
        if len(client.outgoing) > MAX_CONTROL_BUFFER:
            self._disconnect(client)
        else:
            self._interest(client)

    def _control_io(self, client, mask):
        if client.sock not in self.clients:
            return
        try:
            if mask & selectors.EVENT_READ and not client.closing:
                try:
                    data = client.sock.recv(65536)
                except BlockingIOError:
                    data = None
                if data == b"":
                    self._disconnect(client)
                    return
                if data:
                    client.incoming.extend(data)
                    while not client.closing and client.sock in self.clients:
                        frame = pop_frame(client.incoming)
                        if frame is None:
                            break
                        self._control_message(client, frame)
            if client.sock not in self.clients:
                return
            if mask & selectors.EVENT_WRITE and client.outgoing:
                try:
                    sent = client.sock.send(client.outgoing)
                except BlockingIOError:
                    sent = 0
                del client.outgoing[:sent]
                if not client.outgoing:
                    if client.closing:
                        self._disconnect(client)
                        return
                    # WELCOME has actually been written before UDP is accepted.
                    client.ready = client.role is not None
                self._interest(client)
        except (OSError, ProtocolError):
            self._disconnect(client)

    def _control_message(self, client, frame):
        try:
            if client.role is None:
                if frame.kind != Kind.HELLO:
                    raise ProtocolError("HELLO must be the first message")
                role, port, client_id = parse_hello(frame.body)
                if role == Role.BROKER:
                    raise ProtocolError("peer sessions are not implemented")
                endpoint = (client.address[0], port)
                if any(c.client_id == client_id or c.endpoint == endpoint
                       for c in self.sessions.values()):
                    raise ProtocolError("client ID or UDP endpoint already in use")
                session_id = secrets.randbits(64)
                while not session_id or session_id in self.sessions:
                    session_id = secrets.randbits(64)
                client.role, client.endpoint, client.client_id = role, endpoint, client_id
                client.session_id = session_id
                self.sessions[session_id] = client
                self._reply(client, Kind.WELCOME, frame.transmission_id,
                            welcome_body(session_id, self.config.data_port, self.broker_id))
            elif frame.kind == Kind.SUBSCRIBE and client.role == Role.SUBSCRIBER:
                topic_filter, qos = parse_subscription(frame.body)
                client.subscriptions[topic_filter] = qos
                self._reply(client, Kind.OK, frame.transmission_id)
            elif frame.kind == Kind.UNSUBSCRIBE and client.role == Role.SUBSCRIBER:
                topic_filter = parse_unsubscribe(frame.body)
                client.subscriptions.pop(topic_filter, None)
                # Cancel retransmissions of topics with no remaining interest.
                for txid, pending in list(client.pending.items()):
                    if not any(matches(pending.topic, f) for f in client.subscriptions):
                        del client.pending[txid]
                if not client.pending:
                    client.waiting_since = None
                self._reply(client, Kind.OK, frame.transmission_id)
            elif frame.kind == Kind.BYE and not frame.body:
                client.closing = True
                client.ready = False
                client.pending.clear()
                self._reply(client, Kind.OK, frame.transmission_id)
            else:
                raise ProtocolError("message not allowed for this session")
        except ProtocolError as exc:
            self._reply(client, Kind.ERROR, frame.transmission_id, string(str(exc)))
            if client.role is None:
                client.closing = True

    def _send_udp(self, packet, endpoint):
        try:
            self.udp.sendto(packet, endpoint)
        except OSError:
            # UDP failures are retried by the sender's timer, never a blocking wait.
            pass

    def _receive_datagrams(self):
        for _ in range(IO_BATCH):
            try:
                data, address = self.udp.recvfrom(MAX_DATAGRAM + 1)
            except BlockingIOError:
                break
            except OSError:
                break
            try:
                frame = decode_datagram(data)
                if frame.kind == Kind.PUBLISH:
                    publication = parse_publication(frame.body)
                    client = self.sessions.get(publication.session_id)
                    if not self._valid_sender(client, address, Role.PUBLISHER):
                        continue
                    self._publish(client, frame.transmission_id, publication)
                elif frame.kind == Kind.ACK:
                    client = self.sessions.get(parse_ack(frame.body))
                    if not self._valid_sender(client, address, Role.SUBSCRIBER):
                        continue
                    if frame.transmission_id in client.pending:
                        del client.pending[frame.transmission_id]
                        client.waiting_since = time.monotonic() if client.pending else None
            except ProtocolError:
                # One malformed datagram must not kill unrelated sessions.
                continue

    @staticmethod
    def _valid_sender(client, address, role):
        return (client is not None and client.ready and not client.closing
                and client.role == role and client.endpoint == address)

    def _publish(self, publisher, transmission_id, publication):
        identity = (publication.topic, publication.message_id)
        if publication.qos == 1 or identity not in publisher.seen:
            now = time.monotonic()
            for subscriber in list(self.sessions.values()):
                if subscriber.role != Role.SUBSCRIBER or not subscriber.ready or subscriber.closing:
                    continue
                guarantees = [qos for topic_filter, qos in subscriber.subscriptions.items()
                              if matches(publication.topic, topic_filter)]
                if not guarantees:
                    continue
                if subscriber.next_transmission_id > MAX_ID:
                    # No ID reuse while an old ACK might still arrive.
                    self._disconnect(subscriber)
                    continue
                qos = min(publication.qos, max(guarantees))
                txid = subscriber.next_transmission_id
                subscriber.next_transmission_id += 1
                packet = encode_frame(Kind.DELIVER, txid, publication_body(
                    subscriber.session_id, qos, publication.message_id,
                    publication.topic, publication.payload))
                subscriber.pending[txid] = Pending(packet, publication.topic, now)
                if subscriber.waiting_since is None:
                    subscriber.waiting_since = now
                self._send_udp(packet, subscriber.endpoint)
            if publication.qos == 2:
                publisher.seen.add(identity)
        # A publisher ACK means accepted by the broker, not all recipients finished.
        self._send_udp(encode_frame(Kind.ACK, transmission_id,
                                   uint(publisher.session_id, 8)), publisher.endpoint)

    def _timers(self, now):
        for client in list(self.sessions.values()):
            if not client.pending:
                continue
            if now - client.waiting_since >= self.config.evict_after:
                self._disconnect(client)
                continue
            for pending in client.pending.values():
                if now - pending.sent_at >= RETRY_INTERVAL:
                    self._send_udp(pending.packet, client.endpoint)
                    pending.sent_at = now
