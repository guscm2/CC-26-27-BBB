"""Shared TCP session and UDP event loop for publishers and subscribers."""

import errno
import selectors
import socket
import time

from protocol import (
    Kind, MAX_DATAGRAM, MAX_ID, ProtocolError, Reader, decode_datagram,
    encode_frame, hello_body, parse_welcome, pop_frame, valid_name,
)

CONNECT_TIMEOUT = 10.0
CONTROL_TIMEOUT = 10.0
CONNECT_RETRY = 0.1
RETRY_INTERVAL = 0.25
IO_BATCH = 64
MAX_CONTROL_BUFFER = 1024 * 1024


class NetworkClient:
    """One process, no threads; subclass hooks never block awaiting an ACK."""

    def __init__(self, client_id, config, role):
        self.client_id, self.config, self.role = client_id, config, role
        self.started_at = time.monotonic()
        self.deadline = float("inf")
        self.running = True
        self.ready = False
        self.connected = False
        self.session_id = 0
        self.data_address = None
        self.selector = None
        self.tcp = self.udp = self.log = None
        self.incoming = bytearray()
        self.outgoing = bytearray()
        self.requests = {}
        self.next_control_id = 1
        self.next_connect_at = self.started_at

    def stop(self):
        self.running = False

    def run(self):
        try:
            self.log = open(self.config.log, "w", encoding="utf-8")
            self.selector = selectors.DefaultSelector()
            while self.running:
                now = time.monotonic()
                if now >= self.deadline:
                    break
                if not self.ready and now >= self.started_at + CONNECT_TIMEOUT:
                    raise TimeoutError("broker session was not established within 10 seconds")
                if self.tcp is None and now >= self.next_connect_at:
                    self._connect()
                for _, expires in self.requests.values():
                    if now >= expires:
                        raise TimeoutError("broker did not reply to a control request")
                if self.ready:
                    self._tick(now)
                if not self.running:
                    break
                timeout = max(0, min(0.02, self.deadline - time.monotonic()))
                events = self.selector.select(timeout)
                # Observe a closed control session before any queued UDP delivery.
                events.sort(key=lambda item: item[0].data != "tcp")
                for key, mask in events:
                    if not self.running or time.monotonic() >= self.deadline:
                        break
                    if key.data == "tcp":
                        self._control_io(mask)
                    elif self.ready:
                        self._receive_datagrams()
        finally:
            self.close()

    def _connect(self):
        self.tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp.setblocking(False)
        result = self.tcp.connect_ex(self.config.broker_control)
        if result not in (0, errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY):
            self.tcp.close()
            self.tcp = None
            self.next_connect_at = time.monotonic() + CONNECT_RETRY
            return
        self.selector.register(self.tcp, selectors.EVENT_WRITE, "tcp")

    def _interest(self):
        mask = selectors.EVENT_READ
        if self.outgoing:
            mask |= selectors.EVENT_WRITE
        self.selector.modify(self.tcp, mask, "tcp")

    def _request(self, kind, body=b""):
        if self.next_control_id > MAX_ID:
            raise ProtocolError("control ID exhausted; start a new session")
        txid = self.next_control_id
        self.next_control_id += 1
        self.outgoing.extend(encode_frame(kind, txid, body))
        if len(self.outgoing) > MAX_CONTROL_BUFFER:
            raise ProtocolError("too many queued control requests")
        self.requests[txid] = (kind, time.monotonic() + CONTROL_TIMEOUT)
        self._interest()

    def _control_io(self, mask):
        if not self.connected:
            error = self.tcp.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if error:
                self.selector.unregister(self.tcp)
                self.tcp.close()
                self.tcp = None
                self.next_connect_at = time.monotonic() + CONNECT_RETRY
                return
            self.connected = True
            self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp.bind((self.tcp.getsockname()[0], 0))
            self.udp.setblocking(False)
            self.selector.register(self.udp, selectors.EVENT_READ, "udp")
            self._request(Kind.HELLO, hello_body(
                self.role, self.udp.getsockname()[1], self.client_id))
        if mask & selectors.EVENT_READ:
            try:
                data = self.tcp.recv(65536)
            except BlockingIOError:
                data = None
            if data == b"":
                self._disconnected()
                return
            if data:
                self.incoming.extend(data)
                while True:
                    frame = pop_frame(self.incoming)
                    if frame is None:
                        break
                    self._control_message(frame)
        if mask & selectors.EVENT_WRITE and self.outgoing:
            try:
                sent = self.tcp.send(self.outgoing)
                del self.outgoing[:sent]
            except BlockingIOError:
                pass
        self._interest()

    def _control_message(self, frame):
        request = self.requests.pop(frame.transmission_id, None)
        if request is None:
            raise ProtocolError("unexpected control reply ID")
        if frame.kind == Kind.ERROR:
            reader = Reader(frame.body)
            message = reader.text()
            reader.finish()
            raise ProtocolError("broker rejected request: " + message)
        if request[0] == Kind.HELLO:
            if frame.kind != Kind.WELCOME:
                raise ProtocolError("expected WELCOME")
            session_id, data_port, broker_id = parse_welcome(frame.body)
            if not session_id or not data_port or not valid_name(broker_id):
                raise ProtocolError("invalid WELCOME")
            self.session_id = session_id
            self.data_address = (self.config.broker_control[0], data_port)
            self.ready = True
            self._session_ready(time.monotonic())
        elif frame.kind != Kind.OK or frame.body:
            raise ProtocolError("expected empty OK")

    def _send_udp(self, packet):
        try:
            self.udp.sendto(packet, self.data_address)
        except OSError:
            # Lost/temporarily unsendable data is retried; ACK loss causes a resend.
            pass

    def _receive_datagrams(self):
        for _ in range(IO_BATCH):
            if not self.running or time.monotonic() >= self.deadline:
                break
            try:
                raw, source = self.udp.recvfrom(MAX_DATAGRAM + 1)
            except OSError:
                break
            if source != self.data_address:
                continue
            try:
                self._datagram(decode_datagram(raw))
            except ProtocolError:
                continue

    def _write_log(self, topic, message_id):
        self.log.write(f"{time.time_ns() // 1_000_000} {self.client_id} {topic} {message_id}\n")
        self.log.flush()

    def _disconnected(self):
        # An evicted subscriber must not reconnect or consume queued datagrams.
        self.stop()

    def close(self):
        if self.tcp is not None and self.ready:
            try:
                # Preserve framing if a control message was only partially sent.
                goodbye = encode_frame(Kind.BYE, min(self.next_control_id, MAX_ID))
                self.tcp.send(self.outgoing + goodbye)
            except OSError:
                pass
        for sock in (self.tcp, self.udp):
            if sock is not None:
                sock.close()
        if self.selector is not None:
            self.selector.close()
        if self.log is not None:
            self.log.close()

    def _session_ready(self, now):
        raise NotImplementedError

    def _tick(self, now):
        raise NotImplementedError

    def _datagram(self, frame):
        raise NotImplementedError
