"""Concurrent configured flows and selective retransmission until ACK or drain."""

from dataclasses import dataclass
import heapq

from client import NetworkClient, RETRY_INTERVAL, IO_BATCH
from protocol import (
    Kind, Role, MAX_ID, ProtocolError, encode_frame, parse_ack, publication_body,
)


@dataclass
class Pending:
    packet: bytes
    sent_at: float


class Publisher(NetworkClient):
    def __init__(self, client_id, config):
        super().__init__(client_id, config, Role.PUBLISHER)
        self.schedule = []
        self.pending = {}
        self.next_message_id = 1
        self.next_transmission_id = 1
        self.drain_deadline = None

    def _session_ready(self, now):
        start = now + self.config.start_after
        self.schedule = [(start, index, 0) for index, flow in enumerate(self.config.publish)
                         if flow.count]
        heapq.heapify(self.schedule)
        if not self.schedule:
            self.stop()

    def _tick(self, now):
        if self.drain_deadline is not None and now >= self.drain_deadline:
            self.stop()
            return
        for _ in range(IO_BATCH):
            if not self.schedule or self.schedule[0][0] > now:
                break
            due, index, generated = heapq.heappop(self.schedule)
            flow = self.config.publish[index]
            if self.next_message_id > MAX_ID or self.next_transmission_id > MAX_ID:
                raise ProtocolError("publication ID exhausted; start a new session")
            message_id, txid = self.next_message_id, self.next_transmission_id
            self.next_message_id += 1
            self.next_transmission_id += 1
            payload = f"reading={generated + 1}".encode("utf-8")
            packet = encode_frame(Kind.PUBLISH, txid, publication_body(
                self.session_id, flow.qos, message_id, flow.topic, payload))
            self.pending[txid] = Pending(packet, now)
            self._send_udp(packet)
            # Retries reuse the bytes and do not add another publication log line.
            self._write_log(flow.topic, message_id)
            if generated + 1 < flow.count:
                heapq.heappush(self.schedule, (due + 1 / flow.rate, index, generated + 1))
        if not self.schedule and self.drain_deadline is None:
            self.drain_deadline = now + self.config.drain
        if not self.schedule and (not self.pending or now >= self.drain_deadline):
            self.stop()
            return
        for pending in self.pending.values():
            if now - pending.sent_at >= RETRY_INTERVAL:
                self._send_udp(pending.packet)
                pending.sent_at = now

    def _datagram(self, frame):
        if frame.kind == Kind.ACK and parse_ack(frame.body) == self.session_id:
            self.pending.pop(frame.transmission_id, None)
            if not self.schedule and not self.pending:
                self.stop()

    def _disconnected(self):
        if self.schedule or self.pending or not self.ready:
            raise ConnectionError("broker closed the session before publishing completed")
        self.stop()
