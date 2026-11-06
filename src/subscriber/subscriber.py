"""Subscriptions, scheduled changes, per-delivery QoS and immediate ACKs."""

from client import NetworkClient
from protocol import (
    Kind, Role, encode_frame, matches, parse_publication, string, subscribe_body, uint,
)


class Subscriber(NetworkClient):
    def __init__(self, client_id, config):
        super().__init__(client_id, config, Role.SUBSCRIBER)
        self.deadline = self.started_at + config.duration
        self.subscriptions = {}
        self.seen = set()
        # Stable sort preserves config order for simultaneous actions.
        self.actions = sorted(config.actions, key=lambda action: action.at)
        self.next_action = 0

    def _session_ready(self, now):
        for subscription in self.config.subscribe:
            self._subscribe(subscription.topic_filter, subscription.qos)
        self._tick(now)

    def _subscribe(self, topic_filter, qos):
        self._request(Kind.SUBSCRIBE, subscribe_body(topic_filter, qos))
        self.subscriptions[topic_filter] = qos

    def _tick(self, now):
        while self.next_action < len(self.actions):
            action = self.actions[self.next_action]
            if self.started_at + action.at > now:
                break
            self.next_action += 1
            if action.operation == "subscribe":
                self._subscribe(action.topic_filter, action.qos)
            else:
                self._request(Kind.UNSUBSCRIBE, string(action.topic_filter))
                self.subscriptions.pop(action.topic_filter, None)

    def _datagram(self, frame):
        if frame.kind != Kind.DELIVER:
            return
        publication = parse_publication(frame.body)
        if publication.session_id != self.session_id:
            return
        interested = any(matches(publication.topic, f) for f in self.subscriptions)
        identity = (publication.topic, publication.message_id)
        if interested and (publication.qos == 1 or identity not in self.seen):
            self._write_log(publication.topic, publication.message_id)
            if publication.qos == 2:
                self.seen.add(identity)
        # ACK duplicates and in-flight data cancelled by a recent unsubscribe too.
        # Flush precedes ACK: acceptance must mean the log line already exists.
        self._send_udp(encode_frame(Kind.ACK, frame.transmission_id,
                                    uint(self.session_id, 8)))
