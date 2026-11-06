"""Deterministic protocol tests; no network access required."""

from collections import deque
import io
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from broker.broker import Broker, Client
from config import (
    Action, BrokerConfig, Flow, PublisherConfig, SubscriberConfig, Subscription,
    load_publisher_config, load_subscriber_config,
)
from protocol import (
    Frame, Kind, ProtocolError, Role, decode_datagram, encode_frame, parse_publication,
    publication_body, uint, welcome_body,
)
from publisher.publisher import Publisher
from subscriber.subscriber import Subscriber

TOPIC = "north/station7/temperature"


def publisher_config(qos=1, count=3, drain=3):
    return PublisherConfig(("127.0.0.1", 7310), "/tmp/pub.log", 0, drain,
                           (Flow(TOPIC, qos, count, 10),))


def subscriber_config(qos=1, actions=()):
    return SubscriberConfig(("127.0.0.1", 7310), "/tmp/sub.log", 10,
                            (Subscription("north/*", qos),), actions)


class ConfigTests(unittest.TestCase):
    def load(self, text, loader):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config"
            path.write_text(text)
            return loader(path)

    def test_publisher_multiple_flows_and_comments(self):
        cfg = self.load("""
            broker_control = 10.0.3.20:7310
            log = /tmp/pub.log
            start_after = 2
            drain = 4
            publish = north/station7/temperature qos=1 count=20 rate=10 # comment
            publish = north/station7/humidity qos=2 count=3 rate=0.5
        """, load_publisher_config)
        self.assertEqual(cfg.broker_control, ("10.0.3.20", 7310))
        self.assertEqual([f.qos for f in cfg.publish], [1, 2])
        self.assertEqual(cfg.publish[1].rate, 0.5)

    def test_subscriber_actions_and_empty_subscriptions(self):
        cfg = self.load("""
            broker_control = 127.0.0.1:1234
            log = /tmp/sub.log
            duration = 45
            at = 25 subscribe=north/* qos=2
            at = 30 unsubscribe=north/*
        """, load_subscriber_config)
        self.assertEqual(cfg.subscribe, ())
        self.assertEqual(cfg.actions[0], Action(25, "subscribe", "north/*", 2))
        self.assertEqual(cfg.actions[1].operation, "unsubscribe")

    def test_reject_invalid_values_without_defaults(self):
        base = "broker_control=127.0.0.1:7310\nlog=/tmp/pub.log\nstart_after=0\ndrain=1\n"
        bad = [
            base + "publish=north/* qos=1 count=1 rate=1",
            base + f"publish={TOPIC} qos=3 count=1 rate=1",
            base + f"publish={TOPIC} qos=1 count=1 rate=0",
            base + f"publish={TOPIC} qos=1 count=1 rate=nan",
            base + f"publish={TOPIC} qos=1 count=-1 rate=1",
            base + f"publish={TOPIC} qos=1 count=1",
            base + "drain=2", base.replace("drain=1\n", ""),
            base.replace("/tmp/pub.log", "pub.log"), base + "unknown=1",
        ]
        for text in bad:
            with self.subTest(text=text), self.assertRaises(ValueError):
                self.load(text, load_publisher_config)


class PublisherTests(unittest.TestCase):
    def make(self, config=None):
        publisher = Publisher("pub1", config or publisher_config())
        publisher.session_id = 11
        publisher.log = io.StringIO()
        publisher._send_udp = Mock()
        return publisher

    def test_start_after_concurrent_flows_and_ack(self):
        cfg = PublisherConfig(("127.0.0.1", 1), "/tmp/log", 2, 5,
                              (Flow(TOPIC, 1, 2, 2), Flow("north/s1/humidity", 2, 1, 1)))
        pub = self.make(cfg)
        pub._session_ready(100)
        pub._tick(101.9)
        pub._send_udp.assert_not_called()
        pub._tick(102)
        self.assertEqual(len(pub.pending), 2)
        pub._datagram(Frame(Kind.ACK, 1, uint(999, 8)))
        self.assertIn(1, pub.pending)  # Wrong session cannot acknowledge a message.
        for txid in (1, 2):
            pub._datagram(Frame(Kind.ACK, txid, uint(11, 8)))
        pub._tick(102.5)
        self.assertEqual(len(pub.log.getvalue().splitlines()), 3)
        pub._datagram(Frame(Kind.ACK, 3, uint(11, 8)))
        self.assertFalse(pub.running)  # No need to wait out drain after all ACKs.

    def test_retry_same_packet_without_extra_log_and_drain_limit(self):
        pub = self.make(publisher_config(count=1, drain=1))
        pub._session_ready(0)
        pub._tick(0)
        packet = pub._send_udp.call_args.args[0]
        pub._tick(0.25)
        self.assertEqual(pub._send_udp.call_args.args[0], packet)
        self.assertEqual(len(pub.log.getvalue().splitlines()), 1)
        pub._tick(1)
        self.assertFalse(pub.running)
        self.assertEqual(pub._send_udp.call_count, 2)

    def test_only_unacknowledged_packets_are_retransmitted(self):
        pub = self.make()
        pub._session_ready(0)
        pub._tick(0.2)
        pub._datagram(Frame(Kind.ACK, 2, uint(11, 8)))
        pub._send_udp.reset_mock()
        pub._tick(0.5)
        ids = [decode_datagram(call.args[0]).transmission_id
               for call in pub._send_udp.call_args_list]
        self.assertEqual(ids, [1, 3])

    def test_welcome_required_before_publication(self):
        pub = self.make()
        self.assertEqual(pub.schedule, [])
        pub.requests[1] = (Kind.HELLO, 10)
        with patch("client.time.monotonic", return_value=50):
            pub._control_message(Frame(Kind.WELCOME, 1, welcome_body(11, 8888, "broker1")))
        self.assertTrue(pub.ready)
        self.assertEqual(pub.data_address, ("127.0.0.1", 8888))
        self.assertEqual(pub.schedule[0][0], 50)


class SubscriberTests(unittest.TestCase):
    def make(self, qos=1, actions=()):
        sub = Subscriber("sub1", subscriber_config(qos, actions))
        sub.session_id = 22
        sub.log = io.StringIO()
        sub._send_udp = Mock()
        sub._request = Mock()
        sub._session_ready(sub.started_at)
        return sub

    def deliver(self, sub, qos, txid=1, message_id=42, session=22, topic=TOPIC):
        sub._datagram(Frame(Kind.DELIVER, txid,
                            publication_body(session, qos, message_id, topic, b"30")))

    def test_g1_logs_every_arrival_g2_logs_once_but_always_acks(self):
        for qos in (1, 2):
            with self.subTest(qos=qos):
                sub = self.make(qos)
                self.deliver(sub, qos)
                self.deliver(sub, qos)
                self.deliver(sub, qos, txid=2)  # Same publication, different hop ID.
                self.assertEqual(len(sub.log.getvalue().splitlines()), 3 if qos == 1 else 1)
                self.assertEqual(sub._send_udp.call_count, 3)
                self.assertEqual(len(sub.seen), 0 if qos == 1 else 1)

    def test_delivery_qos_overrides_subscription_qos(self):
        sub = self.make(qos=2)
        self.deliver(sub, qos=1)
        self.deliver(sub, qos=1)
        self.assertEqual(len(sub.log.getvalue().splitlines()), 2)

    def test_invalid_session_and_truncated_data_do_not_deliver(self):
        sub = self.make()
        self.deliver(sub, 1, session=999)
        self.assertEqual(sub.log.getvalue(), "")
        sub._send_udp.assert_not_called()
        with self.assertRaises(ProtocolError):
            sub._datagram(Frame(Kind.DELIVER, 1, b""))

    def test_actions_use_process_start_not_session_start(self):
        actions = (Action(3, "unsubscribe", "north/*"), Action(5, "subscribe", "north/*", 2))
        sub = self.make(actions=actions)
        sub._tick(sub.started_at + 3)
        self.assertEqual(sub.subscriptions, {})
        self.deliver(sub, 1)
        self.assertEqual(sub.log.getvalue(), "")
        self.assertEqual(sub._send_udp.call_count, 1)  # Clear broker's pending data.
        sub._tick(sub.started_at + 5)
        self.assertEqual(sub.subscriptions, {"north/*": 2})
        sub._disconnected()
        self.assertFalse(sub.running)

    def test_log_is_flushed_before_ack(self):
        sub = self.make()
        events = []
        sub.log = Mock()
        sub.log.write.side_effect = lambda line: events.append("write")
        sub.log.flush.side_effect = lambda: events.append("flush")
        sub._send_udp.side_effect = lambda packet: events.append("ack")
        self.deliver(sub, 1)
        self.assertEqual(events, ["write", "flush", "ack"])


class BrokerCompatibilityTests(unittest.TestCase):
    def test_loss_of_publications_deliveries_and_acks(self):
        """Use the real broker handlers and real binary packets with injected loss."""
        for qos in (1, 2):
            with self.subTest(qos=qos):
                self.simulate(qos)

    def simulate(self, qos):
        broker = Broker("b1", BrokerConfig("127.0.0.1", 1, 2, 10, 5))
        self.addCleanup(broker.selector.close)
        pub = Publisher("pub1", publisher_config(qos))
        sub = Subscriber("sub1", subscriber_config(qos))
        pub.session_id, sub.session_id = 11, 22
        pub.log, sub.log = io.StringIO(), io.StringIO()
        pub_endpoint, sub_endpoint = ("127.0.0.1", 1001), ("127.0.0.1", 1002)
        publisher = Client(Mock(), pub_endpoint, role=Role.PUBLISHER,
                           session_id=11, endpoint=pub_endpoint, ready=True)
        subscriber = Client(Mock(), sub_endpoint, role=Role.SUBSCRIBER,
                            session_id=22, endpoint=sub_endpoint, ready=True,
                            subscriptions={"north/*": qos, "north/station7/*": qos})
        broker.sessions = {11: publisher, 22: subscriber}
        sub._request = Mock()
        sub._session_ready(sub.started_at)
        pub._session_ready(0)
        queue = deque()
        dropped = set()

        def send(direction, packet, address):
            # Drop one initial packet of every kind in each direction, including ACKs.
            key = (direction, decode_datagram(packet).kind)
            if key not in dropped:
                dropped.add(key)
                return
            queue.append((direction, packet, address))

        pub._send_udp = lambda packet: send("up", packet, pub_endpoint)
        sub._send_udp = lambda packet: send("up", packet, sub_endpoint)
        broker._send_udp = lambda packet, endpoint: send("down", packet, endpoint)
        for step in range(60):
            now = step / 20
            with patch("broker.broker.time.monotonic", return_value=now):
                if pub.running:
                    pub._tick(now)
                broker._timers(now)
                while queue:
                    direction, packet, address = queue.popleft()
                    if direction == "up":
                        broker.udp = Mock()
                        broker.udp.recvfrom.side_effect = [(packet, address), BlockingIOError()]
                        broker._receive_datagrams()
                    elif address == pub_endpoint:
                        pub._datagram(decode_datagram(packet))
                    else:
                        sub._datagram(decode_datagram(packet))
        publications = [row.split()[2:] for row in pub.log.getvalue().splitlines()]
        deliveries = [row.split()[2:] for row in sub.log.getvalue().splitlines()]
        self.assertEqual(len(publications), 3)
        self.assertEqual(set(map(tuple, publications)), set(map(tuple, deliveries)))
        if qos == 2:
            self.assertEqual(len(deliveries), 3)
        else:
            self.assertGreater(len(deliveries), 3)
        self.assertFalse(pub.pending)
        self.assertFalse(subscriber.pending)
        self.assertFalse(pub.running)


if __name__ == "__main__":
    unittest.main()
