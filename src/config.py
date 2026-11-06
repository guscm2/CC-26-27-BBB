"""Configuration for the single-broker implementation."""

from dataclasses import dataclass
import ipaddress
import math
from pathlib import Path

from protocol import validate_topic


@dataclass(frozen=True)
class BrokerConfig:
    bind: str
    control_port: int
    data_port: int
    duration: float
    evict_after: float
    peers: tuple = ()


def port_number(value):
    port = int(value)
    if not 1 <= port <= 65535:
        raise ValueError("port must be between 1 and 65535")
    return port


def seconds(value, key):
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"{key} must be finite and non-negative")
    return result


def load_broker_config(path):
    required = {"bind", "control_port", "data_port", "duration", "evict_after"}
    values, peers = {}, []
    with open(path, encoding="utf-8") as source:
        for number, raw in enumerate(source, 1):
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            if "=" not in line:
                raise ValueError(f"line {number}: expected key = value")
            key, value = (part.strip() for part in line.split("=", 1))
            if not value or len(value.split()) != 1:
                raise ValueError(f"line {number}: expected one value")
            if key == "peer":
                address, port = value.rsplit(":", 1)
                peers.append((str(ipaddress.IPv4Address(address)), port_number(port)))
            elif key not in required or key in values:
                raise ValueError(f"line {number}: unknown or repeated key {key}")
            else:
                values[key] = value
    missing = required - values.keys()
    if missing:
        raise ValueError("missing keys: " + ", ".join(sorted(missing)))
    return BrokerConfig(
        bind=str(ipaddress.IPv4Address(values["bind"])),
        control_port=port_number(values["control_port"]),
        data_port=port_number(values["data_port"]),
        duration=seconds(values["duration"], "duration"),
        evict_after=seconds(values["evict_after"], "evict_after"),
        peers=tuple(peers),
    )


@dataclass(frozen=True)
class Flow:
    topic: str
    qos: int
    count: int
    rate: float


@dataclass(frozen=True)
class Subscription:
    topic_filter: str
    qos: int


@dataclass(frozen=True)
class Action:
    at: float
    operation: str
    topic_filter: str
    qos: int | None = None


@dataclass(frozen=True)
class PublisherConfig:
    broker_control: tuple
    log: str
    start_after: float
    drain: float
    publish: tuple[Flow, ...]


@dataclass(frozen=True)
class SubscriberConfig:
    broker_control: tuple
    log: str
    duration: float
    subscribe: tuple[Subscription, ...]
    actions: tuple[Action, ...]


def _client_values(path, required, repeated):
    values = {}
    lists = {key: [] for key in repeated}
    with open(path, encoding="utf-8") as source:
        for number, raw in enumerate(source, 1):
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            if "=" not in line:
                raise ValueError(f"line {number}: expected key = value")
            key, value = (part.strip() for part in line.split("=", 1))
            if not value:
                raise ValueError(f"line {number}: empty value")
            if key in lists:
                lists[key].append(value)
            elif key in required and key not in values:
                values[key] = value
            else:
                raise ValueError(f"line {number}: unknown or repeated key {key}")
    missing = required - values.keys()
    if missing:
        raise ValueError("missing keys: " + ", ".join(sorted(missing)))
    address, port = values["broker_control"].rsplit(":", 1)
    values["broker_control"] = (str(ipaddress.IPv4Address(address)), port_number(port))
    if not Path(values["log"]).is_absolute():
        raise ValueError("log must be an absolute path")
    return values, lists


def _options(value):
    first, *terms = value.split()
    options = {}
    for term in terms:
        if "=" not in term:
            raise ValueError("expected name=value option")
        name, option = term.split("=", 1)
        if name in options or not option:
            raise ValueError("duplicate or empty option: " + name)
        options[name] = option
    return first, options


def _qos(value):
    if value not in ("1", "2"):
        raise ValueError("qos must be 1 or 2")
    return int(value)


def load_publisher_config(path):
    values, lists = _client_values(
        path, {"broker_control", "log", "start_after", "drain"}, {"publish"})
    flows = []
    for value in lists["publish"]:
        topic, options = _options(value)
        if options.keys() != {"qos", "count", "rate"}:
            raise ValueError("publish requires qos, count and rate")
        validate_topic(topic)
        count = int(options["count"])
        rate = seconds(options["rate"], "rate")
        if count < 0 or rate == 0:
            raise ValueError("count must be non-negative and rate must be positive")
        flows.append(Flow(topic, _qos(options["qos"]), count, rate))
    return PublisherConfig(values["broker_control"], values["log"],
                           seconds(values["start_after"], "start_after"),
                           seconds(values["drain"], "drain"), tuple(flows))


def load_subscriber_config(path):
    values, lists = _client_values(
        path, {"broker_control", "log", "duration"}, {"subscribe", "at"})
    subscriptions, actions = [], []
    for value in lists["subscribe"]:
        topic_filter, options = _options(value)
        if options.keys() != {"qos"}:
            raise ValueError("subscribe requires qos")
        validate_topic(topic_filter, is_filter=True)
        subscriptions.append(Subscription(topic_filter, _qos(options["qos"])))
    for value in lists["at"]:
        when, options = _options(value)
        if options.keys() == {"subscribe", "qos"}:
            operation, qos = "subscribe", _qos(options["qos"])
        elif options.keys() == {"unsubscribe"}:
            operation, qos = "unsubscribe", None
        else:
            raise ValueError("at requires subscribe=<filter> qos=<1|2> or unsubscribe=<filter>")
        validate_topic(options[operation], is_filter=True)
        actions.append(Action(seconds(when, "at"), operation, options[operation], qos))
    return SubscriberConfig(values["broker_control"], values["log"],
                            seconds(values["duration"], "duration"),
                            tuple(subscriptions), tuple(actions))
