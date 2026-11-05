"""Configuration for the single-broker implementation."""

from dataclasses import dataclass
import ipaddress
import math


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
