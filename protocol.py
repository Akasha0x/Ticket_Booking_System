"""Wire protocol: newline-delimited JSON messages over TCP."""
import json

MAX_LINE = 64 * 1024  # reject oversized messages (basic DoS protection)


class ProtocolError(Exception):
    pass


def encode(msg: dict) -> bytes:
    return (json.dumps(msg, separators=(",", ":")) + "\n").encode("utf-8")


def decode(line: bytes) -> dict:
    if len(line) > MAX_LINE:
        raise ProtocolError("message too large")
    try:
        msg = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"malformed message: {exc}") from exc
    if not isinstance(msg, dict):
        raise ProtocolError("message must be a JSON object")
    return msg