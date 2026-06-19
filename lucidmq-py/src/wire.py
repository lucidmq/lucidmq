import json
import time
from typing import Any, Optional, Union

TextLike = Union[str, bytes, bytearray]


def topic_request_describe(topic_name: str) -> bytes:
    req = {"type": "TopicRequest", "topic_name": topic_name, "request_type": "Describe"}
    return create_message_frame(encode_message(req))


def topic_request_create(topic_name: str) -> bytes:
    req = {"type": "TopicRequest", "topic_name": topic_name, "request_type": "Create"}
    return create_message_frame(encode_message(req))


def topic_request_delete(topic_name: str) -> bytes:
    req = {"type": "TopicRequest", "topic_name": topic_name, "request_type": "Delete"}
    return create_message_frame(encode_message(req))


def topic_request_all() -> bytes:
    req = {"type": "TopicRequest", "topic_name": "placeholder", "request_type": "All"}
    return create_message_frame(encode_message(req))


def produce_request(
    topic_name: str,
    source_id: TextLike,
    payload: Any,
    parent_source_id: Optional[TextLike] = None,
) -> bytes:
    timestamp = time.time_ns() // 1_000_000
    message = {
        "timestamp": timestamp,
        "source_id": text_value(source_id),
        "payload": payload_value(payload),
    }
    if parent_source_id is not None:
        message["parent_source_id"] = text_value(parent_source_id)

    req = {
        "type": "ProduceRequest",
        "topic_name": topic_name,
        "messages": [message],
    }
    return create_message_frame(encode_message(req))


def produce_upsert_request(
    topic_name: str,
    source_id: TextLike,
    payload: Any,
    parent_source_id: Optional[TextLike] = None,
) -> bytes:
    return produce_request(topic_name, source_id, payload, parent_source_id)


def state_request_get(topic_name: str, source_id: TextLike) -> bytes:
    req = {
        "type": "StateRequest",
        "topic_name": topic_name,
        "action": "Get",
        "source_id": text_value(source_id),
    }
    return create_message_frame(encode_message(req))


def state_request_get_children(topic_name: str, parent_source_id: TextLike) -> bytes:
    req = {
        "type": "StateRequest",
        "topic_name": topic_name,
        "action": "GetChildren",
        "parent_source_id": text_value(parent_source_id),
    }
    return create_message_frame(encode_message(req))


def state_request_scan_current(topic_name: str) -> bytes:
    req = {
        "type": "StateRequest",
        "topic_name": topic_name,
        "action": "ScanCurrent",
    }
    return create_message_frame(encode_message(req))


def consume_request(topic_name: str, consumer_group: str, timeout: int) -> bytes:
    req = {
        "type": "ConsumeRequest",
        "topic_name": topic_name,
        "consumer_group": consumer_group,
        "timeout": timeout,
    }
    return create_message_frame(encode_message(req))


def encode_message(message: dict) -> bytes:
    return json.dumps(message, separators=(",", ":")).encode("utf-8")


def create_message_frame(original_data: bytes) -> bytes:
    num_bytes = len(original_data)
    size_in_bytes = num_bytes.to_bytes(2, byteorder="little")
    return size_in_bytes + original_data


def response_parser(data: bytes) -> dict:
    return json.loads(data.decode("utf-8"))


def text_value(value: TextLike) -> str:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8")
    return value


def payload_value(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8")
    return value
