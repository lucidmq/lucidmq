import msgpack
import time

def topic_request_describe(topic_name: str) -> bytes:
    req = {"TopicRequest": {"topic_name": topic_name, "request_type": "Describe"}}
    return create_message_frame(msgpack.packb(req, use_bin_type=True))

def topic_request_create(topic_name: str) -> bytes:
    req = {"TopicRequest": {"topic_name": topic_name, "request_type": "Create"}}
    return create_message_frame(msgpack.packb(req, use_bin_type=True))

def topic_request_delete(topic_name: str) -> bytes:
    req = {"TopicRequest": {"topic_name": topic_name, "request_type": "Delete"}}
    return create_message_frame(msgpack.packb(req, use_bin_type=True))

def topic_request_all() -> bytes:
    req = {"TopicRequest": {"topic_name": "placeholder", "request_type": "All"}}
    return create_message_frame(msgpack.packb(req, use_bin_type=True))

def produce_request(
    topic_name: str,
    source_id: bytes,
    payload = None,
    parent_source_id: bytes = None,
    op: str = "Upsert"
) -> bytes:
    ms = time.time_ns() // 1_000_000
    req = {
        "ProduceRequest": {
            "topic_name": topic_name,
            "messages": [{
                "last_updated": ms,
                "source_id": source_id,
                "payload": payload,
                "parent_source_id": parent_source_id,
                "op": op,
            }]
        }
    }
    return create_message_frame(msgpack.packb(req, use_bin_type=True))

def produce_upsert_request(
    topic_name: str,
    source_id: bytes,
    payload: bytes,
    parent_source_id: bytes = None,
) -> bytes:
    return produce_request(topic_name, source_id, payload, parent_source_id, "Upsert")

def produce_delete_request(
    topic_name: str,
    source_id: bytes,
    parent_source_id: bytes = None,
) -> bytes:
    return produce_request(topic_name, source_id, None, parent_source_id, "Delete")

def state_request_get(topic_name: str, source_id: bytes) -> bytes:
    req = {
        "StateRequest": {
            "topic_name": topic_name,
            "action": "Get",
            "source_id": source_id,
        }
    }
    return create_message_frame(msgpack.packb(req, use_bin_type=True))

def state_request_get_children(topic_name: str, parent_source_id: bytes) -> bytes:
    req = {
        "StateRequest": {
            "topic_name": topic_name,
            "action": "GetChildren",
            "parent_source_id": parent_source_id,
        }
    }
    return create_message_frame(msgpack.packb(req, use_bin_type=True))

def state_request_scan_current(topic_name: str) -> bytes:
    req = {
        "StateRequest": {
            "topic_name": topic_name,
            "action": "ScanCurrent",
        }
    }
    return create_message_frame(msgpack.packb(req, use_bin_type=True))

def consume_request(topic_name: str, consumer_group: str, timeout: int) -> bytes:
    req = {
        "ConsumeRequest": {
            "topic_name": topic_name,
            "consumer_group": consumer_group,
            "timeout": timeout
        }
    }
    return create_message_frame(msgpack.packb(req, use_bin_type=True))

def create_message_frame(original_data: bytes) -> bytes:
    num_bytes = len(original_data)
    size_in_bytes = num_bytes.to_bytes(2, byteorder='little')
    return size_in_bytes + original_data

def response_parser(data: bytes) -> dict:
    # Unpack the messagepack bytes back into a python dictionary
    parsed = msgpack.unpackb(data, raw=False)
    
    # Rust Enum serialization wraps the payload in a single outer key (e.g. {"TopicResponse": {...}})
    # We unwrap it here for convenience:
    if "TopicResponse" in parsed:
        return parsed["TopicResponse"]
    elif "ProduceResponse" in parsed:
        return parsed["ProduceResponse"]
    elif "ConsumeResponse" in parsed:
        return parsed["ConsumeResponse"]
    elif "StateResponse" in parsed:
        return parsed["StateResponse"]
    elif "InvalidResponse" in parsed:
        return parsed["InvalidResponse"]
    else:
        print("Invalid envelope type")
        return parsed
