import socket
import msgpack_helper

class LucidmqClient:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.tcp_stream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print(f'Establishing TCP connection to {host}:{port}')
        self.tcp_stream.connect((host, port))
    
    # 1. Context Manager Support
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def send_message_bytes(self, data: bytes) -> None:
        self.tcp_stream.sendall(data)
    
    # 2. Robust Socket Reading
    def _recv_exactly(self, num_bytes: int) -> bytes:
        """Ensure we read exactly `num_bytes` from the TCP stream."""
        chunks = []
        bytes_recd = 0
        while bytes_recd < num_bytes:
            chunk = self.tcp_stream.recv(min(num_bytes - bytes_recd, 4096))
            if chunk == b'':
                raise RuntimeError("Socket connection broken")
            chunks.append(chunk)
            bytes_recd += len(chunk)
        return b''.join(chunks)

    def recieve_response(self) -> bytes:
        # Read the 2-byte frame size
        first_message = self._recv_exactly(2)
        int_val = int.from_bytes(first_message, "little")
        # Read the actual payload
        return self._recv_exactly(int_val)

    def close(self) -> None:
        self.tcp_stream.close()


class Producer(LucidmqClient):
    def upsert(
        self,
        topic_name: str,
        source_id: bytes,
        payload: bytes,
        parent_source_id: bytes = None,
    ) -> dict:
        msg = msgpack_helper.produce_upsert_request(
            topic_name,
            source_id,
            payload,
            parent_source_id,
        )
        self.send_message_bytes(msg)
        data = self.recieve_response()
        return msgpack_helper.response_parser(data)

    def delete(
        self,
        topic_name: str,
        source_id: bytes,
        parent_source_id: bytes = None,
    ) -> dict:
        msg = msgpack_helper.produce_delete_request(
            topic_name,
            source_id,
            parent_source_id,
        )
        self.send_message_bytes(msg)
        data = self.recieve_response()
        return msgpack_helper.response_parser(data)


class Consumer(LucidmqClient):
    def __init__(self, host: str, port: int, timeout: int):
        super().__init__(host, port)
        self.timeout = timeout

    def consume(self, topic_name: str, consumer_group: str) -> dict:
        """Fetches a single batch of messages (raw response)"""
        msg = msgpack_helper.consume_request(topic_name, consumer_group, self.timeout)
        self.send_message_bytes(msg)
        data = self.recieve_response()
        return msgpack_helper.response_parser(data)

    # 3. Leveraging Generators (yield)
    def poll(self, topic_name: str, consumer_group: str):
        """
        Continuously yields messages as they arrive.
        Abstracts away the polling loop and empty responses.
        """
        while True:
            response = self.consume(topic_name, consumer_group)
            
            # If the poll was successful and actually contains messages
            if response.get('success') and response.get('messages'):
                for message in response['messages']:
                    yield message


class StateStore(LucidmqClient):
    def get(self, topic_name: str, source_id: bytes) -> dict:
        msg = msgpack_helper.state_request_get(topic_name, source_id)
        self.send_message_bytes(msg)
        return msgpack_helper.response_parser(self.recieve_response())

    def get_children(self, topic_name: str, parent_source_id: bytes) -> dict:
        msg = msgpack_helper.state_request_get_children(topic_name, parent_source_id)
        self.send_message_bytes(msg)
        return msgpack_helper.response_parser(self.recieve_response())

    def scan_current(self, topic_name: str) -> dict:
        msg = msgpack_helper.state_request_scan_current(topic_name)
        self.send_message_bytes(msg)
        return msgpack_helper.response_parser(self.recieve_response())


class TopicManager(LucidmqClient):
    def create_topic(self, topic_name: str) -> dict:
        msg = msgpack_helper.topic_request_create(topic_name)
        self.send_message_bytes(msg)
        return msgpack_helper.response_parser(self.recieve_response())
    
    def describe_topic(self, topic_name: str) -> dict:
        msg = msgpack_helper.topic_request_describe(topic_name)
        self.send_message_bytes(msg)
        return msgpack_helper.response_parser(self.recieve_response())

    def delete_topic(self, topic_name: str) -> dict:
        msg = msgpack_helper.topic_request_delete(topic_name)
        self.send_message_bytes(msg)
        return msgpack_helper.response_parser(self.recieve_response())
    
    def all_topic(self) -> dict:
        msg = msgpack_helper.topic_request_all()
        self.send_message_bytes(msg)
        return msgpack_helper.response_parser(self.recieve_response())
