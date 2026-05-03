import os
import string
import random
import pytest

from lucidmq_client import Consumer, LucidmqClient, Producer, StateStore, TopicManager
import msgpack_helper

HOST = os.environ.get('LUCIDMQ_SERVER_HOST', '127.0.0.1')
PORT = int(os.environ.get('LUCIDMQ_SERVER_PORT', '6969'))

def get_random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

class TestsOthers:
    def test_send_invalid_bytes(self):
        with LucidmqClient(HOST, PORT) as lucidClient:
            invalid_data = b'invalidData'
            framed = msgpack_helper.create_message_frame(invalid_data)
            lucidClient.send_message_bytes(framed)
            data = lucidClient.recieve_response()
            invalid_response_result = msgpack_helper.response_parser(data)
            assert 'Failed to parse message' in invalid_response_result['error_message']

class TestTopics:
    def test_topic_create(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as topic_manager:
            topic_create_result = topic_manager.create_topic(topic_name)
            assert topic_create_result['success'] == True
            assert topic_create_result['topic_name'] == topic_name
            topic_manager.delete_topic(topic_name)

    def test_create_topic_already_exists(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as topic_manager:
            topic_manager.create_topic(topic_name)
            topic_create_result = topic_manager.create_topic(topic_name)
            assert topic_create_result['success'] == False
            assert topic_create_result['topic_name'] == topic_name
            topic_manager.delete_topic(topic_name)

    def test_topic_describe(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as topic_manager:
            topic_manager.create_topic(topic_name)
            topic_describe_result = topic_manager.describe_topic(topic_name)
            assert topic_describe_result['success'] == True
            assert topic_describe_result['topic_name'] == topic_name
            topic_manager.delete_topic(topic_name)

    def test_describe_topic_dne(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as topic_manager:
            topic_describe_result = topic_manager.describe_topic(topic_name)
            assert topic_describe_result['success'] == False
            assert topic_describe_result['topic_name'] == topic_name

    def test_delete_topic(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as topic_manager:
            topic_manager.create_topic(topic_name)
            topic_delete_result = topic_manager.delete_topic(topic_name)
            assert topic_delete_result['success'] == True
            assert topic_delete_result['topic_name'] == topic_name
    
    def test_delete_topic_dne(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as topic_manager:
            topic_delete_result = topic_manager.delete_topic(topic_name)
            assert topic_delete_result['success'] == False
            assert topic_delete_result['topic_name'] == topic_name

class TestProducer:
    def test_upsert_1_message(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as topic_manager, Producer(HOST, PORT) as producer:
            topic_manager.create_topic(topic_name)

            produce_request_result = producer.upsert(topic_name, b'source-id', b'value')
            assert produce_request_result['success'] == True
            assert produce_request_result['topic_name'] == topic_name
            assert produce_request_result['offset'] == 0
            
            topic_manager.delete_topic(topic_name)

    def test_upsert_10_message(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as topic_manager, Producer(HOST, PORT) as producer:
            topic_manager.create_topic(topic_name)

            for x in range(10):
                source_id = bytes(f"source{x}", 'utf-8')
                payload = bytes(f"value{x}", 'utf-8')
                produce_request_result = producer.upsert(topic_name, source_id, payload)
                assert produce_request_result['success'] == True
                assert produce_request_result['topic_name'] == topic_name
                assert produce_request_result['offset'] == x
                
            topic_manager.delete_topic(topic_name)
    
    def test_upsert_30_large_message(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as topic_manager, Producer(HOST, PORT) as producer:
            topic_manager.create_topic(topic_name)

            for x in range(30):
                source_id = bytes(f"source{x}", 'utf-8')
                payload = bytes(f"myextreamlyverylargevalue{x}", 'utf-8')
                produce_request_result = producer.upsert(topic_name, source_id, payload)
                assert produce_request_result['success'] == True
                assert produce_request_result['topic_name'] == topic_name
                assert produce_request_result['offset'] == x
                
            topic_manager.delete_topic(topic_name)

    def test_delete_tombstone(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as topic_manager, Producer(HOST, PORT) as producer, StateStore(HOST, PORT) as store:
            topic_manager.create_topic(topic_name)

            producer.upsert(topic_name, b'source-id', b'value', b'parent-a')
            delete_result = producer.delete(topic_name, b'source-id', b'parent-a')

            assert delete_result['success'] == True
            assert delete_result['topic_name'] == topic_name
            assert delete_result['offset'] == 1
            scan_result = store.scan_current(topic_name)
            assert scan_result.get('success') == True, scan_result
            assert scan_result['records'] == []

            topic_manager.delete_topic(topic_name)

    def test_produce_topic_dne(self):
        topic_name = get_random_string(10)
        with Producer(HOST, PORT) as producer:
            produce_request_result = producer.produce(topic_name, b'source-id', b'value')
            assert produce_request_result['success'] == False
            assert produce_request_result['topic_name'] == topic_name
            assert produce_request_result['offset'] == 0

class TestConsumer:
    def test_consumer_1_message(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as tm, Producer(HOST, PORT) as prod, Consumer(HOST, PORT, 100) as cons:
            tm.create_topic(topic_name)

            source_id = b'source-id'
            payload = b'value'
            prod.produce(topic_name, source_id, payload)

            consumer_request_result = cons.consume(topic_name, "cg1")

            assert consumer_request_result['success'] == True
            assert consumer_request_result['topic_name'] == topic_name
            assert len(consumer_request_result['messages']) == 1

            message = consumer_request_result['messages'][0]
            assert bytes(message['source_id']) == source_id
            assert bytes(message['payload']) == payload
            
            tm.delete_topic(topic_name)

    def test_consumer_10_message(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as tm, Producer(HOST, PORT) as prod, Consumer(HOST, PORT, 100) as cons:
            tm.create_topic(topic_name)
            
            source_ids_sent = []
            payloads_sent = []
            
            for x in range(10):
                source_id = bytes(f'source{x}', 'utf-8')
                payload = bytes(f'value{x}', 'utf-8')
                source_ids_sent.append(source_id)
                payloads_sent.append(payload)
                prod.produce(topic_name, source_id, payload)

            consumer_request_result = cons.consume(topic_name, "cg1")

            assert consumer_request_result['success'] == True
            assert consumer_request_result['topic_name'] == topic_name
            assert len(consumer_request_result['messages']) == 10
            
            for i in range(len(consumer_request_result['messages'])):
                message = consumer_request_result['messages'][i]
                assert source_ids_sent[i] == bytes(message['source_id'])
                assert payloads_sent[i] == bytes(message['payload'])
        
            tm.delete_topic(topic_name)

    def test_consumer_no_message(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as tm, Consumer(HOST, PORT, 100) as cons:
            tm.create_topic(topic_name)

            consumer_request_result = cons.consume(topic_name, "cg1")

            assert consumer_request_result['success'] == True
            assert consumer_request_result['topic_name'] == topic_name
            assert len(consumer_request_result['messages']) == 0
            
            tm.delete_topic(topic_name)
    
    def test_consume_topic_dne(self):
        topic_name = get_random_string(10)
        with Consumer(HOST, PORT, 100) as cons:
            consumer_request_result = cons.consume(topic_name, "cg1")
            assert consumer_request_result['success'] == False
            assert consumer_request_result['topic_name'] == topic_name
    
    def test_consumer_message_already_consumed(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as tm, Producer(HOST, PORT) as prod, Consumer(HOST, PORT, 100) as cons0, Consumer(HOST, PORT, 100) as cons1:
            consumer_group = "cg1"
            tm.create_topic(topic_name)

            source_id = b'source-id'
            payload = b'value'
            prod.produce(topic_name, source_id, payload)

            consumer_request_result = cons0.consume(topic_name, consumer_group)

            assert consumer_request_result['success'] == True
            assert len(consumer_request_result['messages']) == 1

            message = consumer_request_result['messages'][0]
            assert bytes(message['source_id']) == source_id
            assert bytes(message['payload']) == payload

            # Second consumer polling the same group gets nothing
            consumer_request_result = cons1.consume(topic_name, consumer_group)

            assert consumer_request_result['success'] == True
            assert len(consumer_request_result['messages']) == 0

            tm.delete_topic(topic_name)

    def test_consumer_large_message(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as tm, Producer(HOST, PORT) as prod, Consumer(HOST, PORT, 100) as cons:
            tm.create_topic(topic_name)
            
            source_ids_sent = []
            payload = b'{"data": ["Iggy Azalea", "DaBaby", "21 Savage", "Smokepurpp", "Tee Grizzly", "Quando Rondo", "Drake", "NLE Choppa", "Young Dolph", "Lil Nas X", "Nav", "iann dior", "NoCap", "Gunna", "Russ", "Nipsey Hussle", "Don Toliver", "2 Chainz", "Lil Durk", "J Balvin", "YNW Melly", "Tyga", "Juice WRLD", "Mac Miller", "Roddy Ricch", "Lil Baby", "Trippie Redd", "Megan Thee Stallion", "Lil Mosey", "French Montana", "Pop Smoke", "Pooh Shiesty", "A Boogie wit Da Hoddie", "Kevin Gates", "King Von", "Rich the Kid", "Rae Sremmurd", "Internet Money", "Young Thug", "Gucci Mane", "88Glam", "Kodak Black", "Lil Yachty", "Chris Brown", "Polo G", "Travis Scott", "Lil Pump", "Fivio Foreign", "Lil Tecca", "Ugly God", "Moneybagg Yo", "Lil Uzi Vert", "Migos", "Jack Harlow", "Ozuna"], "status": "success"}'

            for x in range(10):
                source_id = bytes(f'source{x}', 'utf-8')
                source_ids_sent.append(source_id)
                prod.produce(topic_name, source_id, payload)

            consumer_request_result = cons.consume(topic_name, "cg1")

            assert consumer_request_result['success'] == True
            assert len(consumer_request_result['messages']) == 10
            
            for i in range(len(consumer_request_result['messages'])):
                message = consumer_request_result['messages'][i]
                assert source_ids_sent[i] == bytes(message['source_id'])
                assert payload == bytes(message['payload'])
        
            tm.delete_topic(topic_name)

    def test_consumer_poll_generator(self):
        topic_name = get_random_string(10)
        
        with TopicManager(HOST, PORT) as tm, Producer(HOST, PORT) as prod, Consumer(HOST, PORT, 100) as cons:
            tm.create_topic(topic_name)
            
            # Produce exactly 3 messages
            prod.produce(topic_name, b"source0", b"value0")
            prod.produce(topic_name, b"source1", b"value1")
            prod.produce(topic_name, b"source2", b"value2")

            # Create the generator
            message_generator = cons.poll(topic_name, "cg1")
            
            # Pull the first 3 messages from the generator
            msg0 = next(message_generator)
            assert bytes(msg0["source_id"]) == b"source0"
            
            msg1 = next(message_generator)
            assert bytes(msg1["source_id"]) == b"source1"
            
            msg2 = next(message_generator)
            assert bytes(msg2["source_id"]) == b"source2"
            
            tm.delete_topic(topic_name)


class TestStateStore:
    def test_get_returns_latest_record(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as tm, Producer(HOST, PORT) as prod, StateStore(HOST, PORT) as store:
            tm.create_topic(topic_name)

            prod.upsert(topic_name, b"source-1", b'{"version":1}', b"parent-a")
            prod.upsert(topic_name, b"source-1", b'{"version":2}', b"parent-b")

            result = store.get(topic_name, b"source-1")

            assert result.get("success") == True, result
            assert result["topic_name"] == topic_name
            assert result["action"] == "Get"
            assert bytes(result["record"]["source_id"]) == b"source-1"
            assert bytes(result["record"]["payload"]) == b'{"version":2}'
            assert bytes(result["record"]["parent_source_id"]) == b"parent-b"

            tm.delete_topic(topic_name)

    def test_get_children_respects_reparenting(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as tm, Producer(HOST, PORT) as prod, StateStore(HOST, PORT) as store:
            tm.create_topic(topic_name)

            prod.upsert(topic_name, b"source-1", b"value-1", b"parent-a")
            prod.upsert(topic_name, b"source-2", b"value-2", b"parent-a")
            prod.upsert(topic_name, b"source-1", b"value-3", b"parent-b")

            parent_a = store.get_children(topic_name, b"parent-a")
            parent_b = store.get_children(topic_name, b"parent-b")

            assert parent_a.get("success") == True, parent_a
            assert parent_a["action"] == "GetChildren"
            assert [bytes(record["source_id"]) for record in parent_a["records"]] == [b"source-2"]

            assert parent_b.get("success") == True, parent_b
            assert parent_b["action"] == "GetChildren"
            assert [bytes(record["source_id"]) for record in parent_b["records"]] == [b"source-1"]

            tm.delete_topic(topic_name)

    def test_scan_current_excludes_deleted_records(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as tm, Producer(HOST, PORT) as prod, StateStore(HOST, PORT) as store:
            tm.create_topic(topic_name)

            prod.upsert(topic_name, b"source-1", b"value-1", b"parent-a")
            prod.upsert(topic_name, b"source-2", b"value-2", b"parent-a")
            prod.delete(topic_name, b"source-1")

            result = store.scan_current(topic_name)

            assert result.get("success") == True, result
            assert result["action"] == "ScanCurrent"
            assert len(result["records"]) == 1
            assert bytes(result["records"][0]["source_id"]) == b"source-2"

            tm.delete_topic(topic_name)
