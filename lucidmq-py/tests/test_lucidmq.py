import os
import string
import random
import pytest

from lucidmq_client import Producer, Consumer, TopicManager, LucidmqClient
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
    def test_produce_1_message(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as topic_manager, Producer(HOST, PORT) as producer:
            topic_manager.create_topic(topic_name)

            produce_request_result = producer.produce(topic_name, b'key', b'value')
            assert produce_request_result['success'] == True
            assert produce_request_result['topic_name'] == topic_name
            assert produce_request_result['offset'] == 0
            
            topic_manager.delete_topic(topic_name)

    def test_produce_10_message(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as topic_manager, Producer(HOST, PORT) as producer:
            topic_manager.create_topic(topic_name)

            for x in range(10):
                key = bytes(f"key{x}", 'utf-8')
                value = bytes(f"value{x}", 'utf-8')
                produce_request_result = producer.produce(topic_name, key, value)
                assert produce_request_result['success'] == True
                assert produce_request_result['topic_name'] == topic_name
                assert produce_request_result['offset'] == x
                
            topic_manager.delete_topic(topic_name)
    
    def test_produce_30_large_message(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as topic_manager, Producer(HOST, PORT) as producer:
            topic_manager.create_topic(topic_name)

            for x in range(30):
                key = bytes(f"key{x}", 'utf-8')
                value = bytes(f"myextreamlyverylargevalue{x}", 'utf-8')
                produce_request_result = producer.produce(topic_name, key, value)
                assert produce_request_result['success'] == True
                assert produce_request_result['topic_name'] == topic_name
                assert produce_request_result['offset'] == x
                
            topic_manager.delete_topic(topic_name)

    def test_produce_topic_dne(self):
        topic_name = get_random_string(10)
        with Producer(HOST, PORT) as producer:
            produce_request_result = producer.produce(topic_name, b'key', b'value')
            assert produce_request_result['success'] == False
            assert produce_request_result['topic_name'] == topic_name
            assert produce_request_result['offset'] == 0

class TestConsumer:
    def test_consumer_1_message(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as tm, Producer(HOST, PORT) as prod, Consumer(HOST, PORT, 100) as cons:
            tm.create_topic(topic_name)

            key = b'key'
            value = b'value'
            prod.produce(topic_name, key, value)

            consumer_request_result = cons.consume(topic_name, "cg1")

            assert consumer_request_result['success'] == True
            assert consumer_request_result['topic_name'] == topic_name
            assert len(consumer_request_result['messages']) == 1

            message = consumer_request_result['messages'][0]
            assert bytes(message['key']) == key
            assert bytes(message['value']) == value
            
            tm.delete_topic(topic_name)

    def test_consumer_10_message(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as tm, Producer(HOST, PORT) as prod, Consumer(HOST, PORT, 100) as cons:
            tm.create_topic(topic_name)
            
            keys_sent = []
            values_sent = []
            
            for x in range(10):
                key = bytes(f'key{x}', 'utf-8')
                value = bytes(f'value{x}', 'utf-8')
                keys_sent.append(key)
                values_sent.append(value)
                prod.produce(topic_name, key, value)

            consumer_request_result = cons.consume(topic_name, "cg1")

            assert consumer_request_result['success'] == True
            assert consumer_request_result['topic_name'] == topic_name
            assert len(consumer_request_result['messages']) == 10
            
            for i in range(len(consumer_request_result['messages'])):
                message = consumer_request_result['messages'][i]
                assert keys_sent[i] == bytes(message['key'])
                assert values_sent[i] == bytes(message['value'])
        
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

            key = b'key'
            value = b'value'
            prod.produce(topic_name, key, value)

            consumer_request_result = cons0.consume(topic_name, consumer_group)

            assert consumer_request_result['success'] == True
            assert len(consumer_request_result['messages']) == 1

            message = consumer_request_result['messages'][0]
            assert bytes(message['key']) == key
            assert bytes(message['value']) == value

            # Second consumer polling the same group gets nothing
            consumer_request_result = cons1.consume(topic_name, consumer_group)

            assert consumer_request_result['success'] == True
            assert len(consumer_request_result['messages']) == 0

            tm.delete_topic(topic_name)

    def test_consumer_large_message(self):
        topic_name = get_random_string(10)
        with TopicManager(HOST, PORT) as tm, Producer(HOST, PORT) as prod, Consumer(HOST, PORT, 100) as cons:
            tm.create_topic(topic_name)
            
            keys_sent = []
            value = b'{"data": ["Iggy Azalea", "DaBaby", "21 Savage", "Smokepurpp", "Tee Grizzly", "Quando Rondo", "Drake", "NLE Choppa", "Young Dolph", "Lil Nas X", "Nav", "iann dior", "NoCap", "Gunna", "Russ", "Nipsey Hussle", "Don Toliver", "2 Chainz", "Lil Durk", "J Balvin", "YNW Melly", "Tyga", "Juice WRLD", "Mac Miller", "Roddy Ricch", "Lil Baby", "Trippie Redd", "Megan Thee Stallion", "Lil Mosey", "French Montana", "Pop Smoke", "Pooh Shiesty", "A Boogie wit Da Hoddie", "Kevin Gates", "King Von", "Rich the Kid", "Rae Sremmurd", "Internet Money", "Young Thug", "Gucci Mane", "88Glam", "Kodak Black", "Lil Yachty", "Chris Brown", "Polo G", "Travis Scott", "Lil Pump", "Fivio Foreign", "Lil Tecca", "Ugly God", "Moneybagg Yo", "Lil Uzi Vert", "Migos", "Jack Harlow", "Ozuna"], "status": "success"}'

            for x in range(10):
                key = bytes(f'key{x}', 'utf-8')
                keys_sent.append(key)
                prod.produce(topic_name, key, value)

            consumer_request_result = cons.consume(topic_name, "cg1")

            assert consumer_request_result['success'] == True
            assert len(consumer_request_result['messages']) == 10
            
            for i in range(len(consumer_request_result['messages'])):
                message = consumer_request_result['messages'][i]
                assert keys_sent[i] == bytes(message['key'])
                assert value == bytes(message['value'])
        
            tm.delete_topic(topic_name)

    def test_consumer_poll_generator(self):
        topic_name = get_random_string(10)
        
        with TopicManager(HOST, PORT) as tm, Producer(HOST, PORT) as prod, Consumer(HOST, PORT, 100) as cons:
            tm.create_topic(topic_name)
            
            # Produce exactly 3 messages
            prod.produce(topic_name, b"key0", b"value0")
            prod.produce(topic_name, b"key1", b"value1")
            prod.produce(topic_name, b"key2", b"value2")

            # Create the generator
            message_generator = cons.poll(topic_name, "cg1")
            
            # Pull the first 3 messages from the generator
            msg0 = next(message_generator)
            assert bytes(msg0["key"]) == b"key0"
            
            msg1 = next(message_generator)
            assert bytes(msg1["key"]) == b"key1"
            
            msg2 = next(message_generator)
            assert bytes(msg2["key"]) == b"key2"
            
            tm.delete_topic(topic_name)