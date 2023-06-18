from lucidmq_client import Producer, Consumer, TopicManager
import string
import random
import os

HOST = os.environ.get('LUCIDMQ_SERVER_HOST', '127.0.0.1')  # The server's hostname or IP address
PORT = 6969  # The port used by the server

def get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

class TestTopics:
    def test_topic_create(self):
        topic_name = get_random_string(10)
        topic_manager = TopicManager(HOST, PORT)
        # Create a topic
        topic_create_result = topic_manager.create_topic(topic_name)
        assert topic_create_result['success'] == True
        assert topic_create_result['topicName'] == topic_name
        # Delete the topic to clean up
        topic_delete_result = topic_manager.delete_topic(topic_name)

    def test_create_topic_already_exists(self):
        topic_name = get_random_string(10)
        topic_manager = TopicManager(HOST, PORT)
        topic_create_result = topic_manager.create_topic(topic_name)
        topic_create_result = topic_manager.create_topic(topic_name)
        assert topic_create_result['success'] == False
        assert topic_create_result['topicName'] == topic_name
        # Delete the topic to clean up
        topic_delete_result = topic_manager.delete_topic(topic_name)

    def test_topic_describe(self):
        topic_name = get_random_string(10)
        topic_manager = TopicManager(HOST, PORT)
        # Create a topic
        topic_create_result = topic_manager.create_topic(topic_name)
        topic_describe_result = topic_manager.describe_topic(topic_name)
        assert topic_describe_result['success'] == True
        assert topic_describe_result['topicName'] == topic_name
        # Delete the topic to clean up
        topic_delete_result = topic_manager.delete_topic(topic_name)


    def test_describe_topic_dne(self):
        topic_name = get_random_string(10)
        topic_manager = TopicManager(HOST, PORT)
        topic_describe_result = topic_manager.describe_topic(topic_name)
        assert topic_describe_result['success'] == False
        assert topic_describe_result['topicName'] == topic_name
    

    def test_delete_topic_dne(self):
        topic_name = get_random_string(10)
        topic_manager = TopicManager(HOST, PORT)
        topic_delete_result = topic_manager.delete_topic(topic_name)
        assert topic_delete_result['success'] == False
        assert topic_delete_result['topicName'] == topic_name

class TestProducer:
    def test_produce_1_message(self):
        topic_name = get_random_string(10)
        topic_manager = TopicManager(HOST, PORT)
        producer = Producer(HOST, PORT)
        # Create a topic to set up
        topic_create_result = topic_manager.create_topic(topic_name)

        produce_request_result = producer.produce(topic_name, b'key', b'value')
        assert produce_request_result['success'] == True
        assert produce_request_result['topicName'] == topic_name
        assert produce_request_result['offset'] == 0
        
        # Delete the topic to clean up
        topic_delete_result = topic_manager.delete_topic(topic_name)

    def test_produce_10_message(self):
        topic_name = get_random_string(10)
        topic_manager = TopicManager(HOST, PORT)
        producer = Producer(HOST, PORT)
        
        # Create a topic to set up
        topic_create_result = topic_manager.create_topic(topic_name)

        for x in range(10):
            value = bytes("value{0}".format(x), 'utf-8')
            produce_request_result = producer.produce(topic_name, b'key', b'value')
            assert produce_request_result['success'] == True
            assert produce_request_result['topicName'] == topic_name
            assert produce_request_result['offset'] == x
        
        # Delete the topic to clean up
        topic_delete_result = topic_manager.delete_topic(topic_name)
    
    # Run this test so we attempt a split
    def test_produce_20_large_message(self):
        topic_name = get_random_string(10)
        topic_manager = TopicManager(HOST, PORT)
        producer = Producer(HOST, PORT)
        
        # Create a topic to set up
        topic_create_result = topic_manager.create_topic(topic_name)

        for x in range(30):
            value = bytes("myextreamlyverylargevalue{0}".format(x), 'utf-8')
            produce_request_result = producer.produce(topic_name, b'key', b'value')
            assert produce_request_result['success'] == True
            assert produce_request_result['topicName'] == topic_name
            assert produce_request_result['offset'] == x
        
        # Delete the topic to clean up
        topic_delete_result = topic_manager.delete_topic(topic_name)

    def test_produce_topic_dne(self):
        topic_name = get_random_string(10)
        producer = Producer(HOST, PORT)
        
        produce_request_result = producer.produce(topic_name, b'key', b'value')
        assert produce_request_result['success'] == False
        assert produce_request_result['topicName'] == topic_name
        assert produce_request_result['offset'] == 0

class TestConsumer:
    def test_consumeer_1_message(self):
        topic_name = get_random_string(10)
        topic_manager = TopicManager(HOST, PORT)
        producer = Producer(HOST, PORT)
        consumer = Consumer(HOST, PORT, 100)
        # Create a topic to set up
        topic_create_result = topic_manager.create_topic(topic_name)

        key = b'key'
        value = b'value'
        # Produce message to our topic
        produce_request_result = producer.produce(topic_name, key, value)

        consumer_request_result = consumer.consume(topic_name, "cg1")

        # Check the wraper around consumer request
        assert consumer_request_result['success'] == True
        assert consumer_request_result['topicName'] == topic_name
        assert len(consumer_request_result['messages']) == 1

        # Check the message itself
        message = consumer_request_result['messages'][0]
        assert message['key'] == key
        assert message['value'] == value
    
        # Delete the topic to clean up
        topic_delete_result = topic_manager.delete_topic(topic_name)

    def test_consumeer_10_message(self):
        topic_name = get_random_string(10)
        topic_manager = TopicManager(HOST, PORT)
        producer = Producer(HOST, PORT)
        consumer = Consumer(HOST, PORT, 100)
        # Create a topic to set up
        topic_create_result = topic_manager.create_topic(topic_name)
        
        keys_sent = []
        values_sent = []
        
        for x in range(10):
            key = bytes('key{}'.format(x), 'utf-8')
            value = bytes('value{}'.format(x), 'utf-8')
            keys_sent.append(key)
            values_sent.append(value)
            # Produce message to our topic
            produce_request_result = producer.produce(topic_name, key, value)

        consumer_request_result = consumer.consume(topic_name, "cg1")

        # Check the wraper around consumer request
        assert consumer_request_result['success'] == True
        assert consumer_request_result['topicName'] == topic_name
        
        assert len(consumer_request_result['messages']) == 10
        for i in range(len(consumer_request_result['messages'])):
            message = consumer_request_result['messages'][i]
            assert keys_sent[i] ==  message['key']
            assert values_sent[i] ==  message['value']
    
        # Delete the topic to clean up
        topic_delete_result = topic_manager.delete_topic(topic_name)
