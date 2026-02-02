import os
import pytest
import multiprocessing
import time

from common.middleware import middleware_rabbitmq
from utils.message_consumer_tester import MessageConsumerTester

TEST_EXCHANGE_NAME = "test_exchange"

MOM_HOST = os.environ['MOM_HOST']

# -----------------------------------------------------------------------------
# HELP FUNCTIONS
# -----------------------------------------------------------------------------

def _message_set_consumer(message_set, messages_before_close, routing_keys):
	consumer_exchange = middleware_rabbitmq.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, TEST_EXCHANGE_NAME, routing_keys)
	message_consumer_tester = MessageConsumerTester(consumer_exchange, message_set, messages_before_close)
	consumer_exchange.start_consuming(lambda message, ack, nack: message_consumer_tester.callback(message, ack, nack))

def _generate_messages(amount):
	return list(map(lambda n: bytes(f"{n}", "utf-8"), range(amount)))

def _test_exchange(routing_keys_by_consumers, messages_by_routing_key):
	with multiprocessing.Manager() as manager:
		consumers = routing_keys_by_consumers.keys()
		message_set_by_consumer = dict()
		consummer_processes = []

		for consumer in consumers:
			messages_before_close = 0
			for consumer_routing_key in routing_keys_by_consumers[consumer]:
				messages_before_close += len(messages_by_routing_key[consumer_routing_key])
			#Introduced in Python 3.14. Ref: docs.python.org/3/library/multiprocessing.html#multiprocessing.managers.SyncManager.set
			message_set_by_consumer[consumer] = manager.set()
			consummer_process = multiprocessing.Process(target=_message_set_consumer, args=(message_set_by_consumer[consumer], messages_before_close, routing_keys_by_consumers[consumer]))
			consummer_process.start()
			consummer_processes.append(consummer_process)

		#TODO: Barrier to sync actual exchange consumming before sending messages
		time.sleep(0.5)

		for routing_key, messages in messages_by_routing_key.items():
			producer_exchange = middleware_rabbitmq.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, TEST_EXCHANGE_NAME, [routing_key])
			for message in messages:
				producer_exchange.send(message)
			producer_exchange.close()

		for consummer_process in consummer_processes:
			consummer_process.join()

		for consumer in consumers:
			message_set = message_set_by_consumer[consumer]
			messages = []
			for consumer_routing_key in routing_keys_by_consumers[consumer]:
				messages += messages_by_routing_key[consumer_routing_key]

			assert len(message_set) == len(messages), f"The amount of consummed messages is not as expected for consumer {consumer}"
			for message in messages:
				assert message in message_set, f"The message {message} was not consummed by {consumer}"

# -----------------------------------------------------------------------------
# GENERAL TESTS
# -----------------------------------------------------------------------------

def test_init_and_close():
	routing_keys = ["route_1"]
	exchange = middleware_rabbitmq.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, TEST_EXCHANGE_NAME, routing_keys)
	exchange.close()

# -----------------------------------------------------------------------------
#  DIRECT MESSAGING TESTS
# -----------------------------------------------------------------------------
def test_direct_messaging_one_consumer_one_message():
	_test_exchange({
		"consumer_1": ["route_1"]
		}, {
		"route_1": [b"message"]
		})

def test_direct_messaging_one_consumer_many_messages():
	_test_exchange({
		"consumer_1": ["route_1"]
		}, {
		"route_1": [b"message", b"message_2", b"message_3"]
		})

def test_direct_messaging_many_consumers_many_messages():
	_test_exchange({
		"consumer_1": ["route_1"],
		"consumer_2": ["route_2"],
		"consumer_3": ["route_3"]
		}, {
		"route_1": [b"message_1"],
		"route_2": [b"message_2"],
		"route_3": [b"message_3"]
		})

# -----------------------------------------------------------------------------
#  BROADCAST MESSAGING TESTS
# -----------------------------------------------------------------------------

def test_broadcast_single_routing_key():
	_test_exchange({
		"consumer_1": ["route_1"],
		"consumer_2": ["route_1"],
		"consumer_3": ["route_1"]
		}, {
		"route_1": [b"message_1"],
		})

def test_broadcast_many_routing_keys():
	_test_exchange({
		"consumer_1": ["route_1"],
		"consumer_2": ["route_1", "route_2"],
		"consumer_3": ["route_1", "route_2"],
		}, {
		"route_1": [b"message_1"],
		"route_2": [b"message_2"],
		})