from amq.connection import BrokerConnection
from amq.messaging import Consumer


def import_feed_callback(message_data, message):
    feed_url = message_data["import_feed"]
    print("Got feed import message for: %s" % feed_url)
    message.ack()


conn = BrokerConnection(hostname="localhost", port=5672,
                        userid="guest", password="guest", virtual_host="/")
consumer = Consumer(connection=conn, queue="feed",
                    exchange="feed", routing_key="importer")
consumer.register_callback(import_feed_callback)
consumer.wait()
