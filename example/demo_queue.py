from amq.connection import BrokerConnection
from amq.backends.queue import Backend
from amq.messaging import Messaging


def import_feed_callback(message_data, message):
    feed_url = message_data["import_feed"]
    print("Got feed import message for: %s" % feed_url)
    message.ack()


conn = BrokerConnection(hostname="localhost", port=5672,
                        userid="guest", password="guest", virtual_host="/")

m = Messaging(connection=conn, exchange="feed", routing_key="importer", backend_cls=Backend)
m.send({"import_feed": "http://cnn.com/rss/edition.rss"})
print('send msg ...')

m.register_callback(import_feed_callback)
m.fetch(enable_callbacks=True)
m.close()
