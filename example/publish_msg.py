from amq.connection import BrokerConnection
from amq.messaging import Publisher


conn = BrokerConnection(hostname="localhost", port=5672,
                        userid="guest", password="guest", virtual_host="/")

publisher = Publisher(connection=conn, exchange="feed", routing_key="importer")
publisher.send({"import_feed": "http://cnn.com/rss/edition.rss"})
publisher.close()
print('send msg ...')
