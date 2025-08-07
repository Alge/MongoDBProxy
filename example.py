import logging
import time
from pymongo import MongoClient
from mongo_proxy import MongoProxy


def main():
    # --- Configuration ---
    # Define your MongoDB replica set nodes here
    mongo_nodes = [
        "192.168.0.113:27017",
        "192.168.0.113:27018",
        "192.168.0.113:27019",
    ]
    # Define your replica set name
    replica_set_name = "rs0"
    # --- End Configuration ---

    # Configure basic logging to capture WARNING and above
    logging.basicConfig(
        level=logging.WARNING,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Enable DEBUG logging specifically for the mongo_proxy module
    proxy_logger = logging.getLogger("mongo_proxy")
    proxy_logger.setLevel(logging.DEBUG)

    # Also enable INFO level for our script
    script_logger = logging.getLogger(__name__)
    script_logger.setLevel(logging.INFO)

    # Add a console handler to our script logger to see our messages
    if not script_logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        script_logger.addHandler(handler)
        script_logger.propagate = False

    # Connect to the MongoDB replica set using the list of nodes
    script_logger.info(
        f"Attempting to connect to replica set '{replica_set_name}' with nodes: {mongo_nodes}")
    client = MongoClient(mongo_nodes, replicaSet=replica_set_name)

    # Test the connection
    try:
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        script_logger.info("✓ Successfully connected to MongoDB replica set")
    except Exception as e:
        script_logger.error(f"✗ Failed to connect: {e}")
        return

    # Create MongoProxy - this handles AutoReconnect automatically
    proxy = MongoProxy(client)

    collection = proxy.testdb.mycollection

    script_logger.info("MongoProxy example started.")
    script_logger.info(
        "Try: rs.stepDown() in the mongo shell to trigger a failover and see reconnection logs.")
    script_logger.info("Press Ctrl+C to exit.\n")

    counter = 0
    while True:
        try:
            counter += 1

            # Simple database operation
            doc = {"counter": counter, "timestamp": time.time()}
            result = collection.insert_one(doc)
            script_logger.info(f"[{counter}] Inserted document: {result.inserted_id}")

        except KeyboardInterrupt:
            script_logger.info("\nExiting...")
            break
        except Exception as e:
            script_logger.error(f"[{counter}] An error occurred: {e}")

        time.sleep(3)


if __name__ == "__main__":
    main()