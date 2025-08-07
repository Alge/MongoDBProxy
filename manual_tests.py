import logging
import time
import random
from pymongo import MongoClient, errors
from mongo_proxy import MongoProxy, DurableCursor

# --- ‼️ IMPORTANT CONFIGURATION ‼️ ---
# Change this to the connection string for your replica set.
MONGO_URI = "mongodb://192.168.0.116:27017,192.168.0.116:27018,192.168.0.116:27019/"
REPLICA_SET = "rs0" # Change this to your replica set name.
# --- End Configuration ---


def setup_logging():
    """Configures logging to show detailed info from the proxy."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logging.getLogger("mongo_proxy").setLevel(logging.DEBUG)
    # Silence the noisy pymongo connection pool logs for this test
    logging.getLogger("pymongo.pool").setLevel(logging.WARNING)


def test_writer_resilience():
    # ... (this function remains the same and is correct)
    print("\n--- 🚀 Starting Write Resilience Test ---")
    print("This test will insert a new document every 4 seconds.")
    print("While it's running, perform actions on your MongoDB cluster, such as:")
    print("  - Use `rs.stepDown()` in mongosh to trigger a failover.")
    print("  - Stop the primary node's container (`docker stop ...`).")
    print("Watch the output to see the proxy handle the errors and reconnect.\n")
    
    client = MongoClient(MONGO_URI, replicaSet=REPLICA_SET, serverSelectionTimeoutMS=5000)
    proxy = MongoProxy(client)
    collection = proxy.testdb.manual_test_collection

    try:
        collection.drop()
        print("✓ Dropped old test collection.")
    except Exception as e:
        print(f"✗ Could not drop collection (this is okay on first run): {e}")

    counter = 0
    while True:
        try:
            counter += 1
            doc = {"counter": counter, "time": time.ctime()}
            result = collection.insert_one(doc)
            print(f"[{counter}] ✓ Inserted document: {result.inserted_id}")

        except KeyboardInterrupt:
            print("\n--- 🛑 Test stopped by user. ---")
            break
        except Exception as e:
            print(f"[{counter}] ✗ An error occurred: {type(e).__name__} - {e}")
            print(f"[{counter}] ⏳ Proxy will now attempt to reconnect...")

        time.sleep(4)


def test_durable_cursor():
    """
    Tests the DurableCursor's ability to survive a failover mid-iteration
    without losing its place.
    """
    print("\n--- 🚀 Starting Durable Cursor Test ---")
    
    client = MongoClient(MONGO_URI, replicaSet=REPLICA_SET)
    setup_collection = client.testdb.durable_cursor_test
    
    # ** THE FIX: Use a number of documents GREATER than the default batch size (101). **
    num_docs = 300
    failover_point = 105 # A point safely after the first batch is exhausted.
    
    print(f"Setting up the collection with {num_docs} documents...")
    setup_collection.drop()
    setup_collection.insert_many([{'doc_num': i} for i in range(1, num_docs + 1)])
    print("✓ Collection setup complete.")

    print(f"\nStarting slow iteration (1 doc every 2 seconds).")
    print("\n‼️ WATCH THE COUNTER ‼️")
    print(f"TRIGGER A FAILOVER AFTER YOU SEE DOCUMENT #{failover_point} IS RETRIEVED.")
    print("This guarantees the next fetch will require a network call.\n")

    proxy = MongoProxy(client)
    proxied_collection = proxy.testdb.durable_cursor_test
    
    retrieved_docs = []
    try:
        # We still use batch_size=1 to be explicit, but the number of documents is the key.
        durable_cursor = DurableCursor(
            proxied_collection,
            sort=[('doc_num', 1)],
            batch_size=1 
        )
        
        for doc in durable_cursor:
            print(f"[Cursor] -> Retrieved document {doc['doc_num']}/{num_docs}", end='\r')
            retrieved_docs.append(doc['doc_num'])
            
            if doc['doc_num'] == failover_point:
                print(f"\n[!] NOW IS A GOOD TIME TO TRIGGER THE FAILOVER (`rs.stepDown()`) [!]")

            time.sleep(2)
            
        print(f"\n\n--- ✅ Test Complete ---")
        if len(retrieved_docs) == num_docs and sorted(retrieved_docs) == list(range(1, num_docs + 1)):
            print(f"🎉 SUCCESS! All {num_docs} documents were retrieved in order without duplicates.")
        else:
            print(f"🔥 FAILURE! Expected {num_docs} unique documents, but got {len(retrieved_docs)}.")
            print(f"   The first few retrieved documents: {sorted(retrieved_docs)[:20]}...")

    except KeyboardInterrupt:
        print("\n--- 🛑 Test stopped by user. ---")
    except Exception as e:
        print(f"\n--- ✗ TEST FAILED WITH AN UNEXPECTED ERROR ---")
        print(f"Error: {type(e).__name__} - {e}")
        import traceback
        traceback.print_exc()


def main():
    setup_logging()
    print("========================================")
    print("  MongoDBProxy Manual Test Suite")
    print("========================================")
    
    while True:
        print("\nChoose a test to run:")
        print("  [1] Write Resilience Test (Failover/Outage)")
        print("  [2] Durable Cursor Resilience Test")
        print("  [q] Quit")
        
        choice = input("> ")
        
        if choice == '1':
            test_writer_resilience()
        elif choice == '2':
            test_durable_cursor()
        elif choice.lower() == 'q':
            break
        else:
            print("Invalid choice, please try again.")

if __name__ == "__main__":
    main()
