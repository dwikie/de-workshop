import os

from hive_metastore_client import HiveMetastoreClient

HMS_HOST = os.getenv(
    "HMS_HOST",
    "hive-metastore.data-platform.svc.cluster.local",
)
HMS_PORT = int(os.getenv("HMS_PORT", "9083"))


def main():
    client = HiveMetastoreClient(
        host=HMS_HOST,
        port=HMS_PORT,
    )

    try:
        client.open()
        print("Connected to Hive Metastore")
        print("Databases:", client.get_all_databases())
    finally:
        client.close()


if __name__ == "__main__":
    main()