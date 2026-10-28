import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;

public class TestHMS {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(
            "hive.metastore.uris",
            "thrift://hive-metastore.data-platform.svc.cluster.local:9083"
        );

        try (IMetaStoreClient client = new HiveMetaStoreClient(conf)) {
            System.out.println("Connected to Hive Metastore!");
            System.out.println("Databases: " + client.getAllDatabases());
        }
    }
}