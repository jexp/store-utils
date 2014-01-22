package org.neo4j.tool;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.MapUtil;

import java.util.Map;

/**
 * @author mh
 * @since 21.12.11
 */
public class StoreUpdate {
    public static Map<String, String> config() {
        //noinspection unchecked
        return (Map) MapUtil.map(
                "neostore.nodestore.db.mapped_memory", "100M",
                "neostore.relationshipstore.db.mapped_memory", "500M",
                "neostore.propertystore.db.mapped_memory", "300M",
                "neostore.propertystore.db.strings.mapped_memory", "1G",
                "neostore.propertystore.db.arrays.mapped_memory", "300M",
                "neostore.propertystore.db.index.keys.mapped_memory", "100M",
                "neostore.propertystore.db.index.mapped_memory", "100M",
                "allow_store_upgrade", "true",
                "cache_type", "weak"
        );
    }

    public static void main(String[] args) {
        GraphDatabaseService db = null;
        try {
            db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder("target/data").setConfig(config()).newGraphDatabase();
        } finally {
            if (db != null) db.shutdown();
        }
    }
}
