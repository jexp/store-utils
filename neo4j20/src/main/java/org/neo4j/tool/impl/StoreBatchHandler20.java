package org.neo4j.tool.impl;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.tool.api.StoreHandler;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.util.Map;

/**
 * @author mh
 * @since 09.06.14
 */
public class StoreBatchHandler20 implements StoreHandler {



    public static Map<String, String> config() {
        return (Map) MapUtil.map(
                "neostore.nodestore.db.mapped_memory", "100M",
                "neostore.relationshipstore.db.mapped_memory", "500M",
                "neostore.propertystore.db.mapped_memory", "300M",
                "neostore.propertystore.db.strings.mapped_memory", "1G",
                "neostore.propertystore.db.arrays.mapped_memory", "300M",
                "neostore.propertystore.db.index.keys.mapped_memory", "100M",
                "neostore.propertystore.db.index.mapped_memory", "100M",
                "cache_type", "none"
        );
    }

    protected BatchInserter batchInserter;
    protected String dir;

    @Override
    public void init(String dir, Map<String, String> config) {
        this.dir = dir;
        batchInserter = BatchInserters.inserter(dir, config == null ? config() : config);
    }

    @Override
    public void shutdown() {
        batchInserter.shutdown();
    }
}
