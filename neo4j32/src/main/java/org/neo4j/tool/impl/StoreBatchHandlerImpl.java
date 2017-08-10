package org.neo4j.tool.impl;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.tool.api.NodeInfo;
import org.neo4j.tool.api.RelInfo;
import org.neo4j.tool.api.StoreHandler;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * @author mh
 * @since 09.06.14
 */
public class StoreBatchHandlerImpl implements StoreHandler {

    static Label[] NO_LABELS = new Label[0];
    public static final String[] NO_LABELS_NAMES = new String[0];

    protected String[] toLabelStrings(long id) {
        Iterator<Label> labels = batchInserter.getNodeLabels(id).iterator();
        if (labels == null || !labels.hasNext()) return NO_LABELS_NAMES;
        String[] result = new String[50];
        int i = 0;
        while (labels.hasNext()) {
            Label next = labels.next();
            result[i++] = next.name();
        }
        return Arrays.copyOf(result, i);
    }

    public Label[] labels(NodeInfo node) {
        if (node.labels == null || node.labels.length == 0) return NO_LABELS;
        Label[] result = new Label[node.labels.length];
        for (int i = 0; i < result.length; i++) { // todo cache
            result[i] = Label.label(node.labels[i]);
        }
        return result;
    }

    public RelationshipType type(RelInfo rel) {
        return RelationshipType.withName(rel.type);
    }

    protected BatchInserter batchInserter;
    protected String dir;

    @Override
    public void init(String dir, String pageCache) throws IOException {
        this.dir = dir;
        batchInserter = BatchInserters.inserter(new File(dir), MapUtil.stringMap("dbms.memory.pagecache.size", pageCache));
    }

    @Override
    public void shutdown() {
        batchInserter.shutdown();
    }
}
