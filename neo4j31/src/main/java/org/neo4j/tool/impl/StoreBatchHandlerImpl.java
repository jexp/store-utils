package org.neo4j.tool.impl;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.tool.api.NodeInfo;
import org.neo4j.tool.api.RelInfo;
import org.neo4j.tool.api.StoreHandler;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.DirectRecordAccess;
import org.neo4j.unsafe.batchinsert.DirectRecordAccessSet;
import org.neo4j.unsafe.batchinsert.internal.BatchInserterImpl;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
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
    private Runnable flusher;

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
        flusher = getFlusher(batchInserter);
    }

    @Override
    public void flush() {
        flusher.run();
    }

    private static Runnable getFlusher(BatchInserter db) {
        try {
            Field field = BatchInserterImpl.class.getDeclaredField("recordAccess");
            field.setAccessible(true);
            final DirectRecordAccessSet recordAccessSet = (DirectRecordAccessSet) field.get(db);
            final Field cacheField = DirectRecordAccess.class.getDeclaredField("batch");
            cacheField.setAccessible(true);
            return new Runnable() {
                @Override public void run() {
                    try {
                        ((Map) cacheField.get(recordAccessSet.getNodeRecords())).clear();
                        ((Map) cacheField.get(recordAccessSet.getRelRecords())).clear();
                        ((Map) cacheField.get(recordAccessSet.getPropertyRecords())).clear();
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException("Error clearing cache "+cacheField,e);
                    }
                }
            };
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException("Error accessing cache field ", e);
        }
    }

}
