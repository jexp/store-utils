package org.neo4j.tool.impl;

import org.neo4j.graphdb.Label;
import org.neo4j.tool.api.NodeInfo;
import org.neo4j.tool.api.RelInfo;
import org.neo4j.tool.api.StoreReader;
import org.neo4j.unsafe.batchinsert.BatchRelationship;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author mh
 * @since 09.06.14
 */
public class StoreBatchReader20 extends StoreBatchHandler20 implements StoreReader {

    public static final String[] NO_LABELS = new String[0];
    private static final long NODE_RECORD = 14;
    private static final long REL_RECORD = 33;

    @Override
    public NodeInfo readNode(long id) {
        NodeInfo node = new NodeInfo(id);
        node.data = batchInserter.getNodeProperties(id);
        node.labels = toLabelStrings(id);
        return node;
    }

    private String[] toLabelStrings(long id) {
        Iterator<Label> labels = batchInserter.getNodeLabels(id).iterator();
        if (labels == null || !labels.hasNext()) return NO_LABELS;
        String[] result = new String[50];
        int i = 0;
        while (labels.hasNext()) {
            Label next = labels.next();
            result[i++] = next.name();
        }
        return Arrays.copyOf(result,i);
    }

    @Override
    public RelInfo readRel(long id) {
        RelInfo rel = new RelInfo(id);
        rel.data = batchInserter.getRelationshipProperties(id);
        BatchRelationship batchRelationship = batchInserter.getRelationshipById(id);
        rel.from = batchRelationship.getStartNode();
        rel.to = batchRelationship.getEndNode();
        rel.type = batchRelationship.getType().name();
        return rel;
    }

    @Override
    public boolean nodeExists(long node) {
        return batchInserter.nodeExists(node);
    }

    @Override
    public long highestNodeId() {
        return new File(dir,"neostore.nodestore.db").length() / NODE_RECORD;
    }

    @Override
    public long highestRelId() {
        return new File(dir,"neostore.relationshipstore.db").length() / REL_RECORD;
    }
}
