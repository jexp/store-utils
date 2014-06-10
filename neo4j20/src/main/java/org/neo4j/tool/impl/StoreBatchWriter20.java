package org.neo4j.tool.impl;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.tool.api.NodeInfo;
import org.neo4j.tool.api.RelInfo;
import org.neo4j.tool.api.StoreWriter;

/**
 * @author mh
 * @since 09.06.14
 */
public class StoreBatchWriter20 extends StoreBatchHandler20 implements StoreWriter {

    static Label[] NO_LABELS = new Label[0];

    public Label[] labels(NodeInfo node) {
        if (node.labels == null || node.labels.length == 0) return NO_LABELS;
        Label[] result = new Label[node.labels.length];
        for (int i = 0; i < result.length; i++) { // todo cache
            result[i] = DynamicLabel.label(node.labels[i]);
        }
        return result;
    }

    public RelationshipType type(RelInfo rel) {
        return DynamicRelationshipType.withName(rel.type);
    }

    @Override
    public void createNode(NodeInfo node) {
        batchInserter.createNode(node.id, node.data, labels(node));
    }

    @Override
    public void createRelationship(RelInfo rel) {
        batchInserter.createRelationship(rel.from, rel.to, type(rel), rel.data);
    }
}
