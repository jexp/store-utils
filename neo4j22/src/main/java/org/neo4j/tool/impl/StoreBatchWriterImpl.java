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
public class StoreBatchWriterImpl extends StoreBatchHandlerImpl implements StoreWriter {

    @Override
    public void createNode(NodeInfo node) {
        batchInserter.createNode(node.id, node.data, labels(node));
    }

    @Override
    public void createRelationship(RelInfo rel) {
        batchInserter.createRelationship(rel.from, rel.to, type(rel), rel.data);
    }
}
