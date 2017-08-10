package org.neo4j.tool.impl;

import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.tool.api.NodeInfo;
import org.neo4j.tool.api.RelInfo;
import org.neo4j.tool.api.StoreReader;
import org.neo4j.unsafe.batchinsert.BatchRelationship;

import java.io.File;

/**
 * @author mh
 * @since 09.06.14
 */
public class StoreBatchReaderImpl extends StoreBatchHandlerImpl implements StoreReader {


    @Override
    public NodeInfo readNode(long id) {
        NodeInfo node = new NodeInfo(id);
        node.data = batchInserter.getNodeProperties(id);
        node.labels = toLabelStrings(id);
        return node;
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
        return new File(dir, "neostore.nodestore.db").length() /  NodeStore.RECORD_SIZE;
    }

    @Override
    public long highestRelId() {
        return new File(dir, "neostore.relationshipstore.db").length() / NodeStore.RECORD_SIZE;
    }
}
