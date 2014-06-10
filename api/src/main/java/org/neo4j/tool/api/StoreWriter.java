package org.neo4j.tool.api;

/**
* @author mh
* @since 10.06.14
*/
public interface StoreWriter extends StoreHandler {
    void createNode(NodeInfo node);
    void createRelationship(RelInfo rel);
}
