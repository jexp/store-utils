package org.neo4j.tool.api;

/**
* @author mh
* @since 10.06.14
*/
public interface StoreReader extends StoreHandler {
    NodeInfo readNode(long id);
    RelInfo readRel(long id);
    boolean nodeExists(long node);
    long highestNodeId();
    long highestRelId();
}
