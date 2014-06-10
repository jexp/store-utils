package org.neo4j.tool.api;

import java.util.Map;

/**
* @author mh
* @since 10.06.14
*/
public interface StoreHandler {
    void init(String dir, Map<String, String> config);
    void shutdown();
}
