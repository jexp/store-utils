package org.neo4j.tool.api;

import java.io.IOException;

/**
* @author mh
* @since 10.06.14
*/
public interface StoreHandler {
    void init(String dir, String pageCache) throws IOException;
    void shutdown();
}
