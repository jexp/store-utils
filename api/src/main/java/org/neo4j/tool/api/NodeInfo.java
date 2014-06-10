package org.neo4j.tool.api;

import java.util.Map;

/**
* @author mh
* @since 10.06.14
*/
public class NodeInfo {
    public final long id;
    public Map<String,Object> data;
    public String[] labels;

    public NodeInfo(long id) {
        this.id = id;
    }
}
