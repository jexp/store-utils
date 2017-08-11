package org.neo4j.tool.api;

import java.util.Map;

/**
* @author mh
* @since 10.06.14
*/
public class RelInfo {
    public long id = -1;
    public long from;
    public long to;
    public Map<String,Object> data;
    public String type;

    public RelInfo(long id) {
        this.id = id;
    }

    public String toString() {
        return from + "-[:" + type + "]" + "->" + to;
    }
}
