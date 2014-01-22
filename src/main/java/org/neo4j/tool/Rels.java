package org.neo4j.tool;

import org.neo4j.graphdb.RelationshipType;

/**
* @author mh
* @since 12.08.11
*/
enum Rels implements RelationshipType {
    RATED, USED, SUGGESTED, ACTS_IN;

    public static Rels random() {
        final int idx = (int) (Math.random() * size());
        return values()[idx];

    }

    public static int size() {
        return values().length;
    }
}
