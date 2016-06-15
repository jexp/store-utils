package org.neo4j.tool;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.util.Arrays;

/**
 * @author mh
 * @since 12.08.11
 */
public class GraphGenerator {
    public static final int MILLION = 1000 * 1000;

    public static void main(String[] args) {
        final GraphDatabaseService gdb = new GraphDatabaseFactory().newEmbeddedDatabase(new File("target/data"));
        createDatabase(gdb);
        gdb.shutdown();
    }
    public static void createDatabase(GraphDatabaseService graphdb) {
        int [] largeArray = new int[5000];
        Arrays.fill(largeArray, 101);
        long cpuTime = System.currentTimeMillis();
        Transaction tx = graphdb.beginTx();
        try {
            Node last = null, node = null;
            for (int i = 0; i < MILLION; i++) {
                node = graphdb.createNode();
                if (last != null) {
                    final Relationship rel = last.createRelationshipTo(node, Rels.values()[i % Rels.size()]);
                    rel.setProperty("array",largeArray);
                }
                last = node;
                if ((i % 100) == 0) {
                    System.out.print(".");
                    if ((i % 10000) == 0) {
                        tx.success();
                        tx.close();
                        System.out.println(" " + i);
                        tx = graphdb.beginTx();
                    }
                }
            }
        } finally {
            tx.success();
            tx.close();
        }
        System.out.println();
        long delta = (System.currentTimeMillis() - cpuTime);
        System.out.println("create-db delta = " + delta);
    }
}
