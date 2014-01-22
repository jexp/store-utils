package org.neo4j.tool;

/**
 * @author mh
 * @since 12.08.11
 */

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import java.io.File;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * @author mh
 * @since 11.08.11
 */
public class SingleRelationshipDeletion {

    public static final int COUNT = 1000;
    private static final int ROUNDS = 50;

    public static void main(String[] args) {
        final File dir = new File("target/data");
        boolean mustCreate = !dir.exists();
        final GraphDatabaseService graphdb = new GraphDatabaseFactory().newEmbeddedDatabase(dir.getAbsolutePath());
        if (mustCreate) GraphGenerator.createDatabase(graphdb);

        for (int round = 0; round < ROUNDS; round++) {
            long[] relIds = createRelIds(COUNT);
            int success = 0, fail = 0;
            long cpuTime = System.currentTimeMillis();
            final Transaction tx = graphdb.beginTx();
            try {
                for (int i = 0; i < COUNT; i++) {
                    final Relationship rel;
                    try {
                        rel = graphdb.getRelationshipById(relIds[i]);
                    } catch (NotFoundException nfe) {
                        fail++;
                        continue;
                    }
                    try {
                        rel.delete();
                        tx.success();
                        success++;
                    } catch (NotFoundException nfe) {
                        fail++;
//                    tx.failure();
                    }
                }
            } finally {
                tx.finish();
            }

            long delta = (System.currentTimeMillis() - cpuTime);
            System.out.printf("round %d delete %d relationships time = %d ms, succ %d failed %d%n", round, COUNT, delta, success, fail);
        }
        graphdb.shutdown();
    }

    private static long[] createRelIds(final int count) {
        Random random = new Random(System.currentTimeMillis());
        Set<Integer> values = new HashSet<Integer>();
        do {
            values.add(random.nextInt(GraphGenerator.MILLION));
        } while (values.size() < count);
        final long[] result = new long[count];
        int i = 0;
        for (Integer value : values) {
            result[i++] = value.longValue();
        }
        return result;
    }

}
