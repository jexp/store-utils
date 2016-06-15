package org.neo4j.tool;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.Iterables;

import java.io.File;
import java.util.*;

/**
 * This sample app demonstrates performance issues on Linux with small
 * transactions. The app traverses through a very simple graph with 7 nodes and
 * sets the property v of every traversed node to a random value [0, 100).
 * 
 * On Windows 7 x64 on commodity HW, we get ~1000 traversals per second (= 7000
 * setProperty calls per second). This is ok and is expected.
 * On an Arch-Linux x64 we get up to 2500 traversals/s, which is great.
 * 
 * BUT!
 * On Ubuntu 10.04 Server x64 we get around 30 traversals/s. :-(
 * On Ubuntu 10.10 we get around 50 traversals/s. :-(
 * On CentOS 5.6 we get around 25 traversals/s. :-(
 * 
 * This app can be built and run with maven: mvn compile exec:java
 * 
 * This issue has also been discussed here:
 * http://lists.neo4j.org/pipermail/user/2011-May/008822.html
 */
public class DomainAnalyzer {
	private static GraphDatabaseService graphDb;


    static class Sample {
        public final Node node;
        private int count;
        private int emptyCount;

        Sample(Node node) {
            this.node = node;
            this.count = 1;
        }

        public void inc() {
            count++;
        }
        public void incEmpty() {
            emptyCount++;
        }
        public String toString() {
            StringBuilder sb = new StringBuilder("count: ").append(count).append(" empty: ").append(emptyCount).append(" node: ").append(node.getId()).append("\n");
            for (String property : node.getPropertyKeys()) {
                final Object value = node.getProperty(property);

                sb.append("\t").append(property).append("\t").append(toString(value)).append("\n");
            }
            return sb.toString();
        }

        private String toString(Object value) {
            final Class<? extends Object> type = value.getClass();
            if (type.isArray()) {
                final Class<?> componentType = type.getComponentType();
                if (Object.class.isAssignableFrom(componentType)) return Arrays.toString((Object[]) value);
                if (int.class == componentType) return Arrays.toString((int[])value);
                if (byte.class == componentType) return Arrays.toString((byte[])value);
                if (float.class == componentType) return Arrays.toString((float[])value);
                if (double.class == componentType) return Arrays.toString((double[])value);
                if (long.class == componentType) return Arrays.toString((long[])value);
                if (char.class == componentType) return Arrays.toString((char[])value);
                if (boolean.class == componentType) return Arrays.toString((boolean[])value);
            }
            return value.toString();
        }
    }
	public static void main(String[] args) {
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(args[0]));

        long time = System.currentTimeMillis();
        Map<Set<String>,Sample> statistics = new HashMap<Set<String>, Sample>();
        int count = 0;
        for (Node node : graphDb.getAllNodes()) {
            final HashSet<String> keys = Iterables.addToCollection(node.getPropertyKeys(), new HashSet<String>());
            Sample sample = statistics.get(keys);
            if (sample==null) {
                sample = new Sample(node);
                statistics.put(keys,sample);
            } else {
                sample.inc();
            }
            count++;
            if (count % 100000 == 0) System.out.println("count = " + count + " sample "+sample);
        }
        time = System.currentTimeMillis() - time;
        System.out.println(" count " + count + " took " + time + " types " + statistics.size() + "\n" + statistics);
		graphDb.shutdown();
	}
}
