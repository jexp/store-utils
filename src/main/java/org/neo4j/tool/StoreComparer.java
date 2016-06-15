package org.neo4j.tool;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;

public class StoreComparer {

    @SuppressWarnings("unchecked")
    public static Map<String, String> config() {
        return (Map) MapUtil.map(
                "neostore.nodestore.db.mapped_memory", "100M",
                "neostore.relationshipstore.db.mapped_memory", "500M",
                "neostore.propertystore.db.mapped_memory", "300M",
                "neostore.propertystore.db.strings.mapped_memory", "1G",
                "neostore.propertystore.db.arrays.mapped_memory", "300M",
                "neostore.propertystore.db.index.keys.mapped_memory", "100M",
                "neostore.propertystore.db.index.mapped_memory", "100M",
                "cache_type", "weak"
        );
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: StoreComparer source target [rel,types,to,ignore] [properties,to,ignore]");
            return;
        }
        String sourceDir = args[0];
        String targetDir = args[1];
        Set<String> ignoreRelTypes = splitOptionIfExists(args, 2);
        Set<String> ignoreProperties = splitOptionIfExists(args, 3);
        System.out.printf("Copying from %s to %s ingoring rel-types %s ignoring properties %s %n", sourceDir, targetDir, ignoreRelTypes, ignoreProperties);
        compareStore(sourceDir, targetDir, ignoreRelTypes, ignoreProperties);
    }

    private static Set<String> splitOptionIfExists(String[] args, final int index) {
        if (args.length <= index) return emptySet();
        return new HashSet<String>(asList(args[index].toLowerCase().split(",")));
    }

    private static void compareStore(String sourceDir, String targetDir, Set<String> ignoreRelTypes, Set<String> ignoreProperties) throws Exception {
        final File target = new File(targetDir);
        final File source = new File(sourceDir);
        if (!target.exists()) throw new IllegalArgumentException("Target Directory does not exists " + target);
        if (!source.exists()) throw new IllegalArgumentException("Source Database does not exist " + source);

        GraphDatabaseService targetDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(target).setConfig(config()).newGraphDatabase();
        GraphDatabaseService sourceDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(source).setConfig(config()).newGraphDatabase();

        try (Transaction srcDbTx = sourceDb.beginTx();
             Transaction targetDbTx = targetDb.beginTx()) {
            compareCounts(sourceDb, targetDb, ignoreRelTypes, ignoreProperties);
            compareNodes(sourceDb, targetDb, ignoreProperties);
            compareRelationships(sourceDb, targetDb, ignoreRelTypes, ignoreProperties);
        }

        targetDb.shutdown();
        sourceDb.shutdown();
        copyIndex(source, target);
    }

    private static void compareCounts(GraphDatabaseService sourceDb, GraphDatabaseService targetDb, Set<String> ignoreRelTypes, Set<String> ignoreProperties) {
        long time = System.currentTimeMillis();
        final Statistics sourceStatistics = count(sourceDb, ignoreRelTypes, ignoreProperties);
        final Statistics targetStatistics = count(targetDb, ignoreRelTypes, ignoreProperties);
        if (!sourceStatistics.equals(targetStatistics)) {
            System.err.println("Count difference");
            System.err.println("Source " + sourceStatistics);
            System.err.println("Target " + targetStatistics);
        }
        System.out.println("\n comparing of " + "counts" + " took " + (System.currentTimeMillis() - time) + " ms.");
    }

    private static Statistics count(GraphDatabaseService db, Set<String> ignoreRelTypes, Set<String> ignoreProperties) {
        Statistics statistics = new Statistics();
        int count = 0;
        for (Node node : db.getAllNodes()) {
            statistics.nodeCount++;
            statistics.nodeProperties += countProperties(ignoreProperties, node);
            for (Relationship rel : node.getRelationships(Direction.OUTGOING)) {
                if (ignoreRelTypes.contains(rel.getType().name().toLowerCase())) continue;
                statistics.relationshipCount++;
                statistics.relationshipPropertyCount += countProperties(ignoreProperties, rel);
                count++;
                if (count % 1000 == 0) System.out.print(".");
                if (count % 100000 == 0) System.out.println(" " + count);
            }
        }
        return statistics;
    }

    private static int countProperties(Set<String> ignoreProperties, PropertyContainer node) {
        final Collection<String> keys = Iterables.addToCollection(node.getPropertyKeys(), new HashSet<String>());
        keys.removeAll(ignoreProperties);
        return keys.size();
    }

    private static void copyIndex(File source, File target) throws IOException {
        final File indexFile = new File(source, "index.db");
        if (indexFile.exists()) {
            FileUtils.copyFile(indexFile, new File(target, "index.db"));
        }
        final File indexDir = new File(source, "index");
        if (indexDir.exists()) {
            FileUtils.copyRecursively(indexDir, new File(target, "index"));
        }
    }


    private static void compareRelationships(GraphDatabaseService sourceDb, GraphDatabaseService targetDb, Set<String> ignoreRelTypes, Set<String> ignoreProperties) {
        long time = System.currentTimeMillis();
        int count = 0;
        for (Node node : sourceDb.getAllNodes()) {
            compareProperties(node, targetDb.getNodeById(node.getId()), ignoreProperties);
            for (Relationship rel : node.getRelationships(Direction.OUTGOING)) {
                if (ignoreRelTypes.contains(rel.getType().name().toLowerCase())) continue;
                final Relationship targetRel = getTargetRel(targetDb, node.getId(), rel.getOtherNode(node).getId(), rel.getType());
                compareProperties(rel, targetRel,ignoreProperties);
                count++;
                if (count % 1000 == 0) System.out.print(".");
                if (count % 100000 == 0) System.out.println(" " + count);
            }
        }
        System.out.println("\n copying of " + count + " relationships took " + (System.currentTimeMillis() - time) + " ms.");
    }

    private static Relationship getTargetRel(GraphDatabaseService gdb,long startNodeId, long endNodeId, RelationshipType relType) {
        final Node start = gdb.getNodeById(startNodeId);
        final Node endNode = gdb.getNodeById(endNodeId);
        for (Relationship relationship : start.getRelationships(relType, Direction.OUTGOING)) {
            if (relationship.getOtherNode(start).equals(endNode)) return relationship;
        }
        return null;
    }

    private static void compareProperties(PropertyContainer pc1, PropertyContainer pc2, Set<String> ignoreProperties) {
        final Collection<String> keys1 = Iterables.addToCollection(pc1.getPropertyKeys(), new HashSet<String>());
        final Collection<String> keys2 = Iterables.addToCollection(pc2.getPropertyKeys(), new HashSet<String>());
        keys2.removeAll(ignoreProperties);
        keys1.removeAll(ignoreProperties);
        if (!keys1.equals(keys2)) {
            System.err.println("On " + pc1 + " != " + pc2 + " properties mismatch " + keys1 + " != " + keys2);
        }
        for (String prop : keys1) {
            final Object value1 = pc1.getProperty(prop);
            final Object value2 = pc2.getProperty(prop);
            if (!equals(value1, value2)) {
                System.err.println("On " + pc1 + " != " + pc2 + " property " + prop + " mismatch " + toString(value1) + " != " + toString(value2));
            }
        }
    }

    private static String toString(Object value) {
        if (value==null) return "null";
        return value.getClass().isArray() ? Arrays.deepToString((Object[]) value) : value.toString();
    }

    private static boolean equals(Object value1, Object value2) {
        if (value1==null && value2==null) return true;
        if (value1==null || value2==null) return false;
        final Class<?> type = value1.getClass();
        if (type.isArray()) {
            if (type.getComponentType().equals(byte.class)) return Arrays.equals((byte[])value1,(byte[])value2);
            if (type.getComponentType().equals(char.class)) return Arrays.equals((char[])value1,(char[])value2);
            if (type.getComponentType().equals(int.class)) return Arrays.equals((int[])value1,(int[])value2);
            if (type.getComponentType().equals(long.class)) return Arrays.equals((long[])value1,(long[])value2);
            if (type.getComponentType().equals(float.class)) return Arrays.equals((float[])value1,(float[])value2);
            if (type.getComponentType().equals(double.class)) return Arrays.equals((double[])value1,(double[])value2);
            if (type.getComponentType().equals(boolean.class)) return Arrays.equals((boolean[])value1,(boolean[])value2);
            if (type.getComponentType().equals(String.class)) return Arrays.equals((String[])value1,(String[])value2);
            return Arrays.equals((Object[])value1,(Object[])value2);
        }
        return value1.equals(value2);
    }

    private static void compareNodes(GraphDatabaseService sourceDb, GraphDatabaseService targetDb, Set<String> ignoreProperties) {
        long time = System.currentTimeMillis();
        int count = 0;
        for (Node node : sourceDb.getAllNodes()) {
            compareProperties(node, targetDb.getNodeById(node.getId()), ignoreProperties);
            count++;
            if (count % 1000 == 0) System.out.print(".");
            if (count % 100000 == 0) System.out.println(" " + count);
        }

        System.out.println("\n comparing of " + count + " nodes took " + (System.currentTimeMillis() - time) + " ms.");
    }

    private static Map<String, Object> getProperties(PropertyContainer pc, Set<String> ignoreProperties) {
        Map<String, Object> result = new HashMap<String, Object>();
        for (String property : pc.getPropertyKeys()) {
            if (ignoreProperties.contains(property.toLowerCase())) continue;
            result.put(property, pc.getProperty(property));
        }
        return result;
    }

    private static class Statistics {
        public int nodeCount;
        public int nodeProperties;
        public int relationshipCount;
        public int relationshipPropertyCount;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Statistics that = (Statistics) o;

            if (nodeCount != that.nodeCount) {
                System.err.println("nodes " + nodeCount + " != " + that.nodeCount);
                return false;
            }
            if (nodeProperties != that.nodeProperties) {
                System.err.println("nodeProperties " + nodeProperties + " != " + that.nodeProperties);
                return false;
            }
            if (relationshipCount != that.relationshipCount) {
                System.err.println("relationshipCount " + relationshipCount + " != " + that.relationshipCount);
                return false;
            }
            if (relationshipPropertyCount != that.relationshipPropertyCount) {
                System.err.println("relationshipPropertyCount " + relationshipPropertyCount + " != " + that.relationshipPropertyCount);
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = nodeCount;
            result = 31 * result + nodeProperties;
            result = 31 * result + relationshipCount;
            result = 31 * result + relationshipPropertyCount;
            return result;
        }

        @Override
        public String toString() {
            return String.format("Statistics{nodes=%d, nodeProperties=%d, relationshipCount=%d, relationshipPropertyCount=%d}", nodeCount, nodeProperties, relationshipCount, relationshipPropertyCount);
        }
    }
}
