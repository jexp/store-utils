package org.neo4j.tool;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterImpl;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.BatchRelationship;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;

public class StoreCopy {

    private static final Label[] NO_LABELS = new Label[0];
    private static PrintWriter logs;

    @SuppressWarnings("unchecked")
    public static Map<String, String> config() {
        return (Map) MapUtil.map(
                "dbms.pagecache.memory", System.getProperty("dbms.pagecache.memory","2G"),
                "cache_type", "none"
        );
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: StoryCopy source target [rel,types,to,ignore] [properties,to,ignore]");
            return;
        }
        String sourceDir = args[0];
        String targetDir = args[1];
        Set<String> ignoreRelTypes = splitOptionIfExists(args, 2);
        Set<String> ignoreProperties = splitOptionIfExists(args, 3);
        Set<String> ignoreLabels = splitOptionIfExists(args, 4);
        System.out.printf("Copying from %s to %s ingoring rel-types %s ignoring properties %s ignoring labels %s %n", sourceDir, targetDir, ignoreRelTypes, ignoreProperties,ignoreLabels);
        copyStore(sourceDir, targetDir, ignoreRelTypes, ignoreProperties,ignoreLabels);
    }

    private static Set<String> splitOptionIfExists(String[] args, final int index) {
        if (args.length <= index) return emptySet();
        return new HashSet<String>(asList(args[index].toLowerCase().split(",")));
    }

    interface Flusher {
        void flush();
    }
    private static void copyStore(String sourceDir, String targetDir, Set<String> ignoreRelTypes, Set<String> ignoreProperties, Set<String> ignoreLabels) throws Exception {
        final File target = new File(targetDir);
        final File source = new File(sourceDir);
        if (target.exists()) {
            FileUtils.deleteRecursively(target);
            // throw new IllegalArgumentException("Target Directory already exists "+target);
        }
        if (!source.exists()) throw new IllegalArgumentException("Source Database does not exist " + source);

        Pair<Long, Long> highestIds = getHighestNodeId(source);
        BatchInserter targetDb = BatchInserters.inserter(target.getAbsolutePath(), config());
        BatchInserter sourceDb = BatchInserters.inserter(source.getAbsolutePath(), config());
        Flusher flusher = getFlusher(sourceDb);

        logs = new PrintWriter(new FileWriter(new File(target, "store-copy.log")));

        copyNodes(sourceDb, targetDb, ignoreProperties, ignoreLabels, highestIds.first(),flusher);
        copyRelationships(sourceDb, targetDb, ignoreRelTypes, ignoreProperties, highestIds.other(), flusher);
        targetDb.shutdown();
        sourceDb.shutdown();
        logs.close();
        copyIndex(source, target);
    }

    private static Flusher getFlusher(BatchInserter db) {
        try {
            Field field = BatchInserterImpl.class.getDeclaredField("flushStrategy");
            field.setAccessible(true);
            final Object flushStrategy = field.get(db);
            final Method forceFlush = flushStrategy.getClass().getDeclaredMethod("forceFlush");
            forceFlush.setAccessible(true);
            return new Flusher() {
                @Override public void flush() {
                    try {
                        forceFlush.invoke(flushStrategy);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        throw new RuntimeException("Error invoking flush strategy",e);
                    }
                }
            };
        } catch (NoSuchMethodException | IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException("Error accessing flush strategy", e);
        }
    }

    private static Pair<Long, Long> getHighestNodeId(File source) {
        GraphDatabaseAPI api = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase(source.getAbsolutePath());
        IdGeneratorFactory idGenerators = api.getDependencyResolver().resolveDependency(IdGeneratorFactory.class);
        long highestNodeId = idGenerators.get(IdType.NODE).getHighestPossibleIdInUse();
        long highestRelId = idGenerators.get(IdType.RELATIONSHIP).getHighestPossibleIdInUse();
        api.shutdown();
        return Pair.of(highestNodeId, highestRelId);
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

    private static void copyRelationships(BatchInserter sourceDb, BatchInserter targetDb, Set<String> ignoreRelTypes, Set<String> ignoreProperties, long highestRelId, Flusher flusher) {
        long time = System.currentTimeMillis();
        long relId = 0;
        long notFound = 0;
        while (relId <= highestRelId) {
            BatchRelationship rel = null;
            String type = null;
            try {
                rel = sourceDb.getRelationshipById(relId);
                type = rel.getType().name();
                relId++;
                if (!ignoreRelTypes.contains(type.toLowerCase())) {
                    createRelationship(targetDb, sourceDb, rel, ignoreProperties);
                }
                if (relId % 10000 == 0) {
                    System.out.print(".");
                }
                if (relId % 500000 == 0) {
                    flusher.flush();
                    System.out.println(" " + relId + " / " + highestRelId + " (" + 100 *((float)relId / highestRelId) + "%)");
                }
            } catch (Exception e) {
                if (e instanceof org.neo4j.kernel.impl.store.InvalidRecordException && e.getMessage().endsWith("not in use")) {
                   notFound++;
                } else {
                   addLog(rel, "copy Relationship: " + (relId - 1) + "-[:" + type + "]" + "->?", e.getMessage());
                }
            }
        }
        System.out.println("\n copying of "+relId+" relationship records took "+(System.currentTimeMillis()-time)+" ms. Unused Records "+notFound);
    }

    private static long firstNode(BatchInserter sourceDb, long highestNodeId) {
        int node = -1;
        while (++node <= highestNodeId) {
            if (sourceDb.nodeExists(node)) return node;
        }
        return -1;
    }

    private static void createRelationship(BatchInserter targetDb, BatchInserter sourceDb, BatchRelationship rel, Set<String> ignoreProperties) {
        long startNodeId = rel.getStartNode();
        long endNodeId = rel.getEndNode();
        final RelationshipType type = rel.getType();
        try {
            Map<String, Object> props = getProperties(sourceDb.getRelationshipProperties(rel.getId()), ignoreProperties);
//            if (props.isEmpty()) props = Collections.<String,Object>singletonMap("old_id",rel.getId()); else props.put("old_id",rel.getId());
            targetDb.createRelationship(startNodeId, endNodeId, type, props);
        } catch (Exception e) {
            addLog(rel, "create Relationship: " + startNodeId + "-[:" + type + "]" + "->" + endNodeId, e.getMessage());
        }
    }

    private static void copyNodes(BatchInserter sourceDb, BatchInserter targetDb, Set<String> ignoreProperties, Set<String> ignoreLabels, long highestNodeId, Flusher flusher) {
        long time = System.currentTimeMillis();
        int node = 0;
        long notFound = 0;
        while (node <= highestNodeId) {
            try {
              if (sourceDb.nodeExists(node)) {
                 targetDb.createNode(node, getProperties(sourceDb.getNodeProperties(node), ignoreProperties), labelsArray(sourceDb, node, ignoreLabels));
              }
              node++;
                if (node % 10000 == 0) {
                    System.out.print(".");
                }
                if (node % 500000 == 0) {
                    flusher.flush();
                    logs.flush();
                    System.out.println(" " + node + " / " + highestNodeId + " (" + 100 *((float)node / highestNodeId) + "%)");
                }
            }
            catch (Exception e) {
                if (e instanceof org.neo4j.kernel.impl.store.InvalidRecordException && e.getMessage().endsWith("not in use")) {
                    notFound += 1;
                } else addLog(node, e.getMessage());
            }
        }
        System.out.println("\n copying of " + node + " node records took " + (System.currentTimeMillis() - time) + " ms. Unused Records " + notFound);
    }

    private static Label[] labelsArray(BatchInserter db, long node, Set<String> ignoreLabels) {
        Collection<Label> labels = IteratorUtil.asCollection(db.getNodeLabels(node));
        if (labels.isEmpty()) return NO_LABELS;
        if (!ignoreLabels.isEmpty()) {
            for (Iterator<Label> it = labels.iterator(); it.hasNext(); ) {
                Label label = it.next();
                if (ignoreLabels.contains(label.name().toLowerCase())) {
                    it.remove();
                }
            }
        }
        return labels.toArray(new Label[labels.size()]);
    }

    private static Map<String, Object> getProperties(Map<String, Object> pc, Set<String> ignoreProperties) {
        if (!ignoreProperties.isEmpty()) pc.keySet().removeAll(ignoreProperties);
        return pc;
    }

    private static void addLog(BatchRelationship rel, String property, String message) {
        logs.append(String.format("%s.%s %s%n", rel, property, message));
    }

    private static void addLog(long node, String message) {
        logs.append(String.format("Node: %s %s%n", node, message));
    }

    private static void addLog(PropertyContainer pc, String property, String message) {
        logs.append(String.format("%s.%s %s%n", pc, property, message));
    }
}
