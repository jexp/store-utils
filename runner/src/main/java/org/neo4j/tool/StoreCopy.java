package org.neo4j.tool;

import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.LongLongScatterMap;
import org.apache.commons.io.FileUtils;
import org.neo4j.tool.api.*;

import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;

public class StoreCopy {

    private static final File PROP_FILE = new File("neo4j.properties");
    private static PrintWriter logs;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: StoryCopy source target [rel,types,to,ignore] [properties,to,ignore] [labels,to,ignore] [labels,to,delete]");
            return;
        }
        Properties properties = new Properties();
        if (PROP_FILE.exists()) properties.load(new FileReader(PROP_FILE));
        String[] sourceDir = getArgument(args,0,properties,"source_db_dir").split(":");
        String[] targetDir = getArgument(args,1,properties,"target_db_dir").split(":");

        Set<String> ignoreRelTypes = splitToSet(getArgument(args,2,properties,"rel_types_to_ignore"));
        Set<String> ignoreProperties = splitToSet(getArgument(args,3,properties,"properties_to_ignore"));
        Set<String> ignoreLabels = splitToSet(getArgument(args,4,properties,"labels_to_ignore"));
        Set<String> deleteNodesWithLabels = splitToSet(getArgument(args,5,properties,"labels_to_delete"));
        String keepNodeIdsParam = getArgument(args, 6, properties, "keep_node_ids");
        boolean keepNodeIds = !("false".equalsIgnoreCase(keepNodeIdsParam));
        System.out.printf("Copying from %s version %s to %s version %s ingoring rel-types %s ignoring properties %s ignoring labels %s removing nodes with labels %s keep node ids %s %n", sourceDir[0], sourceDir[1], targetDir[0], targetDir[1], ignoreRelTypes, ignoreProperties,ignoreLabels, deleteNodesWithLabels,keepNodeIds);
        copyStore(sourceDir[0],sourceDir[1], targetDir[0],targetDir[1], ignoreRelTypes, ignoreProperties,ignoreLabels,deleteNodesWithLabels, keepNodeIds);
    }

    private static String getArgument(String[] args, int index, Properties properties, String key) {
        if (args.length > index) return args[index];
        return properties.getProperty(key);
    }

    private static Set<String> splitToSet(String value) {
        if (value == null || value.trim().isEmpty()) return emptySet();
        return new HashSet<>(asList(value.trim().split(", *")));
    }

    interface Flusher {
        void flush();
    }
    private static void copyStore(String sourceDir, String fromVersion, String targetDir, String toVersion, Set<String> ignoreRelTypes, Set<String> ignoreProperties, Set<String> ignoreLabels, Set<String> deleteNodesWithLabels, boolean stableNodeIds) throws Exception {
        final File source = new File(sourceDir);
        if (!source.exists()) throw new IllegalArgumentException("Source Database does not exist " + source);

        final File target = new File(targetDir);
        if (target.exists()) {
            throw new IllegalArgumentException("Target Directory already exists "+target);
        } else {
            if (!target.mkdirs()) throw new IOException("Could not create "+targetDir);
        }

        logs = new PrintWriter(new FileWriter(new File(target, "store-copy.log")));

        String targetPageCache = System.getProperty("dbms.pagecache.memory","1g");
        String sourcePageCache = System.getProperty("dbms.pagecache.memory.source",targetPageCache);;

        StoreReader sourceDb = null;
        StoreWriter targetDb = null;
        try {
            sourceDb = createInstance("org.neo4j.tool.impl.StoreBatchReaderImpl", fromVersion);
            sourceDb.init(source.getAbsolutePath(), sourcePageCache);
            targetDb = createInstance("org.neo4j.tool.impl.StoreBatchWriterImpl", toVersion);
            targetDb.init(target.getAbsolutePath(), targetPageCache);

            LongLongMap copiedNodeIds = copyNodes(sourceDb, targetDb, ignoreProperties, ignoreLabels, deleteNodesWithLabels, stableNodeIds);
            copyRelationships(sourceDb, targetDb, ignoreRelTypes, ignoreProperties, copiedNodeIds);
        } finally {
            if (targetDb != null) {
                try {
                    targetDb.shutdown();
                } catch (Exception e) {
                    System.err.println("Error shutting down target database");
                    e.printStackTrace();
                }
            }
            if (sourceDb != null) {
                try {
                    sourceDb.shutdown();
                } catch (Exception e) {
                    logs.append(String.format("Noncritical error closing the source database:%n%s", stringify(e)));
                }
            }
        }
        logs.close();
        copyIndex(source, target);
    }

    private static String stringify(Throwable e) {
        StringBuilder sb = new StringBuilder();
        do {
            sb.append(e.getMessage());
            e = e.getCause();
        } while (e != null && e != e.getCause());
        return sb.toString();
    }


    public static <T extends StoreHandler> T createInstance(String name, String version) {
        try {
            URLClassLoader classLoader = new URLClassLoader(new URL[] {jar(version).toURL()},StoreCopy.class.getClassLoader());
            Class<?> targetClass = classLoader.loadClass(name);
            return (T)targetClass.newInstance();
        } catch(Exception e) {
            throw new RuntimeException("Error loading class "+name,e);
        }
    }

    private static File jar(String version) {
        return new File(System.getProperty("user.home"), ".m2/repository/org/neo4j/util/store-util-impl-"+version+"/3.2.0/store-util-impl-"+version+"-3.2.0.jar");
    }

    private static void copyIndex(File source, File target) throws IOException {
        final File indexFile = new File(source, "index.db");
        if (indexFile.exists()) {
            FileUtils.copyFile(indexFile, new File(target, "index.db"));
        }
        final File indexDir = new File(source, "index");
        if (indexDir.exists()) {
            FileUtils.copyDirectory(indexDir, new File(target, "index"));
        }
    }

    private static void copyRelationships(StoreReader sourceDb, StoreWriter targetDb, Set<String> ignoreRelTypes, Set<String> ignoreProperties, LongLongMap copiedNodeIds) {
        long time = System.currentTimeMillis();
        long relId = 0;
        long notFound = 0;
        long removed = 0;
        long highestRelId = sourceDb.highestRelId();
        while (relId <= highestRelId) {
            RelInfo rel = null;
            String type = null;
            try {
                rel = sourceDb.readRel(relId);
                type = rel.type;
                if (!ignoreRelTypes.contains(type)) {
                    if (!createRelationship(targetDb, sourceDb, rel, ignoreProperties, copiedNodeIds)) {
                        removed++;
                    }
                } else {
                    removed++;
                }
            } catch (Exception e) {
                if (notInUse(e)) {
                    notFound++;
                } else {
                    addLog(rel, "copy Relationship: " + relId + "-[:" + type + "]" + "->?", e.getMessage());
                }
            }
            relId++;
            if (relId % 10_000 == 0) {
                System.out.print(".");
            }
            if (relId % 500_000 == 0) {
                sourceDb.flush();
            }
            if (relId % 1_000_000 == 0) {
                logs.flush();
                System.out.printf(" %d / %d (%d%%) unused %d removed %d %d seconds%n", relId, highestRelId, percent(relId,highestRelId), notFound,removed, secondsSince(time));
            }
        }
        time = Math.max(1,(System.currentTimeMillis() - time)/1000);
        System.out.printf("%n copying of %d relationship records took %d seconds (%d rec/s). Unused Records %d (%d%%) Removed Records %d (%d%%)%n",
                relId, time, relId/time, notFound, percent(notFound,relId),removed, percent(removed,relId));
    }

    private static long secondsSince(long time) {
        return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()-time);
    }

    private static boolean notInUse(Exception e) {
        return e.getClass().getName().endsWith("InvalidRecordException") && e.getMessage().endsWith("not in use");
    }

    private static int percent(Number part, Number total) {
        return (int) (100 * part.floatValue() / total.floatValue());
    }

    private static boolean createRelationship(StoreWriter targetDb, StoreReader sourceDb, RelInfo rel, Set<String> ignoreProperties, LongLongMap copiedNodeIds) {
        rel.from = copiedNodeIds.get(rel.from);
        rel.to = copiedNodeIds.get(rel.to);
        if (rel.from == -1L || rel.to == -1L) return false;
        try {
            rel.data = getProperties(rel.data, ignoreProperties);
            targetDb.createRelationship(rel);
            return true;
        } catch (Exception e) {
            addLog(rel, "create Relationship: " + rel.toString(), e.getMessage());
            return false;
        }
    }

    private static LongLongMap copyNodes(StoreReader sourceDb, StoreWriter targetDb, Set<String> ignoreProperties, Set<String> ignoreLabels, Set<String> deleteNodesWithLabels, boolean stableNodeIds) {
        LongLongMap copiedNodes = new LongLongHashMap((int)sourceDb.highestNodeId());
        long time = System.currentTimeMillis();
        long nodeId = 0;
        long notFound = 0;
        long removed = 0;
        long highestNodeId = sourceDb.highestNodeId();
        while (nodeId <= highestNodeId) {
            try {
                if (sourceDb.nodeExists(nodeId)) {
                    NodeInfo node = sourceDb.readNode(nodeId);
                    if (labelInSet(node.labels,deleteNodesWithLabels)) {
                        removed ++;
                    } else {
                        node.data = getProperties(node.data, ignoreProperties);
                        node.labels = labelsArray(node, ignoreLabels);
                        if (!stableNodeIds) {
                            node.id = -1;
                        }
                        targetDb.createNode(node);
                        copiedNodes.put(nodeId,node.id);
                    }
                } else {
                    notFound++;
                }
            } catch (Exception e) {
                if (notInUse(e)) {
                    notFound++;
                } else addLog(nodeId, e.getMessage());
            }
            nodeId++;
            if (nodeId % 10_000 == 0) {
                System.out.print(".");
            }
            if (nodeId % 500_000 == 0) {
                sourceDb.flush();
            }
            if (nodeId % 1_000_000 == 0) {
                logs.flush();
                System.out.printf(" %d / %d (%d%%) unused %d removed %d seconds %d%n", nodeId, highestNodeId, percent(nodeId,highestNodeId), notFound, removed, secondsSince(time));
            }
        }
        time = Math.max(1,(System.currentTimeMillis() - time)/1000);
        System.out.printf("%n copying of %d node records took %d seconds (%d rec/s). Unused Records %d (%d%%). Removed Records %d (%d%%).%n",
                nodeId, time, nodeId/time, notFound, percent(notFound,nodeId),removed, percent(removed,nodeId));
        return copiedNodes;
    }

    private static boolean labelInSet(String[] nodeLabels, Set<String> labelSet) {
        if (labelSet == null || labelSet.isEmpty()) return false;
        for (String nodeLabel : nodeLabels) {
            if (labelSet.contains(nodeLabel)) return true;
        }
        return false;
    }

    private static String[] labelsArray(NodeInfo node, Set<String> ignoreLabels) {
        String[] labels = node.labels;
        if (labels == null || labels.length == 0 || ignoreLabels.isEmpty()) return node.labels;
        int idx = 0;
        for (int i = 0; i < labels.length; i++) {
            String label = labels[i];
            if (ignoreLabels.contains(label)) {
                labels[idx++] = label;
            }
        }
        return idx < labels.length ? Arrays.copyOf(labels, idx) : labels;
    }

    private static Map<String, Object> getProperties(Map<String, Object> pc, Set<String> ignoreProperties) {
        if (pc.isEmpty()) return Collections.emptyMap();
        if (ignoreProperties.isEmpty()) return pc;
        pc.keySet().removeAll(ignoreProperties);
        return pc;
    }

    private static void addLog(RelInfo rel, String property, String message) {
        logs.append(String.format("%s.%s %s%n", rel, property, message));
    }

    private static void addLog(long node, String message) {
        logs.append(String.format("Node: %s %s%n", node, message));
    }

    private static void addLog(NodeInfo pc, String property, String message) {
        logs.append(String.format("%s.%s %s%n", pc, property, message));
    }
}
