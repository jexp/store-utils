package org.neo4j.tool;

import org.apache.commons.io.FileUtils;
import org.neo4j.tool.api.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;

public class StoreCopyRevert {

    public static final String[] NO_LABELS = new String[0];
    private static PrintWriter logs;

    private static Map<String,Integer> stats = new TreeMap<>();

    private static void record(String type,String...labels) {
        for (String label : labels) {
            label = type + ":"+label;
            if (!stats.containsKey(label)) stats.put(label,1);
            else stats.put(label,stats.get(label)+1);
        }
    }
    private static void record(String type, Iterable<String> labels) {
        for (String label : labels) {
            label = type + ":"+label;
            if (!stats.containsKey(label)) stats.put(label,1);
            else stats.put(label,stats.get(label)+1);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: StoryCopyRevert source:version target:version [rel,types,to,ignore] [properties,to,ignore]");
            return;
        }
        String[] sourceDir = args[0].split(":");
        String[] targetDir = args[1].split(":");
        Set<String> ignoreRelTypes = splitOptionIfExists(args, 2);
        Set<String> ignoreProperties = splitOptionIfExists(args, 3);
        Set<String> ignoreLabels = splitOptionIfExists(args, 4);
        System.out.printf("Copying from %s version %s to %s version %s ingoring rel-types %s ignoring properties %s ignoring labels %s %n", sourceDir[0],sourceDir[1], targetDir[0],targetDir[1], ignoreRelTypes, ignoreProperties,ignoreLabels);
        copyStore(sourceDir[0],sourceDir[1], targetDir[0],targetDir[1], ignoreRelTypes, ignoreProperties,ignoreLabels);
    }

    private static Set<String> splitOptionIfExists(String[] args, final int index) {
        if (args.length <= index) return emptySet();
        return new HashSet<String>(asList(args[index].toLowerCase().split(",")));
    }

    private static void copyStore(String sourceDir, String fromVersion, String targetDir, String toVersion, Set<String> ignoreRelTypes, Set<String> ignoreProperties, Set<String> ignoreLabels) throws Exception {
        final File target = new File(targetDir);
        final File source = new File(sourceDir);
        if (target.exists()) {
            FileUtils.deleteDirectory(target);
            // throw new IllegalArgumentException("Target Directory already exists "+target);
        }
        FileUtils.forceMkdir(target);
        logs = new PrintWriter(new FileWriter(new File(target, "store-copy.log")));
        if (!source.exists()) throw new IllegalArgumentException("Source Database does not exist " + source);

        StoreReader sourceDb = null;
        StoreWriter targetDb = null;
        try {
            sourceDb = createInstance("org.neo4j.tool.impl.StoreBatchReaderImpl", fromVersion);
            String sourcePageCache = System.getProperty("source.page.cache","1g");;
            sourceDb.init(source.getAbsolutePath(), sourcePageCache);
            targetDb = createInstance("org.neo4j.tool.impl.StoreBatchWriterImpl", toVersion);
            String targetPageCache = System.getProperty("target.page.cache",sourcePageCache);
            targetDb.init(target.getAbsolutePath(), targetPageCache);

            copyNodes(sourceDb, targetDb, ignoreProperties, ignoreLabels);
            copyRelationships(sourceDb, targetDb, ignoreRelTypes, ignoreProperties);

        } finally {
            if (targetDb!=null) {
                try {
                    targetDb.shutdown();
                } catch (Exception e) {
                    System.err.println("Error shutting down target database");
                    e.printStackTrace();
                }
            }
            if (sourceDb!=null) {
                try {
                    sourceDb.shutdown();
                } catch (Exception e) {
                    System.err.println("Error shutting down source database");
                    e.printStackTrace();
                }
            }
            printStats();
            logs.close();
            copyIndex(source, target);
        }
    }

    public static <T extends StoreHandler> T createInstance(String name, String version) {
        try {
            URLClassLoader classLoader = new URLClassLoader(new URL[] {jar(version).toURL()},StoreCopyRevert.class.getClassLoader());
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

    private static void copyRelationships(StoreReader sourceDb, StoreWriter targetDb, Set<String> ignoreRelTypes, Set<String> ignoreProperties) {
        long highestRelId = sourceDb.highestRelId();
        long time = System.currentTimeMillis();
        long relId = 0;
        long notFound = 0;
        while (relId <= highestRelId) {
            RelInfo rel = null;
            try {
                rel = sourceDb.readRel(relId++);
                record("TYPE",rel.type);
                rel.data = nonIgnoredProperties(rel.data, ignoreProperties);
            } catch (Exception nfe) {
                addLog(rel,"not-found",nfe.getMessage());
                notFound++;
                continue;
            }
            if (ignoreRelTypes.contains(rel.type.toLowerCase())) continue;
            createRelationship(targetDb, rel);
            if (relId % 1000 == 0) System.out.print(".");
            if (relId % 100000 == 0) System.out.println(" " + relId);
        }
        System.out.println("\n copying of "+relId+" relationships took "+(System.currentTimeMillis()-time)+" ms. Not found "+notFound);
    }

    private static void createRelationship(StoreWriter targetDb, RelInfo rel) {
        try {
            targetDb.createRelationship(rel);
        } catch (Exception ire) {
            addLog(rel, "create Relationship: " + rel.from + "-[:" + rel.type + "]" + "->" + rel.to, ire.getMessage());
        }
    }

    private static void copyNodes(StoreReader sourceDb, StoreWriter targetDb, Set<String> ignoreProperties, Set<String> ignoreLabels) {
        long highestNodeId = sourceDb.highestNodeId();
        long time = System.currentTimeMillis();
        int id = -1;
        while (++id <= highestNodeId) {
            if (!sourceDb.nodeExists(id)) continue;
            NodeInfo node = sourceDb.readNode(id);
            node.data = nonIgnoredProperties(node.data, ignoreProperties);
            node.labels = nonIgnoredLabels(node.labels, ignoreLabels);
            targetDb.createNode(node);
            if (id % 1000 == 0) System.out.print(".");
            if (id % 100000 == 0) {
                logs.flush();
                System.out.println(" " + id);
            }
        }
        System.out.println("\n copying of " + id + " nodes took " + (System.currentTimeMillis() - time) + " ms.");
    }

    private static void printStats() {
        for (Map.Entry<String, Integer> entry : stats.entrySet()) {
            System.out.printf("%10s %-5d%n", entry.getKey(), entry.getValue());
        }
    }

    private static String[] nonIgnoredLabels(String[] labels, Set<String> ignoreLabels) {
        if (labels==null || labels.length==0) return NO_LABELS;
        if (ignoreLabels.isEmpty()) return labels;
        String[] result = new String[labels.length];
        int i=0;
        for (String label : labels) {
            if (ignoreLabels.contains(label)) continue;
            result[i++] = label;
        }
        result = Arrays.copyOf(result, i);
        record("Label",result);
        return result;
    }

    private static Map<String, Object> nonIgnoredProperties(Map<String, Object> pc, Set<String> ignoreProperties) {
        if (!ignoreProperties.isEmpty()) pc.keySet().removeAll(ignoreProperties);
        record("prop",pc.keySet());
        return pc;
    }

    private static void addLog(RelInfo rel, String property, String message) {
        logs.append(String.format("%s.%s %s%n", rel, property, message));
    }

    private static void addLog(NodeInfo pc, String property, String message) {
        logs.append(String.format("%s.%s %s%n", pc, property, message));
    }
}
