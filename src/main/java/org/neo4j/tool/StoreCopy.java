package org.neo4j.tool;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongLongMap;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.Token;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.values.storable.Value;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.function.Function;

public class StoreCopy {

    private static final int[] NO_IDS = new int[0];
    private static final Label[] NO_LABELS = new Label[0];
    private static PrintWriter logs;

    public static void main(String[] args) throws Exception {
        StoreCopyConfig config = new StoreCopyConfig(args);
        if (!config.isValid()) return;

        copyStore(config);
    }

    private static void copyStore(StoreCopyConfig config) throws Exception {
        final File target = new File(config.targetDir);
        final File source = new File(config.sourceDir);
        if (target.exists()) {
            FileUtils.deleteRecursively(target);
            // throw new IllegalArgumentException("Target Directory already exists "+target);
        }
        if (!source.exists()) throw new IllegalArgumentException("Source Database does not exist " + source);

        String pageCacheSize = System.getProperty("dbms.pagecache.memory", "2G");
        Map<String, String> targetConfig = MapUtil.stringMap("dbms.pagecache.memory", pageCacheSize);
        BatchInserter targetDb = BatchInserters.inserter(target, targetConfig);
        Map<String, String> sourceConfig = MapUtil.stringMap("dbms.pagecache.memory", System.getProperty("dbms.pagecache.memory.source", pageCacheSize));

        GraphDatabaseAPI sourceDb = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(source).setConfig(sourceConfig).newGraphDatabase();
        Pair<Long, Long> highestIds = getHighestNodeId(sourceDb);

        logs = new PrintWriter(new FileWriter(new File(target, "store-copy.log")));

        int[] ignorePropertyIds = readFromDb(sourceDb, (ro) -> TokenType.PropertyKeys.resolve(ro, config.ignoreProperties));
        int[] ignoreLabelIds = readFromDb(sourceDb, (ro) -> TokenType.Labels.resolve(ro, config.ignoreLabels));
        int[] ignoreRelTypeIds = readFromDb(sourceDb, (ro) -> TokenType.RelTypes.resolve(ro, config.ignoreRelTypes));
        int[] deleteNodesWithLabelsIds = readFromDb(sourceDb, (ro) -> TokenType.Labels.resolve(ro, config.deleteNodesWithLabels));
        PrimitiveLongLongMap copiedNodeIds = copyNodes(sourceDb, targetDb, ignorePropertyIds, ignoreLabelIds, deleteNodesWithLabelsIds, highestIds.first(), config.keepNodeIds);
        copyRelationships(sourceDb, targetDb, ignoreRelTypeIds, ignorePropertyIds, copiedNodeIds, highestIds.other());
        targetDb.shutdown();
        try {
            sourceDb.shutdown();
        } catch (Exception e) {
            logs.append(String.format("Noncritical error closing the source database:%n%s", Exceptions.stringify(e)));
        }
        logs.close();
        copyIndex(source, target);
    }

    enum TokenType {
        Labels, RelTypes, PropertyKeys;

        public int[] resolve(ReadOperations ro, Collection<String> names) {
            if (names == null || names.isEmpty()) return null;
            int[] result = new int[names.size()];
            int i = 0;
            for (String name : names) {
                int id = -1;
                switch (this) {
                    case Labels:
                        id = ro.labelGetForName(name);
                        break;
                    case RelTypes:
                        id = ro.relationshipTypeGetForName(name);
                        break;
                    case PropertyKeys:
                        id = ro.propertyKeyGetForName(name);
                        break;
                }
                result[i++] = id;
            }
            Arrays.sort(result);
            return result;
        }

        public Iterator<Token> readAllFromDB(ReadOperations ro) {
            switch (this) {
                case Labels:
                    return ro.labelsGetAllTokens();
                case RelTypes:
                    return ro.relationshipTypesGetAllTokens();
                default:
                    return Collections.emptyIterator();
            }
        }
    }

    public static <T> T readFromDb(GraphDatabaseAPI api, Function<ReadOperations, T> fun) {
        try (Transaction tx = api.beginTx();
             Statement statement = api.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class).get()) {
            return fun.apply(statement.readOperations());
        }
    }

    private static Pair<Long, Long> getHighestNodeId(GraphDatabaseAPI api) {
        IdGeneratorFactory idGenerators = api.getDependencyResolver().resolveDependency(IdGeneratorFactory.class);
        long highestNodeId = idGenerators.get(IdType.NODE).getHighestPossibleIdInUse();
        long highestRelId = idGenerators.get(IdType.RELATIONSHIP).getHighestPossibleIdInUse();
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

    private static void copyRelationships(GraphDatabaseAPI sourceDb, BatchInserter targetDb, int[] ignoreRelTypes, int[] ignoreProperties, PrimitiveLongLongMap copiedNodeIds, long highestRelId) {
        long time = System.currentTimeMillis();
        long relId = 0;
        long notFound = 0;
        long removed = 0;

        try (Transaction tx = sourceDb.beginTx();
             Statement statement = sourceDb.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class).get()) {
            ReadOperations readOperations = statement.readOperations();
            PropertyMap propertyMap = new PropertyMap(sourceDb);

            while (relId <= highestRelId) {
                RelationshipItem rel = null;
                int type = ReadOperations.ANY_RELATIONSHIP_TYPE;
                try {
                    rel = readOperations.relationshipCursorById(relId++).get();
                    type = rel.type();
                    if (!contains(ignoreRelTypes, type)) {
                        if (!createRelationship(targetDb, readOperations, rel, ignoreProperties, copiedNodeIds, propertyMap)) {
                            removed++;
                        }
                    } else {
                        removed++;
                    }
                } catch (Exception e) {
                    if (e instanceof EntityNotFoundException || (e instanceof org.neo4j.kernel.impl.store.InvalidRecordException && e.getMessage().endsWith("not in use"))) {
                        notFound++;
                    } else {
                        addLog(rel, "copy Relationship: " + (relId - 1) + "-[:" + type + "]" + "->?", e.getMessage());
                    }
                }
                if (relId % 10000 == 0) {
                    System.out.print(".");
                    logs.flush();
                }
                if (relId % 500000 == 0) {
                    System.out.printf(" %d / %d (%d%%) unused %d removed %d%n", relId, highestRelId, percent(relId, highestRelId), notFound, removed);
                }
            }
        }
        time = Math.max(1, (System.currentTimeMillis() - time) / 1000);
        System.out.printf("%n copying of %d relationship records took %d seconds (%d rec/s). Unused Records %d (%d%%) Removed Records %d (%d%%)%n",
                relId, time, relId / time, notFound, percent(notFound, relId), removed, percent(removed, relId));
    }

    private static boolean contains(int[] ids, int id) {
        if (ids == null || ids.length == 0) return false;
        if (id < ids[0] || id > ids[ids.length - 1]) return false;
        for (int i : ids) {
            if (i >= id) return i == id;
        }
        return false;
    }

    private static int percent(Number part, Number total) {
        return (int) (100 * part.floatValue() / total.floatValue());
    }

    private static long firstNode(BatchInserter sourceDb, long highestNodeId) {
        long node = -1;
        while (++node <= highestNodeId) {
            if (sourceDb.nodeExists(node) && !sourceDb.getNodeProperties(node).isEmpty()) return node;
        }
        return -1;
    }

    private static boolean createRelationship(BatchInserter targetDb, ReadOperations readOperations, RelationshipItem rel, int[] ignoreProperties, PrimitiveLongLongMap copiedNodeIds, PropertyMap propertyMap) {
        long startNodeId = copiedNodeIds.get(rel.startNode());
        long endNodeId = copiedNodeIds.get(rel.endNode());
        if (startNodeId == -1L || endNodeId == -1L) return false;
        final int type = rel.type();
        try {
            int[] propertyKeys = relPropertiesArray(readOperations, rel.id(), ignoreProperties);
            Map<String, Object> properties = propertyMap.relMap(propertyKeys, rel.id(), readOperations);
            // todo :(
            RelationshipType relationshipType = RelationshipType.withName(readOperations.relationshipTypeGetName(type));
            targetDb.createRelationship(startNodeId, endNodeId, relationshipType, properties);
            return true;
        } catch (Exception e) {
            addLog(rel, "create Relationship: " + startNodeId + "-[:" + type + "]" + "->" + endNodeId, e.getMessage());
            return false;
        }
    }

    private static PrimitiveLongLongMap copyNodes(GraphDatabaseAPI sourceDb, BatchInserter targetDb, int[] ignoreProperties, int[] ignoreLabels, int[] deleteNodesWithLabels, long highestNodeId, boolean stableNodeIds) {
        long time = System.currentTimeMillis();
        long node = 0;
        long notFound = 0;
        long removed = 0;
        PrimitiveLongLongMap copiedNodes = Primitive.offHeapLongLongMap();

        try (Transaction sourceTx = sourceDb.beginTx();
             Statement statement = sourceDb.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class).get()) {
            ReadOperations readOperations = statement.readOperations();
            Label[] allLabels = asLabels(TokenType.Labels.readAllFromDB(readOperations));
            PropertyMap propertyMap = new PropertyMap(sourceDb);
            while (node <= highestNodeId) {
                try {
                    if (readOperations.nodeExists(node)) {
                        if (intersect(readOperations.nodeGetLabels(node), deleteNodesWithLabels)) {
                            removed++;
                        } else {
                            long newNodeId = node;
                            int[] propertyKeys = nodePropertiesArray(readOperations, node, ignoreProperties);
                            Map<String, Object> properties = propertyMap.nodeMap(propertyKeys, node, readOperations);
                            int[] labels = labelsArray(readOperations, node, ignoreLabels);
                            if (stableNodeIds) {
                                targetDb.createNode(node, properties, asLabels(labels,allLabels));
                            } else {
                                newNodeId = targetDb.createNode(properties, asLabels(labels,allLabels));
                            }
                            copiedNodes.put(node, newNodeId);
                        }
                    } else {
                        notFound++;
                    }
                } catch (Exception e) {
                    if (e instanceof EntityNotFoundException || (e instanceof org.neo4j.kernel.impl.store.InvalidRecordException && e.getMessage().endsWith("not in use"))) {
                        notFound++;
                    } else addLog(node, e.getMessage());
                }
                node++;
                if (node % 10000 == 0) {
                    System.out.print(".");
                }
                if (node % 500000 == 0) {
                    logs.flush();
                    System.out.printf(" %d / %d (%d%%) unused %d removed %d%n", node, highestNodeId, percent(node, highestNodeId), notFound, removed);
                }
            }
        }
        time = Math.max(1, (System.currentTimeMillis() - time) / 1000);
        System.out.printf("%n copying of %d node records took %d seconds (%d rec/s). Unused Records %d (%d%%). Removed Records %d (%d%%).%n",
                node, time, node / time, notFound, percent(notFound, node), removed, percent(removed, node));
        return copiedNodes;
    }

    private static Label[] asLabels(int[] labels, Label[] allLabels) {
        if (labels==null || labels.length == 0) return NO_LABELS;
        Label[] result = new Label[labels.length];
        int i=0;
        for (int label : labels) {
            result[i++]=allLabels[label];
        }
        return result;
    }

    private static Label[] asLabels(Iterator<Token> tokenIterator) {
        List<Token> tokens = Iterators.asList(tokenIterator);
        Label[] labels = new Label[tokens.size()];
        for (Token token : tokens) {
            labels[token.id()]=Label.label(token.name());
        }
        return labels;
    }

    static class PropertyMap extends AbstractMap<String, Object> {
        private int[] ids;
        private String[] names;
        private int[] keys;
        private Object[] values = new Object[10];
        int idx;

        public PropertyMap(GraphDatabaseAPI api) {
            readFromDb(api, (ro) -> {
                int count = (int) Iterators.count(ro.propertyKeyGetAllTokens());
                ids = new int[count];
                names = new String[count];
                Iterator<Token> keys = ro.propertyKeyGetAllTokens();
                int i = 0;
                while (keys.hasNext()) {
                    Token t = keys.next();
                    ids[t.id()] = i;
                    names[i] = t.name();
                    i++;
                }
                return null;
            });
        }

        public Map<String, Object> nodeMap(int[] keys, long id, ReadOperations op) throws EntityNotFoundException {
            if (keys == null || keys.length == 0) return Collections.emptyMap();
            this.keys = keys;
            if (this.values.length < this.keys.length) {
                this.values = new Object[this.keys.length];
            }
            for (int i = 0; i < keys.length; i++) {
                int key = keys[i];
                this.values[i] = op.nodeGetProperty(id, key).asObject();
            }
            return this;
        }

        public Map<String, Object> relMap(int[] keys, long id, ReadOperations op) throws EntityNotFoundException {
            if (keys == null || keys.length == 0) return Collections.emptyMap();
            this.keys = keys;
            if (this.values.length < this.keys.length) {
                this.values = new Object[this.keys.length];
            }
            for (int i = 0; i < keys.length; i++) {
                int key = keys[i];
                this.values[i] = op.relationshipGetProperty(id, key).asObject();
            }
            return this;
        }

        Set<Entry<String, Object>> set = new AbstractSet<Entry<String, Object>>() {
            Iterator<Entry<String, Object>> it = new Iterator<Entry<String, Object>>() {
                Map.Entry<String, Object> entry = new Entry<String, Object>() {
                    @Override
                    public String getKey() {
                        return names[ids[keys[idx]]];
                    }

                    @Override
                    public Object getValue() {
                        return values[idx];
                    }

                    @Override
                    public Object setValue(Object o) {
                        return null;
                    }

                    @Override
                    public boolean equals(Object o) {
                        return o instanceof Map.Entry && ((Map.Entry)o).getKey().equals(getKey()) && ((Map.Entry)o).getValue().equals(getValue());
                    }

                    @Override
                    public int hashCode() {
                        return keys[idx] + values[idx].hashCode();
                    }
                };

                @Override
                public boolean hasNext() {
                    return idx < keys.length;
                }

                @Override
                public Entry<String, Object> next() {
                    idx++;
                    return entry;
                }
            };

            @Override
            public Iterator<Entry<String, Object>> iterator() {
                idx = 0;
                return it;
            }

            @Override
            public int size() {
                return keys.length;
            }
        };

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return set;
        }
    }

    private static int[] labelsArray(ReadOperations db, long node, int[] ignoreLabels) throws EntityNotFoundException {
        PrimitiveIntIterator labels = db.nodeGetLabels(node);
        if (!labels.hasNext()) return NO_IDS;
        return filterIdArray(ignoreLabels, labels, PrimitiveIntCollections.count(db.nodeGetLabels(node)));
    }

    private static int[] nodePropertiesArray(ReadOperations db, long node, int[] ignore) throws EntityNotFoundException {
        PrimitiveIntIterator ids = db.nodeGetPropertyKeys(node);
        if (!ids.hasNext()) return NO_IDS;
        return filterIdArray(ignore, ids, PrimitiveIntCollections.count(db.nodeGetPropertyKeys(node)));
    }
    private static int[] relPropertiesArray(ReadOperations db, long rel, int[] ignore) throws EntityNotFoundException {
        PrimitiveIntIterator ids = db.relationshipGetPropertyKeys(rel);
        if (!ids.hasNext()) return NO_IDS;
        return filterIdArray(ignore, ids, PrimitiveIntCollections.count(db.relationshipGetPropertyKeys(rel)));
    }

    private static int[] filterIdArray(int[] ignore, PrimitiveIntIterator ids, int count) {
        int result[] = new int[count];
        int i=0;
        if (ignore == null) {
            while (ids.hasNext()) result[i++]=ids.next();
            return result;
        } else {
            while (ids.hasNext()) {
                int id = ids.next();
                if (!contains(ignore, id)) result[i++] = ids.next();
            }
            return i == result.length ? result : Arrays.copyOf(result,i);
        }
    }

    private static boolean intersect(PrimitiveIntIterator ids, int[] set) throws EntityNotFoundException {
        if (set == null || !ids.hasNext()) return false;
        while (ids.hasNext()) {
            int id = ids.next();
            if (contains(set,id)) return true;
        }
        return false;
    }

    private static Map<String, Object> getProperties(Map<String, Value> pc, Set<String> ignoreProperties) {
        if (pc.isEmpty()) return Collections.emptyMap();
        Map<String,Object> result = new HashMap<>(pc.size());
        if (!ignoreProperties.isEmpty()) {
            pc.keySet().removeAll(ignoreProperties);
        }
        pc.forEach((k,v) -> result.put(k,v.asObject()));
        return result;
    }

    private static void addLog(RelationshipItem rel, String property, String message) {
        logs.append(String.format("%s.%s %s%n", rel, property, message));
    }

    private static void addLog(long node, String message) {
        logs.append(String.format("Node: %s %s%n", node, message));
    }

    private static void addLog(PropertyContainer pc, String property, String message) {
        logs.append(String.format("%s.%s %s%n", pc, property, message));
    }
}
