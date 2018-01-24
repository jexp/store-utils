package org.neo4j.tool;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.Token;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerators;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntity.NO_PROPERTIES;
import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors.defaultVisible;

/*
 unwind range(1,1000) as id create (n:Node {id:id, name:"Name "+id})
 with collect(n) as nodes unwind nodes as n
 with n, nodes[toInt(rand()*1000)] as m create (n)-[:REL]->(m);
 */
public class StoreCopyNewParallel {

    private static final int[] NO_LABELS = new int[0];
    private static final int OUTPUT_BATCH = 10000;
    static final long BATCH_SIZE = 500_000;

    private static PrintWriter logs;

    static volatile boolean running = true;

    public static void main(String[] args) throws Exception {
        StoreCopyConfig config = new StoreCopyConfig(args);
        if (!config.isValid()) return;

        copyStore(config);
    }

    private static void copyStore(StoreCopyConfig config) throws Exception {
        try {
            final File target = new File(config.targetDir);
            final File source = new File(config.sourceDir);
            if (target.exists()) {
                FileUtils.deleteRecursively(target);
                // throw new IllegalArgumentException("Target Directory already exists "+target);
            }
            if (!source.exists()) throw new IllegalArgumentException("Source Database does not exist " + source);

            String pageCacheSize = System.getProperty("dbms.pagecache.memory", "2G");
            Map<String, String> targetConfig = MapUtil.stringMap("dbms.pagecache.memory", pageCacheSize);
            Map<String, String> sourceConfig = MapUtil.stringMap("dbms.pagecache.memory", System.getProperty("dbms.pagecache.memory.source", pageCacheSize));

            GraphDatabaseAPI sourceDb = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(source).setConfig(sourceConfig).newGraphDatabase();
            Pair<Long, Long> highestIds = getHighestNodeId(sourceDb);
            Tokens tokens = new Tokens(sourceDb, config);
            tokens.createTargetTokens(target);

            boolean skipBadEntriesLogging = false;
            BufferedOutputStream badEntriesLog = new BufferedOutputStream(new FileOutputStream(new File(config.badEntriesLogDir, "bad-entries.log")));

            FormattedLogProvider logProvider = FormattedLogProvider.toOutputStream(System.out); // TODO other log output?

            SimpleLogService logService = new SimpleLogService(logProvider, logProvider);

            ParallelBatchImporter targetDb = new ParallelBatchImporter(target, new DefaultFileSystemAbstraction(), DEFAULT, logService, defaultVisible(), Config.defaults(targetConfig));

            logs = new PrintWriter(new FileWriter(new File(config.storeCopyLogDir, "store-copy.log")));

            ArrayBlockingQueue<InputNode> nodeQueue = new ArrayBlockingQueue<>(10_000_000);
            new Thread(() -> readNodes(sourceDb, nodeQueue, highestIds.first(), config, tokens)).start();
            ArrayBlockingQueue<InputRelationship> relQueue = new ArrayBlockingQueue<>(10_000_000);
            new Thread(() -> readRelationships(sourceDb, relQueue, highestIds.other(), config, tokens)).start();

            targetDb.doImport(new StoreCopyInput(nodeQueue, relQueue, badEntriesLog, skipBadEntriesLogging, config));

            try {
                sourceDb.shutdown();
            } catch (Exception e) {
                logs.append(String.format("Noncritical error closing the source database:%n%s", Exceptions.stringify(e)));
            }
            logs.close();
            copyIndex(source, target);
        } finally {
            Pools.DEFAULT.shutdown();
            Pools.DEFAULT.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    enum TokenTypes {
        Labels,RelTypes,PropKeys
    }

    private static <T> T  take(BlockingQueue<T> queue, T tombstone) {
        try {
            return queue.poll(10,TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
            logs.append(String.format("Interrupt during take %s%n", e.getMessage()));
            return tombstone;
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

    private static void readRelationships(GraphDatabaseAPI sourceDb, BlockingQueue<InputRelationship> relQueue, long highestRelId, StoreCopyConfig config, Tokens tokens) {
        Stats totalStats = new Stats();

        File storeDir = sourceDb.getStoreDir();
        String path = storeDir.getPath();
        List<Future<Stats>> futures = new ArrayList<>(100);

        for (long start = 0; start <= highestRelId; start += BATCH_SIZE) {
            long startRelId = start;
            long endRelId = Math.min(startRelId + BATCH_SIZE, highestRelId + 1);
            futures.add(Pools.inTx(sourceDb, (readOps) -> {
                Stats stats = new Stats();

                int[] ignoreRelTypesArray = tokens.ignoreRelTypesArray;
                int[] ignorePropertiesArray = tokens.ignorePropertiesArray;
                int[] propertyTargetTokens = tokens.targetTokens(TokenTypes.PropKeys);
                int[] relTypeTargetTokens = tokens.targetTokens(TokenTypes.RelTypes);

                int type = ReadOperations.ANY_RELATIONSHIP_TYPE;
                long relId = startRelId;
                long count = 0;
                while (running && relId < endRelId) {
                    try {
                        try (Cursor<RelationshipItem> relItemCursor = readOps.relationshipCursorById(relId)) {
                            RelationshipItem relItem = relItemCursor.get();
                            if (inArray(ignoreRelTypesArray, relItem.type())) {
                                stats.notFound++;
                            } else {
                                type = relTypeTargetTokens[relItem.type()];
                                Object[] properties = getProperties(readOps.relationshipGetProperties(relItem), ignorePropertiesArray, propertyTargetTokens);
                                InputRelationship inputRel = new InputRelationship(path, relId, 0, properties, null, relItem.startNode(), relItem.endNode(), null, type);
                                relQueue.offer(inputRel, 10, TimeUnit.SECONDS); // TODO ?
                            }
                        }
                    } catch (Exception e) {
                        if (e instanceof InvalidRecordException && e.getMessage().endsWith("not in use") || e instanceof EntityNotFoundException) {
                            stats.notFound++;
                        } else {
                            addRelLog("copy Relationship: " + (relId - 1) + "-[:" + type + "]" + "->?", e.getMessage());
                        }
                    }
                    relId++;
                    count++;
                    if (relId % OUTPUT_BATCH == 0) {
                        System.out.print("R");
                        logs.flush();
                    }
                }
                if (relId - 1 == highestRelId) {
                    relQueue.offer(TOMBSTONE_REL);
                }

                stats.count = count;
                stats.done(running);
                if (running) {
                    stats.printSummary("Relationships Batch " + startRelId / BATCH_SIZE);
                }
                return stats;
            }));
            if (futures.size() > 25) {
                totalStats.accumlulate(Pools.waitForFutures(futures, true));
            }
        }
        totalStats.accumlulate(Pools.waitForFutures(futures,false));
        totalStats.done(running);
        totalStats.printSummary("Relationships Total");
    }

    private static int percent(Number part, Number total) {
        if (total.longValue() == 0L) return 0;
        return (int) (100 * part.floatValue() / total.floatValue());
    }

    private static long firstNode(BatchInserter sourceDb, long highestNodeId) {
        long node = -1;
        while (++node <= highestNodeId) {
            if (sourceDb.nodeExists(node) && !sourceDb.getNodeProperties(node).isEmpty()) return node;
        }
        return -1;
    }

    private static InputNode TOMBSTONE = new InputNode(null,-1,-1,-1,null,null,null,null);
    private static InputRelationship TOMBSTONE_REL = new InputRelationship(null,-1,-1,null,null,-1,-1,null,null);

    static class Tokens {
        private final GraphDatabaseAPI sourceDb;
        private final StoreCopyConfig config;
        private final List<Token> labelTokens, relTypeTokens, propertyKeyTokens;
        int[] ignoreLabelsArray, deleteLabelsArray, ignorePropertiesArray, ignoreRelTypesArray;
        String[] allLabels, allProperties;
        int[][] targetTypeTokens;

        public Tokens(GraphDatabaseAPI sourceDb, StoreCopyConfig config) {
            this.sourceDb = sourceDb;
            this.config = config;
            try (Transaction sourceTx = sourceDb.beginTx();
                 Statement stmt = ((GraphDatabaseAPI) sourceDb).getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class).get();
            ) { // todo parallelize
                ReadOperations readOps = stmt.readOperations();

                this.labelTokens = Iterators.asList(Iterators.filter((t) -> !config.ignoreLabels.contains(t.name()) && !config.deleteNodesWithLabels.contains(t.name()),readOps.labelsGetAllTokens()));
                this.relTypeTokens = Iterators.asList(Iterators.filter((t) -> !config.ignoreRelTypes.contains(t.name()),readOps.relationshipTypesGetAllTokens()));
                this.propertyKeyTokens = Iterators.asList(Iterators.filter((t) -> !config.ignoreProperties.contains(t.name()),readOps.propertyKeyGetAllTokens()));

                ignoreLabelsArray = toTokenIdArray(config.ignoreLabels, readOps::labelGetForName);
                deleteLabelsArray = toTokenIdArray(config.deleteNodesWithLabels, readOps::labelGetForName);
                ignoreRelTypesArray = toTokenIdArray(config.ignoreRelTypes, readOps::relationshipTypeGetForName);
                ignorePropertiesArray = toTokenIdArray(config.ignoreProperties, readOps::propertyKeyGetForName);
                allLabels = tokenArray(readOps.labelsGetAllTokens());
                allProperties = tokenArray(readOps.propertyKeyGetAllTokens());
            }
        }


        private void createTargetTokens(File target) throws IllegalTokenNameException, TooManyLabelsException {
            targetTypeTokens = new int[3][];
            GraphDatabaseAPI targetDb = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase(target.getAbsoluteFile());
            try (Transaction writeTx = targetDb.beginTx();
                 Statement writeStmt = targetDb.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class).get()) {
                TokenWriteOperations ops = writeStmt.tokenWriteOperations();

                targetTypeTokens[TokenTypes.Labels.ordinal()] = new int[labelTokens.stream().mapToInt(Token::id).max().getAsInt() + 1];
                for (Token t : labelTokens)
                    targetTypeTokens[TokenTypes.Labels.ordinal()][t.id()] = ops.labelGetOrCreateForName(t.name());

                targetTypeTokens[TokenTypes.RelTypes.ordinal()] = new int[relTypeTokens.stream().mapToInt(Token::id).max().getAsInt() + 1];
                for (Token t : relTypeTokens)
                    targetTypeTokens[TokenTypes.RelTypes.ordinal()][t.id()] = ops.relationshipTypeGetOrCreateForName(t.name());

                targetTypeTokens[TokenTypes.PropKeys.ordinal()] = new int[propertyKeyTokens.stream().mapToInt(Token::id).max().getAsInt() + 1];
                for (Token t : propertyKeyTokens)
                    targetTypeTokens[TokenTypes.PropKeys.ordinal()][t.id()] = ops.propertyKeyGetOrCreateForName(t.name());
                writeTx.success();
            }
            targetDb.shutdown();
        }

        public int[] targetTokens(TokenTypes type) {
            return targetTypeTokens[type.ordinal()];
        }

        private static int[] toTokenIdArray(Set<String> names, Function<String, Integer> resolver) {
            if (names == null || names.isEmpty()) return null;

            int[] result = new int[names.size()];
            int idx = 0;
            for (String name : names) {
                result[idx++] = resolver.apply(name);
            }
            Arrays.sort(result);
            return result;
        }

        private static String[] tokenArray(Iterator<Token> tokenIt) {
            List<Token> tokens = Iterators.asList(tokenIt);
            Token highId = Collections.max(tokens, Comparator.comparingInt(Token::id));
            String[] names = new String[highId.id()+1];
            for (Token token : tokens) {
                names[token.id()]=token.name();
            }
            return names;
        }
    }
    static class Stats {
        long start = System.currentTimeMillis();
        long count, notFound, removed;
        private long end;
        private long time;
        private boolean success;

        void done(boolean success) {
            this.end = System.currentTimeMillis();
            this.time = TimeUnit.MILLISECONDS.toSeconds(end - start);
            this.success = success;
        }

        public void accumlulate(List<Stats> allStats) {
            for (Stats stats : allStats) {
                this.count += stats.count;
                this.removed += stats.removed;
                this.notFound += stats.notFound;
                this.time += stats.time;
            }
        }

        private void printSummary(String msg) {
            long divTime = Math.max(time,1);
            System.out.printf("%n%s: %s copying of %d records took %d seconds (%d rec/s). Unused Records %d (%d%%) Removed Records %d (%d%%)%n",
                    msg, success ? "OK":"FAIL", count, time, count / divTime, notFound, percent(notFound, count), removed, percent(removed, count));
        }
    }

    private static void readNodes(GraphDatabaseAPI sourceDb, BlockingQueue<InputNode> nodeQueue, long highestNodeId, StoreCopyConfig config, Tokens tokens) {
        Stats totalStats = new Stats();

        File storeDir = sourceDb.getStoreDir();
        String path = storeDir.getPath();
        List<Future<Stats>> futures = new ArrayList<>(100);

        for (long start=0;start<=highestNodeId;start+=BATCH_SIZE) {
            long startNodeId = start;
            long endNodeId = Math.min(startNodeId + BATCH_SIZE, highestNodeId + 1);
            futures.add(Pools.inTx(sourceDb, (readOps) -> {
                Stats stats = new Stats();
                int[] ignoreLabelsArray = tokens.ignoreLabelsArray;
                int[] deleteLabelsArray = tokens.deleteLabelsArray;
                int[] ignorePropertiesArray = tokens.ignorePropertiesArray;
                int[] propertyTargetTokens = tokens.targetTokens(TokenTypes.PropKeys);
                String[] allLabels = tokens.allLabels; // todo
                long node = startNodeId;
                long count = 0;
                while (running && node < endNodeId) {
                    try {
                        if (readOps.nodeExists(node)) {
                            try (Cursor<NodeItem> nodeItemCursor = readOps.nodeCursorById(node)) {
                                NodeItem nodeItem = nodeItemCursor.get();
                                if (labelMatches(nodeItem.labels(),deleteLabelsArray)) {
                                    stats.removed ++;
                                } else {
                                    String[] labels = labelStrings(ignoreLabelsArray, allLabels, nodeItem);
                                    Object[] properties = getProperties(readOps.nodeGetProperties(nodeItem), ignorePropertiesArray, propertyTargetTokens);
                                    InputNode inputNode = new InputNode(path, node, 0, node, properties, null, labels, null);
                                    nodeQueue.offer(inputNode, 10, TimeUnit.SECONDS); // TODO ?
                                }
                            }
                        } else {
                            stats.notFound++;
                        }
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                        if (e instanceof EntityNotFoundException) {
                            stats.notFound++;
                        } else if (e instanceof InvalidRecordException && e.getMessage().endsWith("not in use")) {
                            stats.notFound++;
                        } else addNodeLog(node, e.getMessage());
                    }

                    node++;
                    count++;
                    if (count % OUTPUT_BATCH == 0) {
                        System.out.print("N");
                    }
                }
                if (node - 1 == highestNodeId) {
                    nodeQueue.offer(TOMBSTONE);
                }
                logs.flush();
                stats.count = count;
                stats.done(running);
                if (running) {
                    stats.printSummary("Nodes Batch "+startNodeId/BATCH_SIZE);
                }
                return stats;
            }));
            if (futures.size() > 25) {
                totalStats.accumlulate(Pools.waitForFutures(futures,true));
            }
        }
        totalStats.accumlulate(Pools.waitForFutures(futures,false));
        totalStats.done(running);
        totalStats.printSummary("Nodes Total");
    }

    private static boolean labelMatches(PrimitiveIntSet labels, int[] labelsArray) {
        if (labelsArray==null || labelsArray.length == 0 || labels.isEmpty()) return false;
        for (int label : labelsArray) {
            if (labels.contains(label)) return true;
        }
        return false;
    }

    private static String[] labelStrings(int[] ignoreLabels, String[] allLabels, NodeItem nodeItem) throws EntityNotFoundException, LabelNotFoundKernelException {
        PrimitiveIntSet labelSet = nodeItem.labels();
        if (labelSet.isEmpty()) return InputEntity.NO_LABELS;

        if (ignoreLabels != null && ignoreLabels.length > 0) {
            for (int ignoreLabel : ignoreLabels) {
                labelSet.remove(ignoreLabel);
            }
        }
        String[] labels = new String[labelSet.size()];
        PrimitiveIntIterator it = labelSet.iterator();
        int idx=0;
        while (it.hasNext()) labels[idx++]=allLabels[it.next()];
        return idx < allLabels.length ? Arrays.copyOf(labels,idx) : labels;
    }

    private static boolean inArray(int[] ids, int id) {
        if (ids==null || ids.length == 0) return false;
        for (int element : ids) {
            if (element == id) return true;
            if (id > element) return false;
        }
        return false;
    }

    private static Object[] getProperties(Cursor<PropertyItem> pc, int[] ignoreProperties, int[] targetTokens) {
        if (!pc.next()) return NO_PROPERTIES;

        Object[] result = new Object[100*2]; // todo reuse?
        int idx=0;
        PropertyItem propertyItem = pc.get();
        do {
            if (!inArray(ignoreProperties,propertyItem.propertyKeyId())) {
                if (idx >= result.length) result = Arrays.copyOf(result,result.length+50);
                result[idx++] = targetTokens[propertyItem.propertyKeyId()];
                result[idx++] = propertyItem.value().asObject();
            }
            propertyItem = pc.get();
        } while (pc.next());
        return Arrays.copyOf(result,idx);
    }

    private static void addRelLog(String message, String detail) {
        logs.append(String.format("Rel: %s %s%n", message, detail));
    }

    private static void addNodeLog(long node, String message) {
        logs.append(String.format("Node: %s %s%n", node, message));
    }

    private static class StoreCopyInput implements Input {

        private final ArrayBlockingQueue<InputNode> nodeQueue;
        private final ArrayBlockingQueue<InputRelationship> relQueue;
        private final StoreCopyConfig config;
        private BadCollector badCollector;

        public StoreCopyInput(ArrayBlockingQueue<InputNode> nodeQueue, ArrayBlockingQueue<InputRelationship> relQueue, BufferedOutputStream badEntriesLog, boolean skipBadEntriesLogging, StoreCopyConfig config) {
            this.nodeQueue = nodeQueue;
            this.relQueue = relQueue;
            this.config = config;


            badCollector = new BadCollector(badEntriesLog, BadCollector.UNLIMITED_TOLERANCE, BadCollector.COLLECT_ALL, skipBadEntriesLogging);
        }

        @Override
        public InputIterable<InputNode> nodes() {
            return new InputIterable<InputNode>() {
                @Override
                public InputIterator<InputNode> iterator() {
                    return new InputIterator<InputNode>() {
                        InputNode item = take(nodeQueue,TOMBSTONE);
                        @Override
                        public void close() {
                            System.err.println("Close called");
                        }

                        @Override
                        public boolean hasNext() {
                            return running && item != TOMBSTONE;
                        }

                        @Override
                        public InputNode next() {
                            InputNode tmp = item;
                            if (item != TOMBSTONE) {
                                item = take(nodeQueue, TOMBSTONE);
                            }
                            return tmp;
                        }

                        @Override
                        public String sourceDescription() {
                            return item.sourceDescription();
                        }

                        @Override
                        public long lineNumber() {
                            return item.lineNumber();
                        }

                        @Override
                        public long position() {
                            return item.position();
                        }

                        @Override
                        public void receivePanic(Throwable cause) {
                            running = false;
                            item = TOMBSTONE;
                            System.err.println("Received Panic ");
                            cause.printStackTrace(System.err);
                            throw new RuntimeException(cause);
                        }
                    };
                }

                @Override
                public boolean supportsMultiplePasses() {
                    return false;
                }
            };
        }

        @Override
        public InputIterable<InputRelationship> relationships() {
            return new InputIterable<InputRelationship>() {
                @Override
                public boolean supportsMultiplePasses() {
                    return false;
                }

                @Override
                public InputIterator<InputRelationship> iterator() {
                    return new InputIterator<InputRelationship>() {
                        InputRelationship item = take(relQueue,TOMBSTONE_REL);
                        @Override
                        public void close() {
                            System.err.println("Close called");
                        }

                        @Override
                        public boolean hasNext() {
                            return running && item != TOMBSTONE_REL;
                        }

                        @Override
                        public InputRelationship next() {
                            InputRelationship tmp = item;
                            if (item != TOMBSTONE_REL) {
                                item = take(relQueue, TOMBSTONE_REL);
                            }
                            return tmp;
                        }

                        @Override
                        public String sourceDescription() {
                            return item.sourceDescription();
                        }

                        @Override
                        public long lineNumber() {
                            return item.lineNumber();
                        }

                        @Override
                        public long position() {
                            return item.position();
                        }

                        @Override
                        public void receivePanic(Throwable cause) {
                            item = TOMBSTONE_REL;
                            running = false;
                            System.err.println("Received Panic ");
                            cause.printStackTrace(System.err);
                            throw new RuntimeException(cause);
                        }
                    };
                }
            };
        }

        @Override
        public IdMapper idMapper(NumberArrayFactory numberArrayFactory) {
            return config.keepNodeIds ? IdMappers.actual() : IdMappers.longs(numberArrayFactory);
        }

        @Override
        public IdGenerator idGenerator() {
            return config.keepNodeIds ? IdGenerators.fromInput() : IdGenerators.startingFromTheBeginning();
        }

        @Override
        public Collector badCollector() {
            return badCollector;
        }
    }
}
