package org.neo4j.tool;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author mh
 * @since 07.12.17
 */
public class Pools {
    private final static int DEFAULT_POOL_THREADS = Math.max(Runtime.getRuntime().availableProcessors()/2,1);

    public final static ExecutorService DEFAULT = createDefaultPool(DEFAULT_POOL_THREADS);

    private Pools() {
        throw new UnsupportedOperationException();
    }

    public static ExecutorService createDefaultPool(int threads) {
        int queueSize = threads * 25;
        return new ThreadPoolExecutor(Math.max(1, threads / 2), threads, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(queueSize),
                new CallerBlocksPolicy());
//                new ThreadPoolExecutor.CallerRunsPolicy());
    }
    static class CallerBlocksPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (!executor.isShutdown()) {
                // block caller for 100ns
                LockSupport.parkNanos(10000);
                try {
                    executor.submit(r).get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static <T> Future<T> inTx(GraphDatabaseAPI db, Function<ReadOperations, T> fun) {
        return DEFAULT.submit(() -> {
            try (Transaction tx = db.beginTx();
                 Statement stmt = db.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class).get();
            ) {
                T result = fun.apply(stmt.readOperations());
                tx.success();
                return result;
            }
        });
    }

    public static <T> List<T> waitForFutures(List<Future<T>> futures, boolean onlyDone) {
        List<T> results = new ArrayList<T>(futures.size());
        Iterator<Future<T>> it = futures.iterator();
        while (it.hasNext()) {
            Future<T> future = it.next();
            try {
                if (future != null) {
                    if (!onlyDone || future.isDone()){
                        results.add(future.get());
                        it.remove();
                    }
                } else {
                    it.remove();
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                // ignore
            }
        }
        return results;
    }

    public static <T> Future<Void> processBatch(List<T> batch, GraphDatabaseService db, Consumer<T> action) {
        return DEFAULT.submit((Callable<Void>) () -> {
                    try (Transaction tx = db.beginTx()) {
                        batch.forEach(action);
                        tx.success();
                    }
                    return null;
                }
        );
    }
}
