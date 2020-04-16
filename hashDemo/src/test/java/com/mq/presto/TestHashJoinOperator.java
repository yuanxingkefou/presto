package com.mq.presto;

import com.facebook.presto.Session;
import com.facebook.presto.client.ClientCapabilities;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.LookupSourceFactory;
import com.facebook.presto.operator.LookupSourceProvider;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.SpillContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.GenericPartitioningSpillerFactory;
import com.facebook.presto.spiller.PartitioningSpillerFactory;
import com.facebook.presto.spiller.SingleStreamSpiller;
import com.facebook.presto.spiller.SingleStreamSpillerFactory;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.mq.presto.operator.Driver;
import com.mq.presto.operator.HashBuilderOperator;
import com.mq.presto.operator.JoinBridgeManager;
import com.mq.presto.operator.LookupJoinOperator;
import com.mq.presto.operator.LookupJoinOperatorFactory;
import com.mq.presto.operator.LookupJoinOperators;
import com.mq.presto.operator.Operator;
import com.mq.presto.operator.OperatorFactory;
import com.mq.presto.operator.PagesBufferOperator;
import com.mq.presto.operator.PartitionedLookupSourceFactory;
import com.mq.presto.operator.ValuesOperator;
import com.mq.presto.utils.GeneratePages;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterators.unmodifiableIterator;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestHashJoinOperator
{
    private static int concurrency = 4;
    private static int buckets=2;
    private static boolean isBucketed=true;

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private PartitioningProviderManager partitioningProviderManager;
    private Session TEST_SESSION;
    private JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager;
    private List<List<Page>> partitions;
    private List<List<Page>> probePages;
    private HashBuilderOperator.HashBuilderOperatorFactory hashBuilderOperatorFactory;
    private LookupJoinOperatorFactory lookupJoinOperatorFactory;

    private List<Driver> leftDrivers = new ArrayList<>(concurrency);
    private List<Driver> rightDrivers = new ArrayList<>(concurrency);

    private List<HashBuilderOperator> hashBuilderOperators=new ArrayList<>(concurrency);

    private ValuesOperator leftValuesOperator;
    private ValuesOperator rightValuesOperator;

    private static final SingleStreamSpillerFactory SINGLE_STREAM_SPILLER_FACTORY = new DummySpillerFactory();
    private static final PartitioningSpillerFactory PARTITIONING_SPILLER_FACTORY = new GenericPartitioningSpillerFactory(SINGLE_STREAM_SPILLER_FACTORY);

    private void testInnerJoin(boolean spillEnabled)
    {
        setup();

        TaskContext taskContext = createTaskContext();
        PipelineContext buildPipeline = taskContext.addPipelineContext(1, true, true, false);
        PipelineContext probePipeline = taskContext.addPipelineContext(2, true, true, false);

        OperatorFactory leftOperatorFactory = new OperatorFactory(partitions, probePages);
        OperatorFactory rightOperatorFactory = new OperatorFactory(partitions, probePages);

        createHashBuilderAndLookupJoinOperatorFactory();
        for (int i = 0; i < concurrency; i++) {
            DriverContext leftDriverContext = probePipeline.addDriverContext();
            DriverContext rightDriverContext = buildPipeline.addDriverContext();

            createDriverAndOperators(i, leftDriverContext, rightDriverContext, leftOperatorFactory, rightOperatorFactory);
        }

        executeLeftDriver();

        if(spillEnabled)
        {
            executeRightDriverWithSpill();
        }
        else
        {
            executeRightDriver();
        }

    }

    private void createHashBuilderAndLookupJoinOperatorFactory()
    {
        List<Type> types = ImmutableList.<Type>builder()
                .add(BIGINT)
                .add(BIGINT)
                .build();
        lookupJoinOperatorFactory = new LookupJoinOperators().innerJoin(
                1,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                types,
                Ints.asList(0),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.of(concurrency),
                PARTITIONING_SPILLER_FACTORY);

        hashBuilderOperatorFactory = new HashBuilderOperator.HashBuilderOperatorFactory(
                1,
                concurrency,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                ImmutableList.of(0, 1),
                Ints.asList(0),
                OptionalInt.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                1_500_000,
                new PagesIndex.TestingFactory(false),
                true,
                SINGLE_STREAM_SPILLER_FACTORY);
    }

    private void setup()
    {
        executor = new ThreadPoolExecutor(
                0,
                Integer.MAX_VALUE,
                60L,
                SECONDS,
                new SynchronousQueue<Runnable>(),
                daemonThreadsNamed("test-executor-%s"),
                new ThreadPoolExecutor.DiscardPolicy());
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        partitioningProviderManager = new PartitioningProviderManager();
        TEST_SESSION = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setClientCapabilities(stream(ClientCapabilities.values())
                        .map(ClientCapabilities::toString)
                        .collect(toImmutableSet()))
                .build();

        //how many pages in a partition is depended on the pages
        partitions = GeneratePages.generatePartitions(concurrency,buckets,1,isBucketed);
        probePages = GeneratePages.generateProbePages(concurrency);

        List<Type> sourceTypes = ImmutableList.<Type>builder()
                .add(BIGINT)
                .add(BIGINT)
                .build();

        if(isBucketed) {
            lookupSourceFactoryManager = JoinBridgeManager.lookupAllAtOnce(new PartitionedLookupSourceFactory(
                    sourceTypes,
                    ImmutableList.of(0, 1).stream()
                            .map(sourceTypes::get)
                            .collect(toImmutableList()),
                    Ints.asList(0).stream()
                            .map(sourceTypes::get)
                            .collect(toImmutableList()),
                    concurrency*buckets,
                    requireNonNull(ImmutableMap.of(), "layout is null"),
                    false));
        }
        else{
            lookupSourceFactoryManager = JoinBridgeManager.lookupAllAtOnce(new PartitionedLookupSourceFactory(
                    sourceTypes,
                    ImmutableList.of(0, 1).stream()
                            .map(sourceTypes::get)
                            .collect(toImmutableList()),
                    Ints.asList(0).stream()
                            .map(sourceTypes::get)
                            .collect(toImmutableList()),
                    concurrency,
                    requireNonNull(ImmutableMap.of(), "layout is null"),
                    false));
        }

    }

    private TaskContext createTaskContext()
    {
        return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION);
    }

    public void createDriverAndOperators(int driverId, DriverContext leftDriverContext, DriverContext rightDriverContext, OperatorFactory leftOperatorFactory, OperatorFactory rightOperatorFactory)
    {

        leftValuesOperator = (ValuesOperator) leftOperatorFactory.createOperator(driverId, leftDriverContext, 0, "LeftValuesOperator");
        LookupJoinOperator lookupJoinOperator = lookupJoinOperatorFactory.createOperator(leftDriverContext);
        PagesBufferOperator pagesBufferOperator = (PagesBufferOperator) leftOperatorFactory.createOperator(driverId, leftDriverContext, 2, PagesBufferOperator.class.getSimpleName());

        List<Operator> leftOperators = new ArrayList<>();
        List<Operator> rightOperators = new ArrayList<>();

        leftOperators.add(leftValuesOperator);
        leftOperators.add(lookupJoinOperator);
        leftOperators.add(pagesBufferOperator);
        leftDrivers.add(Driver.createDriver(leftDriverContext, leftOperators));

        rightValuesOperator = (ValuesOperator) rightOperatorFactory.createOperator(driverId, rightDriverContext, 0, "RightValuesOperator");
        HashBuilderOperator hashBuilderOperator = hashBuilderOperatorFactory.createOperator(rightDriverContext);

        rightOperators.add(rightValuesOperator);
        rightOperators.add(hashBuilderOperator);
        rightDrivers.add(Driver.createDriver(rightDriverContext, rightOperators));
        hashBuilderOperators.add(hashBuilderOperator);
    }

    public void executeLeftDriver()
    {
        ExecutorService leftService = Executors.newFixedThreadPool(concurrency);
        for (int i = 0; i < concurrency; i++) {
            final int finalI = i;
            leftService.execute(() -> {
                try {
                    while (!leftDrivers.get(finalI).isFinished()) {
                        leftDrivers.get(finalI).process();
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private void executeRightDriver()
    {
        LookupSourceFactory lookupSourceFactory = lookupSourceFactoryManager.getJoinBridge(Lifespan.taskWide());
        Future<LookupSourceProvider> lookupSourceProvider = lookupSourceFactory.createLookupSourceProvider();

        while (!lookupSourceProvider.isDone()) {
            for (Driver buildDriver : rightDrivers) {
                buildDriver.process();
            }
        }
        getFutureValue(lookupSourceProvider).close();

        for (Driver buildDriver : rightDrivers) {
            runDriverInThread(executor, buildDriver);
        }
    }

    private void executeRightDriverWithSpill()
    {
        LookupSourceFactory lookupSourceFactory = lookupSourceFactoryManager.getJoinBridge(Lifespan.taskWide());
        Future<LookupSourceProvider> lookupSourceProvider = lookupSourceFactory.createLookupSourceProvider();

        int count=0;
        while (!lookupSourceProvider.isDone()) {
            for(int i=0;i<rightDrivers.size();i++)
            {
                rightDrivers.get(i).process();
                HashBuilderOperator hashBuild=hashBuilderOperators.get(i);
                if(count>=concurrency){
                    continue;
                }else {
                    if (hashBuild.getOperatorContext().getReservedRevocableBytes() > 0) {
                        checkState(!lookupSourceProvider.isDone(), "Too late, LookupSource already done");
                        revokeMemory(hashBuild);
                        count++;
                    }
                }
            }
        }

        getFutureValue(lookupSourceProvider).close();

        for (Driver buildDriver : rightDrivers) {
            runDriverInThread(executor, buildDriver);
        }
    }
    private static void runDriverInThread(ExecutorService executor, Driver driver)
    {
        executor.execute(() -> {
            if (!driver.isFinished()) {
                try {
                    driver.process();
                }
                catch (PrestoException e) {
                    driver.getDriverContext().failed(e);
                    throw e;
                }
                runDriverInThread(executor, driver);
            }
        });
    }

    private static void revokeMemory(HashBuilderOperator operator)
    {
        getFutureValue(operator.startMemoryRevoke());
        operator.finishMemoryRevoke();
        checkState(operator.getState() == HashBuilderOperator.State.SPILLING_INPUT || operator.getState() == HashBuilderOperator.State.INPUT_SPILLED);
    }

    private static class DummySpillerFactory
            implements SingleStreamSpillerFactory
    {
        private volatile boolean failSpill;
        private volatile boolean failUnspill;

        void failSpill()
        {
            failSpill = true;
        }

        void failUnspill()
        {
            failUnspill = true;
        }

        @Override
        public SingleStreamSpiller create(List<Type> types, SpillContext spillContext, LocalMemoryContext memoryContext)
        {
            return new SingleStreamSpiller()
            {
                private boolean writing = true;
                private final List<Page> spills = new ArrayList<>();

                @Override
                public ListenableFuture<?> spill(Iterator<Page> pageIterator)
                {
                    checkState(writing, "writing already finished");
                    if (failSpill) {
                        return immediateFailedFuture(new PrestoException(GENERIC_INTERNAL_ERROR, "Spill failed"));
                    }
                    Iterators.addAll(spills, pageIterator);
                    return immediateFuture(null);
                }

                @Override
                public Iterator<Page> getSpilledPages()
                {
                    if (failUnspill) {
                        throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unspill failed");
                    }
                    writing = false;
                    return unmodifiableIterator(spills.iterator());
                }

                @Override
                public long getSpilledPagesInMemorySize()
                {
                    return spills.stream()
                            .mapToLong(Page::getSizeInBytes)
                            .sum();
                }

                @Override
                public ListenableFuture<List<Page>> getAllSpilledPages()
                {
                    if (failUnspill) {
                        return immediateFailedFuture(new PrestoException(GENERIC_INTERNAL_ERROR, "Unspill failed"));
                    }
                    writing = false;
                    return immediateFuture(ImmutableList.copyOf(spills));
                }

                @Override
                public void close()
                {
                    writing = false;
                }
            };
        }
    }

    public static void main(String[] args)
    {
        TestHashJoinOperator test = new TestHashJoinOperator();
        test.testInnerJoin(false);
    }


}
