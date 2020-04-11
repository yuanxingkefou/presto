package com.mq.presto.operator;

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.HashGenerator;
import com.facebook.presto.operator.InterpretedHashGenerator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.SpillContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.GenericPartitioningSpillerFactory;
import com.facebook.presto.spiller.PartitioningSpillerFactory;
import com.facebook.presto.spiller.SingleStreamSpiller;
import com.facebook.presto.spiller.SingleStreamSpillerFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.mq.presto.utils.GeneratePages;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.unmodifiableIterator;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class OperatorFactory
{
    private List<List<Page>> partitions = new ArrayList<>();
    private List<List<Page>> probePages = new ArrayList<>();
        private static final PlanNodeId planNodeId = new PlanNodeId("Test");

    public OperatorFactory(List<List<Page>> partitions, List<List<Page>> probePages)
    {
        this.partitions = partitions;
        this.probePages = probePages;
    }

    public Operator createOperator(int driverId, DriverContext driverContext, int operatorId, String operatorType)
    {
        Operator operator = null;

        switch (operatorType) {
            case "LeftValuesOperator":
                operator = createValuesOperator(driverId, driverContext, operatorId, operatorType);
                break;
            case "RightValuesOperator":
                operator = createValuesOperator(driverId, driverContext, operatorId, operatorType);
                break;
            case "PagesBufferOperator":
                operator = createPagesBufferOperator(operatorId, operatorType, driverContext);
                break;
        }
        return operator;
    }

    private ValuesOperator createValuesOperator(int driverId, DriverContext driverContext, int operatorId, String operatorType)
    {
        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, operatorType);
        if (operatorType.equals("LeftValuesOperator")) {
            return new ValuesOperator(operatorContext, probePages.get(driverId));
        }
        else {
//            return new ValuesOperator(operatorContext,generatePages(9,false));
            return new ValuesOperator(operatorContext, partitions.get(driverId));
        }
    }

    private PagesBufferOperator createPagesBufferOperator(int operatorId, String operatorType, DriverContext driverContext)
    {
        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, operatorType);
        List<Page> pageBuffer = new ArrayList<>();
        PagesBufferOperator pagesBufferOperator = new PagesBufferOperator(operatorContext, pageBuffer);
        return pagesBufferOperator;
    }

    private List<Page> generatePages(int num, boolean left)
    {
        List<Integer> hashChannels = new ArrayList<>();
        hashChannels.add(0);
        List<Page> pages = new ArrayList<>(num);
        int count = 0;
        for (int i = 0; i < num; i++) {
            Page page;
            if (left) {
                page = GeneratePages.generateFixedLeftData();
                System.out.println(page.toString());
            }
            else {
                page = GeneratePages.generateFixedRightData(count);
                count += 20;
            }
//            pages.add(getHashPage(page, hashChannels));
            pages.add(page);
        }
        return pages;
    }

    private Page getHashPage(Page page, List<Integer> hashChannels)
    {
        Block[] hashBlocks = new Block[hashChannels.size()];

        int hashBlockIndex = 0;

        for (int channel : hashChannels) {
            hashBlocks[hashBlockIndex++] = page.getBlock(channel);
        }
        return page.appendColumn(getHashBlock(hashBlocks));
    }

    private Block getHashBlock(Block[] hashBlocks)
    {
        int[] hashChannels = new int[hashBlocks.length];
        for (int i = 0; i < hashBlocks.length; i++) {
            hashChannels[i] = i;
        }

        List<Type> hashTypes = new ArrayList<>();
        hashTypes.add(BIGINT);

        HashGenerator hashGenerator = new InterpretedHashGenerator(ImmutableList.copyOf(hashTypes), hashChannels);
        int positionCount = hashBlocks[0].getPositionCount();
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(positionCount);
        Page page = new Page(hashBlocks);
        for (int i = 0; i < positionCount; i++) {
            builder.writeLong(hashGenerator.hashPosition(i, page));
        }
        return builder.build();
    }
}
