package com.mq.presto.utils;

import com.facebook.presto.operator.HashGenerator;
import com.facebook.presto.operator.InterpretedHashGenerator;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.operator.exchange.LocalPartitionGenerator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.type.IntegerType;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static java.util.Optional.of;

public class GeneratePages
{
    private static int count = 0;

    public static Page generateSourceData(int positionCount)
    {
        Random random = new Random();

        boolean[] valueIsNull = new boolean[positionCount];
        Arrays.fill(valueIsNull, false);

        long[] array1 = new long[positionCount];
        long[] array2 = new long[positionCount];

        for (int i = 0; i < positionCount; i++) {
            array1[i] = i;
            array2[i] = i;
        }
        Block block1 = new LongArrayBlock(positionCount, of(valueIsNull), array1);
        Block block2 = new LongArrayBlock(positionCount, of(valueIsNull), array2);

        Page page = new Page(positionCount, block1, block2);

//        System.out.println("预先生成的page"+count++);
//        System.out.println(page.toString());
        return page;
    }

    public static Page generateFixedRightData(int num)
    {
        int positionCount = 10;
        Random random = new Random();

        boolean[] valueIsNull = new boolean[positionCount];
        Arrays.fill(valueIsNull, false);

        long[] array1 = new long[positionCount];
        long[] array2 = new long[positionCount];

        for (int i = 0; i < positionCount; i++) {
            array1[i] = i + num;
            array2[i] = i + num;
        }
        Block block1 = new LongArrayBlock(positionCount, of(valueIsNull), array1);
        Block block2 = new LongArrayBlock(positionCount, of(valueIsNull), array2);

        Page page = new Page(positionCount, block1, block2);

//        System.out.println("预先生成的page"+count++);
//        System.out.println(page.toString());
        return page;
    }

    public static Page generateFixedLeftData()
    {
        int positionCount = 100;
        Random random = new Random();

        boolean[] valueIsNull = new boolean[positionCount];
        Arrays.fill(valueIsNull, false);

        long[] array1 = new long[positionCount];
        long[] array2 = new long[positionCount];

        for (int i = 0; i < positionCount; i++) {
            array1[i] = i;
            array2[i] = random.nextInt(100);
        }
        Block block1 = new LongArrayBlock(positionCount, of(valueIsNull), array1);
        Block block2 = new LongArrayBlock(positionCount, of(valueIsNull), array2);

        Page page = new Page(positionCount, block1, block2);

        System.out.println("预先生成的page"+count++);
        System.out.println(page.toString());
        showPage(page);
        return page;
    }

    public static List<List<Page>> generatePartitions(int concurrency)
    {
        List<List<Page>> partitions = new ArrayList<>(concurrency);
        HashGenerator hashGenerator = new InterpretedHashGenerator(ImmutableList.of(BIGINT), new int[] {0});
        PartitionFunction partitionFunction = new LocalPartitionGenerator(hashGenerator, concurrency);
        IntArrayList[] partitionAssignments = new IntArrayList[concurrency];

        for (int i = 0; i < concurrency; i++) {
            partitionAssignments[i] = new IntArrayList();
            partitions.add(new ArrayList<>());
        }

        for (int num = 0; num < 1; num++) {
            Page page = generateSourceData(100);

            for (int position = 0; position < page.getPositionCount(); position++) {
                int partition = partitionFunction.getPartition(page, position);
                partitionAssignments[partition].add(position);
            }

            Block[] outputBlocks = new Block[page.getChannelCount()];
            for (int partition = 0; partition < concurrency; partition++) {
                IntArrayList positions = partitionAssignments[partition];
                if (!positions.isEmpty()) {
                    for (int i = 0; i < page.getChannelCount(); i++) {
                        outputBlocks[i] = page.getBlock(i).copyPositions(positions.elements(), 0, positions.size());
                    }

                    Page pageSplit = new Page(positions.size(), outputBlocks);
                    System.out.println(partition+":"+pageSplit.toString());
                    showPage(pageSplit);
                    partitions.get(partition).add(pageSplit);
                }
            }
        }
        return partitions;
    }

    public static List<List<Page>> generateProbePages(int concurrency)
    {
        List<List<Page>> partitions = new ArrayList<>(concurrency);

        IntArrayList[] partitionAssignments = new IntArrayList[concurrency];

        for (int i = 0; i < concurrency; i++) {
            partitionAssignments[i] = new IntArrayList();
            partitions.add(new ArrayList<>());
        }

        //每个partition中只有一个page
        for (int num = 0; num < concurrency; num++) {
            Page page = generateFixedRightData(10*num);
            System.out.println(num+":");
            showPage(page);
            partitions.get(num).add(page);
        }
        return partitions;
    }

    private static void showPage(Page page)
    {
        for(int i=0;i<page.getChannelCount();i++)
        {
            System.out.print("Block ");
        }
        System.out.println();

        for (int i=0;i<page.getPositionCount();i++)
        {
            System.out.print("Position"+i+": ");
            for (int j=0;j<page.getChannelCount();j++)
            {
                Block block=page.getBlock(j);
                System.out.print(block.getLong(i)+" ");
            }
            System.out.println();
        }
    }
}
