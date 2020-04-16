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

    public static List<List<Page>> generatePartitions(int concurrency,int buckets,int pages,boolean isBucketed)
    {
        if(isBucketed)
        {
            concurrency*=buckets;
        }
        List<List<Page>> partitions = new ArrayList<>(concurrency);
        HashGenerator hashGenerator = new InterpretedHashGenerator(ImmutableList.of(BIGINT), new int[] {0});
        PartitionFunction partitionFunction = new LocalPartitionGenerator(hashGenerator, concurrency);
        IntArrayList[] partitionAssignments = new IntArrayList[concurrency];


        for (int num = 0; num < pages; num++) {
            Page page = generateSourceData(100);

            for (int i = 0; i < concurrency; i++) {
                partitionAssignments[i] = new IntArrayList();
                partitions.add(new ArrayList<>());
            }

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
                    partitions.get(partition).add(pageSplit);
                }
            }
        }
        if(isBucketed)
        {
            int capacity=concurrency/buckets;
            int num=0;
            List<List<Page>> result = new ArrayList<>(capacity);

            for(int i=0;i<capacity;i++)
            {
                result.add(new ArrayList<>());
                for(int j=0;j<buckets;j++)
                {
                    result.get(i).addAll(partitions.get(i*buckets+j));
                }
            }
            showPartitions(result);
            return result;
        }
        else {
            showPartitions(partitions);
            return partitions;
        }
    }

    private static void showPartitions(List<List<Page>> partitions)
    {
        for(int i=0;i<partitions.size();i++)
        {
            System.out.println("Partition:"+i);
            for(int j=0;j<partitions.get(i).size();j++)
            {
                System.out.println("Page"+j+":");
                showPage(partitions.get(i).get(j));
            }
        }
    }
    public static List<List<Page>> generateBuckets(int bucketNum,List<Page> partition)
    {
        List<List<Page>> buckets = new ArrayList<>(bucketNum);
        HashGenerator hashGenerator = new InterpretedHashGenerator(ImmutableList.of(BIGINT), new int[] {0});
        PartitionFunction bucketFunction = new LocalPartitionGenerator(hashGenerator, bucketNum);
        IntArrayList[] bucketAssignments = new IntArrayList[bucketNum];


        for (int num = 0; num < partition.size(); num++) {
            Page page = partition.get(num);

            for (int i = 0; i < bucketNum; i++) {
                bucketAssignments[i] = new IntArrayList();
                buckets.add(new ArrayList<>());
            }

            for (int position = 0; position < page.getPositionCount(); position++) {
                int bucket = bucketFunction.getPartition(page, position);
                bucketAssignments[bucket].add(position);
            }

            Block[] outputBlocks = new Block[page.getChannelCount()];
            for (int bucket = 0; bucket < buckets.size(); bucket++) {
                IntArrayList positions = bucketAssignments[bucket];
                if (!positions.isEmpty()) {
                    for (int i = 0; i < page.getChannelCount(); i++) {
                        outputBlocks[i] = page.getBlock(i).copyPositions(positions.elements(), 0, positions.size());
                    }

                    Page pageSplit = new Page(positions.size(), outputBlocks);
                    System.out.println(bucket+(buckets.size()*num)+":"+pageSplit.toString());
                    showPage(pageSplit);
                    buckets.get(bucket).add(pageSplit);
                }
            }
        }
        return buckets;
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
        System.out.println(page.toString());
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
