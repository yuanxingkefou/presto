package com.mq.presto.operator;

import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;

import java.util.ArrayList;
import java.util.List;

public class PagesBufferOperator
        implements Operator
{
    private OperatorContext operatorContext;

    private final List<Page> pageBuffer;
//    private List<Integer> channels;

    private boolean finished;

    public PagesBufferOperator(OperatorContext operatorContext, List<Page> pageBuffer)
    {
        this.operatorContext = operatorContext;
        this.pageBuffer = pageBuffer;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return !finished;
    }

    @Override
    public void addInput(Page page)
    {
        pageBuffer.add(page);
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void finish()
    {
        showResult();
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    private void showResult()
    {
        System.out.println("match的page有：");
        for (int i = 0; i < pageBuffer.size(); i++) {
            System.out.println("Page" + i);
            showPage(pageBuffer.get(i));
        }
    }

    private void showPage(Page page)
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
