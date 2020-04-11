package com.mq.presto.operator;

import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.List;

public class ValuesOperator
        implements Operator
{
    private OperatorContext operatorContext;
    private final Iterator<Page> pages;

    public ValuesOperator(OperatorContext operatorContext, List<Page> pages)
    {
        this.operatorContext = operatorContext;
        this.pages = ImmutableList.copyOf(pages).iterator();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        if (!pages.hasNext()) {
            return null;
        }
        Page page = pages.next();

        return page;
    }

    @Override
    public void finish()
    {
        Iterators.size(pages);
    }

    @Override
    public boolean isFinished()
    {
        return !pages.hasNext();
    }
}
