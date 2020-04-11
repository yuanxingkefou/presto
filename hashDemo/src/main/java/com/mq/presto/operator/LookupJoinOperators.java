/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mq.presto.operator;

import com.mq.presto.operator.JoinBridgeManager;
import com.facebook.presto.operator.JoinProbe.JoinProbeFactory;
import com.facebook.presto.operator.LookupSourceFactory;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.PartitioningSpillerFactory;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class LookupJoinOperators
{
    public enum JoinType
    {
        INNER,
        PROBE_OUTER, // the Probe is the outer side of the join
        LOOKUP_OUTER, // The LookupSource is the outer side of the join
        FULL_OUTER,
    }

    @Inject
    public LookupJoinOperators()
    {
    }

    public LookupJoinOperatorFactory innerJoin(int operatorId, PlanNodeId planNodeId, JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactory, List<Type> probeTypes, List<Integer> probeJoinChannel, OptionalInt probeHashChannel, Optional<List<Integer>> probeOutputChannels, OptionalInt totalOperatorsCount, PartitioningSpillerFactory partitioningSpillerFactory)
    {
        return createJoinOperatorFactory(operatorId, planNodeId, lookupSourceFactory, probeTypes, probeJoinChannel, probeHashChannel, probeOutputChannels.orElse(rangeList(probeTypes.size())), JoinType.INNER, totalOperatorsCount, partitioningSpillerFactory);
    }

    public LookupJoinOperatorFactory probeOuterJoin(int operatorId, PlanNodeId planNodeId, JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactory, List<Type> probeTypes, List<Integer> probeJoinChannel, OptionalInt probeHashChannel, Optional<List<Integer>> probeOutputChannels, OptionalInt totalOperatorsCount, PartitioningSpillerFactory partitioningSpillerFactory)
    {
        return createJoinOperatorFactory(operatorId, planNodeId, lookupSourceFactory, probeTypes, probeJoinChannel, probeHashChannel, probeOutputChannels.orElse(rangeList(probeTypes.size())), JoinType.PROBE_OUTER, totalOperatorsCount, partitioningSpillerFactory);
    }

    public LookupJoinOperatorFactory lookupOuterJoin(int operatorId, PlanNodeId planNodeId, JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactory, List<Type> probeTypes, List<Integer> probeJoinChannel, OptionalInt probeHashChannel, Optional<List<Integer>> probeOutputChannels, OptionalInt totalOperatorsCount, PartitioningSpillerFactory partitioningSpillerFactory)
    {
        return createJoinOperatorFactory(operatorId, planNodeId, lookupSourceFactory, probeTypes, probeJoinChannel, probeHashChannel, probeOutputChannels.orElse(rangeList(probeTypes.size())), JoinType.LOOKUP_OUTER, totalOperatorsCount, partitioningSpillerFactory);
    }

    public LookupJoinOperatorFactory fullOuterJoin(int operatorId, PlanNodeId planNodeId, JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactory, List<Type> probeTypes, List<Integer> probeJoinChannel, OptionalInt probeHashChannel, Optional<List<Integer>> probeOutputChannels, OptionalInt totalOperatorsCount, PartitioningSpillerFactory partitioningSpillerFactory)
    {
        return createJoinOperatorFactory(operatorId, planNodeId, lookupSourceFactory, probeTypes, probeJoinChannel, probeHashChannel, probeOutputChannels.orElse(rangeList(probeTypes.size())), JoinType.FULL_OUTER, totalOperatorsCount, partitioningSpillerFactory);
    }

    private static List<Integer> rangeList(int endExclusive)
    {
        return IntStream.range(0, endExclusive)
                .boxed()
                .collect(toImmutableList());
    }

    private LookupJoinOperatorFactory createJoinOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactoryManager,
            List<Type> probeTypes,
            List<Integer> probeJoinChannel,
            OptionalInt probeHashChannel,
            List<Integer> probeOutputChannels,
            JoinType joinType,
            OptionalInt totalOperatorsCount,
            PartitioningSpillerFactory partitioningSpillerFactory)
    {
        List<Type> probeOutputChannelTypes = probeOutputChannels.stream()
                .map(probeTypes::get)
                .collect(toImmutableList());

        return new LookupJoinOperatorFactory(
                operatorId,
                planNodeId,
                lookupSourceFactoryManager,
                probeTypes,
                probeOutputChannelTypes,
                lookupSourceFactoryManager.getBuildOutputTypes(),
                joinType,
                new JoinProbeFactory(probeOutputChannels.stream().mapToInt(i -> i).toArray(), probeJoinChannel, probeHashChannel),
                totalOperatorsCount,
                probeJoinChannel,
                probeHashChannel,
                partitioningSpillerFactory);
    }
}
