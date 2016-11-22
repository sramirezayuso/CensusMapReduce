package ar.edu.itba.pod.census.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class SumCombinerFactory<T> implements CombinerFactory<T, Integer, Integer> {

    @Override
    public Combiner<Integer, Integer> newCombiner(final T group) {

        return new Combiner<Integer, Integer>() {
            private int sum;

            @Override
            public void combine(Integer value) {
                sum += value;
            }

            @Override
            public Integer finalizeChunk() {
                return sum;
            }

            @Override
            public void reset() {
                sum = 0;
            }
        };
    }
}