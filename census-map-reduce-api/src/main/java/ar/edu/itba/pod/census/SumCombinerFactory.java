package ar.edu.itba.pod.census;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

/**
 * Created by santi698 on 21/11/16.
 */

public class SumCombinerFactory
        implements CombinerFactory<Object, Integer, Integer> {
    public Combiner<Integer, Integer> newCombiner(final Object group) {
        return new Combiner<Integer, Integer>() {
            private int sum;

            public void combine(Integer value) {
                sum += value;
            }
            public Integer finalizeChunk() {
                return sum;
            }
            public void reset() {
                sum = 0;
            }
        };
    }
}