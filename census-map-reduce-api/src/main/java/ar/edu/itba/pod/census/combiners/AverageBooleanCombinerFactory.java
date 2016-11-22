package ar.edu.itba.pod.census.combiners;

import ar.edu.itba.pod.census.DynamicAverage;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

/**
 * Created by santi698 on 22/11/16.
 */
public class AverageBooleanCombinerFactory<T> implements CombinerFactory<T, Boolean, DynamicAverage> {

    @Override
    public Combiner<Boolean, DynamicAverage> newCombiner(final T group) {

        return new Combiner<Boolean, DynamicAverage>() {
            private DynamicAverage average = new DynamicAverage();

            @Override
            public void combine(Boolean value) {
                average = average.addValue(value ? 1 : 0);
            }

            @Override
            public DynamicAverage finalizeChunk() {
                return average;
            }

            @Override
            public void reset() {
                average = new DynamicAverage();
            }
        };
    }
}
