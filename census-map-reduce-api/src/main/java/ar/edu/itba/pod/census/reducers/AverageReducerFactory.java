package ar.edu.itba.pod.census.reducers;

import java.util.HashSet;
import java.util.Set;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AverageReducerFactory<T> implements ReducerFactory<T, Long, Float> {

    private static final long serialVersionUID = 7760072689178320492L;
    private static final Logger LOGGER = LogManager.getLogger("AverageReducer");

    @Override
    public Reducer<Long, Float> newReducer(final T group) {
        return new Reducer<Long, Float>() {
            private Set<Long> uniqueValues;
            private int total;

            @Override
            public void beginReduce() {
                uniqueValues = new HashSet<>();
                total = 0;
            }

            @Override
            public void reduce(final Long value) {
                LOGGER.debug("Adding to average for group: {}", group.toString());
                uniqueValues.add(value);
                total++;
            }

            @Override
            public Float finalizeReduce() {
                Float average = Float.valueOf(total) / Float.valueOf(uniqueValues.size());
                LOGGER.debug("Final average for group {}: {}", group.toString(), average);
                return average;
            }
        };
    }
}
