package ar.edu.itba.pod.census.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SumReducerFactory<T> implements ReducerFactory<T, Integer, Long> {

    private static final long serialVersionUID = 7760070699178320492L;
    private static final Logger LOGGER = LogManager.getLogger("SumReducer");

    @Override
    public Reducer<Integer, Long> newReducer(final T group) {
        return new Reducer<Integer, Long>() {
            private Long sum;

            @Override
            public void beginReduce() {
                sum = 0L;
            }

            @Override
            public void reduce(final Integer value) {
                LOGGER.debug("Adding to sum for group: {}", group.toString());
                sum += value;
            }

            @Override
            public Long finalizeReduce() {
                LOGGER.debug("Final sum for group {}: {}", group.toString(), sum);
                return sum;
            }
        };
    }
}
