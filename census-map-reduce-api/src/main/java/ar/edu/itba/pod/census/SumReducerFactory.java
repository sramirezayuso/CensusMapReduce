package ar.edu.itba.pod.census;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SumReducerFactory<T> implements ReducerFactory<T, Integer, Long> {

    private static final long serialVersionUID = 7760070699178320492L;
    private static final Logger LOGGER = LogManager.getLogger("AgeGroupReducer");


    @Override
    public Reducer<Integer, Long> newReducer(final T group) {
        return new Reducer<Integer, Long>() {
            private Long sum;

            @Override
            public void beginReduce() // una sola vez en cada instancia
            {
                sum = 0L;
            }

            @Override
            public void reduce(final Integer value) {
                LOGGER.debug("Adding to sum for age group: {}", group.toString());
                sum += value;
            }

            @Override
            public Long finalizeReduce() {
                LOGGER.debug("Final sum for age group {}: {}", group.toString(), sum);
                return sum;
            }
        };
    }
}
