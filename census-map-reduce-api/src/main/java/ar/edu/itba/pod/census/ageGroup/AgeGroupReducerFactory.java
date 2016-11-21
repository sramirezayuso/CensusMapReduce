package ar.edu.itba.pod.census.ageGroup;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AgeGroupReducerFactory implements ReducerFactory<Integer, Integer, Long> {

    private static final long serialVersionUID = 7760070699178320492L;
    private static final Logger LOGGER = LogManager.getLogger("AgeGroupReducer");


    @Override
    public Reducer<Integer, Long> newReducer(final Integer ageGroup) {
        return new Reducer<Integer, Long>() {
            private Long count;

            @Override
            public void beginReduce() // una sola vez en cada instancia
            {
                count = 0L;
            }

            @Override
            public void reduce(final Integer value) {
                LOGGER.debug("Adding to count for age group: {}", ageGroup);
                count += value;
            }

            @Override
            public Long finalizeReduce() {
                LOGGER.debug("Final count for age group {}: {}", ageGroup, count);
                return count;
            }
        };
    }
}