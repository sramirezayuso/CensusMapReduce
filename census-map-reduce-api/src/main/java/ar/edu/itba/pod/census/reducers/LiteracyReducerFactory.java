package ar.edu.itba.pod.census.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LiteracyReducerFactory<T> implements ReducerFactory<T, Boolean, Float> {

    private static final long serialVersionUID = 7760072689158320391L;
    private static final Logger LOGGER = LogManager.getLogger("AverageReducer");

    @Override
    public Reducer<Boolean, Float> newReducer(final T group) {
        return new Reducer<Boolean, Float>() {
            private int selected;
            private int total;

            @Override
            public void beginReduce() {
                selected = 0;
                total = 0;
            }

            @Override
            public void reduce(final Boolean value) {
                LOGGER.debug("Adding to sum for group: {}", group.toString());
                total++;
                if (value) {
                    selected++;
                }
            }

            @Override
            public Float finalizeReduce() {
                Float average = Float.valueOf(selected) / Float.valueOf(total);
                LOGGER.debug("Final average for group {}: {}", group.toString(), average);
                return average;
            }
        };
    }
}
