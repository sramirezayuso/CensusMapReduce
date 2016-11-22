package ar.edu.itba.pod.census.reducers;

import ar.edu.itba.pod.census.DynamicAverage;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LiteracyReducerFactory<T> implements ReducerFactory<T, DynamicAverage, Float> {

    private static final long serialVersionUID = 7760072689158320391L;
    private static final Logger LOGGER = LogManager.getLogger("AverageReducer");

    @Override
    public Reducer<DynamicAverage, Float> newReducer(final T group) {
        return new Reducer<DynamicAverage, Float>() {
            private DynamicAverage partialAverage;

            @Override
            public void beginReduce() {
                partialAverage = new DynamicAverage();
            }

            @Override
            public void reduce(final DynamicAverage partial) {
                LOGGER.debug("Adding to sum for group: {}", group.toString());
                partialAverage = partialAverage.add(partial);
            }

            @Override
            public Float finalizeReduce() {
                return partialAverage.getFloatValue();
            }
        };
    }
}
