package ar.edu.itba.pod.census.reducers;

import ar.edu.itba.pod.census.HomeTypeData;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HomeTypeReducerFactory implements ReducerFactory<Integer, HomeTypeData, Float> {

    private static final long serialVersionUID = 7760072689178320492L;
    private static final Logger LOGGER = LogManager.getLogger("AverageReducer");

    @Override
    public Reducer<HomeTypeData, Float> newReducer(final Integer homeType) {
        return new Reducer<HomeTypeData, Float>() {
            private HomeTypeData homeTypeData;

            @Override
            public void beginReduce() {
                homeTypeData = new HomeTypeData(homeType);
            }

            @Override
            public void reduce(final HomeTypeData homeTypeData) {
                this.homeTypeData.combine(homeTypeData);
            }

            @Override
            public Float finalizeReduce() {
                LOGGER.debug("Final average for group {}: {}", homeType, homeTypeData);
                return homeTypeData.getAverageInhabitants();
            }
        };
    }
}
