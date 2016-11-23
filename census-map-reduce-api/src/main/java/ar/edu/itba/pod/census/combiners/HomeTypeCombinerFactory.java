package ar.edu.itba.pod.census.combiners;

import ar.edu.itba.pod.census.DynamicAverage;
import ar.edu.itba.pod.census.HomeTypeData;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

/**
 * Created by santi698 on 22/11/16.
 */
public class HomeTypeCombinerFactory implements CombinerFactory<Integer, Long, HomeTypeData> {

    @Override
    public Combiner<Long, HomeTypeData> newCombiner(final Integer homeType) {

        return new Combiner<Long, HomeTypeData>() {
            private HomeTypeData homeTypeData = new HomeTypeData(homeType);

            @Override
            public void combine(final Long homeId) {
                homeTypeData.addInhabitant(homeId);
            }

            @Override
            public HomeTypeData finalizeChunk() {
                return homeTypeData;
            }

            @Override
            public void reset() {
                homeTypeData = new HomeTypeData(homeType);
            }
        };
    }
}
