package ar.edu.itba.pod.census.queries;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;

import ar.edu.itba.pod.census.CensusData;
import ar.edu.itba.pod.census.mappers.HomeTypeMapperFactory;
import ar.edu.itba.pod.census.reducers.AverageReducerFactory;

public class HomeTypeQuery implements CensusQuery<String> {

    @Override
    public String submit(Job<String, CensusData> job) throws ExecutionException, InterruptedException {

        StringBuilder sb = new StringBuilder();

        ICompletableFuture<Map<Integer, Float>> result = job.mapper(new HomeTypeMapperFactory())
                .reducer(new AverageReducerFactory<>())
                .submit();

        Map<Integer, Float> resultMap = result.get();

        for (Map.Entry<Integer, Float> e : resultMap.entrySet()) {
            sb.append(String.format("%s = %.2f\n", e.getKey(), e.getValue()));
        }

        return sb.toString();
    }
}
