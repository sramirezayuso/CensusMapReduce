package ar.edu.itba.pod.census.queries;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;

import ar.edu.itba.pod.census.CensusData;
import ar.edu.itba.pod.census.combiners.SumCombinerFactory;
import ar.edu.itba.pod.census.reducers.SumReducerFactory;
import ar.edu.itba.pod.census.mappers.AgeGroupMapperFactory;

public class AgeGroupQuery implements CensusQuery<Long> {

    @Override
    public String submit(Job<Long, CensusData> job) throws ExecutionException, InterruptedException {

        StringBuilder sb = new StringBuilder();

        ICompletableFuture<Map<String, Long>> result = job.mapper(new AgeGroupMapperFactory())
                .combiner(new SumCombinerFactory<>())
                .reducer(new SumReducerFactory<>())
                .submit();

        Map<String, Long> resultMap = result.get();

        sb.append(String.format("0-14 = %s\n", resultMap.get("0-14")));
        sb.append(String.format("15-64 = %s\n", resultMap.get("15-64")));
        sb.append(String.format("65-? = %s\n", resultMap.get("65-?")));

        return sb.toString();
    }
}
