package ar.edu.itba.pod.census.queries;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import ar.edu.itba.pod.census.combiners.AverageBooleanCombinerFactory;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;

import ar.edu.itba.pod.census.CensusData;
import ar.edu.itba.pod.census.mappers.LiteracyMapperFactory;
import ar.edu.itba.pod.census.reducers.LiteracyReducerFactory;

public class LiteracyQuery implements CensusQuery<String> {

    private final int maxDepartments;

    public LiteracyQuery(int maxDepartments) {
        this.maxDepartments = maxDepartments;
    }

    @Override
    public String submit(Job<String, CensusData> job) throws ExecutionException, InterruptedException {

        StringBuilder sb = new StringBuilder();

        ICompletableFuture<Map<String, Float>> result = job.mapper(new LiteracyMapperFactory())
                .combiner(new AverageBooleanCombinerFactory<>())
                .reducer(new LiteracyReducerFactory<>())
                .submit();

        Map<String, Float> resultMap = result.get();

        resultMap.entrySet().stream()
                .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                .limit(maxDepartments)
                .forEach(e -> sb.append(String.format("%s = %.2f\n", e.getKey(), e.getValue())));

        return sb.toString();
    }
}

