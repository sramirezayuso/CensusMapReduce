package ar.edu.itba.pod.census.queries;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;

import ar.edu.itba.pod.census.CensusData;
import ar.edu.itba.pod.census.combiners.SumCombinerFactory;
import ar.edu.itba.pod.census.keypredicates.ProvinceKeyPredicate;
import ar.edu.itba.pod.census.mappers.DepartmentMapperFactory;
import ar.edu.itba.pod.census.reducers.SumReducerFactory;

public class ProvinceDepartmentQuery implements CensusQuery<String> {

    private final String province;

    private final int cutoff;

    public ProvinceDepartmentQuery(String province, int cutoff) {
        this.province = province;
        this.cutoff = cutoff;
    }

    @Override
    public String submit(Job<String, CensusData> job) throws ExecutionException, InterruptedException {

        StringBuilder sb = new StringBuilder();

        ICompletableFuture<Map<String, Long>> result = job.mapper(new DepartmentMapperFactory())
                .combiner(new SumCombinerFactory<>())
                .reducer(new SumReducerFactory<>())
                .keyPredicate(new ProvinceKeyPredicate(province))
                .submit();

        Map<String, Long> resultMap = result.get();

        resultMap.entrySet().stream()
                .filter(e -> e.getValue() < cutoff)
                .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                .forEach(e -> sb.append(String.format("%s = %s\n", e.getKey(), e.getValue())));

        return sb.toString();
    }
}