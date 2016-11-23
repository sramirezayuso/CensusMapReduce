package ar.edu.itba.pod.census.queries;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;

import ar.edu.itba.pod.census.CensusData;
import ar.edu.itba.pod.census.combiners.SumCombinerFactory;
import ar.edu.itba.pod.census.mappers.DepartmentProvinceMapperFactory;
import ar.edu.itba.pod.census.reducers.SumReducerFactory;

public class DepartmentPairQuery implements CensusQuery<String> {

    @Override
    public String submit(Job<String, CensusData> job) throws ExecutionException, InterruptedException {

        StringBuilder sb = new StringBuilder();

        ICompletableFuture<Map<String, Long>> result = job.mapper(new DepartmentProvinceMapperFactory())
                .combiner(new SumCombinerFactory<>())
                .reducer(new SumReducerFactory<>())
                .submit();

        Map<String, Long> resultMap = result.get();

        resultMap.entrySet().stream()
                .collect(Collectors.groupingBy(e -> e.getValue() - (e.getValue() % 100),
                        Collectors.mapping(Map.Entry::getKey, Collectors.toList())))
                .entrySet().stream()
                .filter(e -> e.getValue().size() > 1)
                .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
                .forEach(e -> {
                    sb.append(String.format("%s\n", e.getKey()));
                    for (int i = 0; i < e.getValue().size();  i++) {
                        for (int j = i+1; j < e.getValue().size(); j ++) {
                            sb.append(String.format("%s + %s\n", e.getValue().get(i), e.getValue().get(j)));
                        }
                    }
                });

        return sb.toString();
    }
}
