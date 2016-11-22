package ar.edu.itba.pod.census.ageGroup;

import ar.edu.itba.pod.census.CensusData;
import ar.edu.itba.pod.census.CensusQuery;

import ar.edu.itba.pod.census.SumCombinerFactory;
import ar.edu.itba.pod.census.SumReducerFactory;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.mapreduce.Job;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class AgeGroupQuery implements CensusQuery<Long> {
  @Override
  public String submit(Job<Long, CensusData> job) throws ExecutionException, InterruptedException {
    StringBuilder sb = new StringBuilder();
    ICompletableFuture<Map<String, Long>> result = job.mapper(new AgeGroupMapperFactory())
                                                      .combiner(new SumCombinerFactory())
                                                      .reducer(new SumReducerFactory<>())
                                                      .submit();
    Map<String, Long> resultMap = result.get();

    for (Map.Entry<String, Long> e : resultMap.entrySet()) {
      sb.append(String.format("%s = %s\n", e.getKey(), e.getValue()));
    }
    return sb.toString();
  }
}
