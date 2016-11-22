package ar.edu.itba.pod.census;

import com.hazelcast.mapreduce.Job;

import java.util.concurrent.ExecutionException;

public interface CensusQuery<T> {
  String submit(Job<T, CensusData> job) throws ExecutionException, InterruptedException;
}
