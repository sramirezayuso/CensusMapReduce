package ar.edu.itba.pod.census.queries;

import java.util.concurrent.ExecutionException;

import com.hazelcast.mapreduce.Job;

import ar.edu.itba.pod.census.CensusData;

public interface CensusQuery<T> {

    String submit(Job<T, CensusData> job) throws ExecutionException, InterruptedException;

}