package ar.edu.itba.pod.census.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ar.edu.itba.pod.census.CensusData;

public class HomeTypeMapperFactory implements Mapper<Long, CensusData, Integer, Long> {

    private static final long serialVersionUID = -3713325164422665033L;
    private static final Logger LOGGER = LogManager.getLogger("AgeGroupMapper");

    @Override
    public void map(Long recordNumber, CensusData censusData, Context<Integer, Long> outputContext) {
        LOGGER.debug("Processing input for record: {} with value: {}", recordNumber, censusData);

        outputContext.emit(censusData.getHomeType(), censusData.getHomeId());

        LOGGER.debug("Classified into home type {}", censusData.getHomeType());
    }
}
