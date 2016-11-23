package ar.edu.itba.pod.census.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ar.edu.itba.pod.census.CensusData;

public class DepartmentMapperFactory implements Mapper<String, CensusData, String, Integer> {

    private static final long serialVersionUID = -3713325164465665033L;
    private static final Logger LOGGER = LogManager.getLogger("AgeGroupMapper");

    @Override
    public void map(String id, CensusData censusData, Context<String, Integer> outputContext) {
        LOGGER.debug("Processing input for record: {}", censusData);

        outputContext.emit(censusData.getDepartmentName(), 1);

        LOGGER.debug("Classified into department {}", censusData.getDepartmentName());
    }
}
