package ar.edu.itba.pod.census.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ar.edu.itba.pod.census.CensusData;

public class LiteracyMapperFactory implements Mapper<String, CensusData, String, Boolean> {

    private static final long serialVersionUID = -3713325164465664013L;
    private static final Logger LOGGER = LogManager.getLogger("AgeGroupMapper");

    @Override
    public void map(String id, CensusData censusData, Context<String, Boolean> outputContext) {
        LOGGER.debug("Processing input for record: {} ", censusData);

        outputContext.emit(censusData.getDepartmentName(), censusData.getLiteracy() == 2);

        LOGGER.debug("Is citizen illiterate?", censusData.getLiteracy() == 2);
    }
}
