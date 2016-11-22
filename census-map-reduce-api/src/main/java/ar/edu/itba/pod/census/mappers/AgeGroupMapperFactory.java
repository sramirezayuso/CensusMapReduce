package ar.edu.itba.pod.census.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ar.edu.itba.pod.census.CensusData;

public class AgeGroupMapperFactory implements Mapper<Long, CensusData, String, Integer> {

    private static final long serialVersionUID = -3713325164465665033L;
    private static final Logger LOGGER = LogManager.getLogger("AgeGroupMapper");

    @Override
    public void map(Long recordNumber, CensusData censusData, Context<String, Integer> outputContext) {
        LOGGER.debug("Processing input for record: {} with value: {}", recordNumber, censusData);

        String ageGroup;

        if (censusData.getAge() < 15) {
            ageGroup = "0-14";
        } else if (censusData.getAge() < 65) {
            ageGroup = "15-64";
        } else {
            ageGroup = "65-?";
        }

        outputContext.emit(ageGroup, 1);

        LOGGER.debug("Classified into age group {}", ageGroup);
    }
}
