package ar.edu.itba.pod.census.ageGroup;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ar.edu.itba.pod.census.CensusData;

public class AgeGroupMapperFactory implements Mapper<Long, CensusData, Integer, Integer> {

    private static final long serialVersionUID = -3713325164465665033L;
    private static final Logger LOGGER = LogManager.getLogger("AgeGroupMapper");

    @Override
    public void map(Long inputKey, CensusData inputValue, Context<Integer, Integer> outputContext) {
        LOGGER.debug("Processing input for home: {} with value: {}", inputKey, inputValue);

        int ageGroup;

        if (inputValue.getAge() < 15) {
            ageGroup = 1;
        } else if (inputValue.getAge() < 65) {
            ageGroup = 2;
        } else {
            ageGroup = 3;
        }

        outputContext.emit(ageGroup, 1);

        LOGGER.debug("Classified into age group {}", ageGroup);
    }
}