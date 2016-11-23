package ar.edu.itba.pod;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import com.hazelcast.core.IMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.ICsvBeanReader;
import org.supercsv.prefs.CsvPreference;

import ar.edu.itba.pod.census.CensusData;

public class CensusDataParser {

    private static final Logger LOGGER = LogManager.getLogger("CensusDataParser");

    private static CellProcessor[] getProcessors() {
        return new CellProcessor[] {
                new ParseInt(new NotNull()),    // homeType
                new ParseInt(new NotNull()),    // serviceQuality
                new ParseInt(new NotNull()),    // gender
                new ParseInt(new NotNull()),    // age
                new ParseInt(new NotNull()),    // literacy
                new ParseInt(new NotNull()),    // activity
                new NotNull(),                  // departmentName
                new NotNull(),                  // provinceName
                new ParseLong(new NotNull())    // homeId
        };
    }

    public static void populateDataMap(IMap<String, CensusData> dataMap, String fileName) throws Exception {
        InputStream is = new FileInputStream(new File(fileName));
        Reader reader = new InputStreamReader(is);
        try (ICsvBeanReader beanReader = new CsvBeanReader(reader, CsvPreference.STANDARD_PREFERENCE)) {
            beanReader.getHeader(true);
            final String[] header = {"homeType", "serviceQuality", "gender", "age", "literacy", "activity", "departmentName", "provinceName", "homeId"};
            final CellProcessor[] processors = getProcessors();

            CensusData censusData;
            while ((censusData = beanReader.read(CensusData.class, header, processors)) != null) {
                LOGGER.debug("lineNumber={}, rowNumber={}, data={}", beanReader.getLineNumber(), beanReader.getRowNumber(), censusData);
                dataMap.set(beanReader.getRowNumber() + " - "+ censusData.toString(), censusData);
            }
        }
    }
}
