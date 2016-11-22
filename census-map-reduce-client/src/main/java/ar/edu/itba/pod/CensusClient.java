package ar.edu.itba.pod;

import ar.edu.itba.pod.census.CensusData;
import ar.edu.itba.pod.census.ageGroup.AgeGroupQuery;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.PrintWriter;

public class CensusClient {

    private static final String MAP_NAME = "52561-53346";
    private HazelcastInstance hazelcastInstance;
    private final String outPath;
    private final String inPath;
    private final String[] addresses;

    private static final Logger LOGGER = LogManager.getLogger("CensusClient");

    public CensusClient() {
        this.hazelcastInstance = HazelcastClient.newHazelcastClient(getClientConfig());
        this.addresses = System.getProperty("addresses", "127.0.0.1").split("[,;]");
        this.outPath = System.getProperty("outPath", "result.txt");
        this.inPath = System.getProperty("inPath");
    }

    private ClientConfig getClientConfig() {
        ClientConfig ccfg = new ClientConfig();
        if (addresses != null) {
            String[] arrayAddresses = addresses;
            ClientNetworkConfig net = new ClientNetworkConfig();
            net.addAddress(arrayAddresses);
            ccfg.setNetworkConfig(net);
        }
        return ccfg;
    }

    private void executeQuery() throws Exception {
        final String query = System.getProperty("query");
        final long startTime = System.currentTimeMillis();
        switch (query) {
            case "1": executeQuery1(); break;
            case "2":
            case "3":
            case "4":
            case "5":
                LOGGER.warn("Query {} unimplemented.", query);
                break;
            default: LOGGER.warn("Unknown query");
        }
        LOGGER.info("Query took {} ms", (System.currentTimeMillis() - startTime));
    }
    private void executeQuery1() throws Exception {
        Job<Long, CensusData> job = getCensusJob();
        String result = new AgeGroupQuery().submit(job);

        // Tomar resultado e Escribirlo al archivo de salida
        try(PrintWriter out = new PrintWriter(outPath)){
            out.print(result);
        }

    }

    public static void main(String[] args) throws Exception {
        CensusClient client = new CensusClient();

        client.executeQuery();

        System.exit(0);
    }

    private Job<Long, CensusData> getCensusJob() throws Exception {
        // Ahora el JobTracker y los Workers!
        JobTracker tracker = hazelcastInstance.getJobTracker("default");

        LOGGER.info(hazelcastInstance.getCluster());

        IMap<Long, CensusData> myMap = hazelcastInstance.getMap(MAP_NAME);
        CensusDataParser.populateDataMap(myMap, inPath);

        // Ahora el Job desde los pares(key, Value) que precisa MapReduce
        KeyValueSource<Long, CensusData> source = KeyValueSource.fromMap(myMap);
        return tracker.newJob(source);
    }
}
