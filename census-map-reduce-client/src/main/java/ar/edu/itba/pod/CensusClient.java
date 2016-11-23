package ar.edu.itba.pod;

import java.io.PrintWriter;
import java.util.Optional;

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

import ar.edu.itba.pod.census.CensusData;
import ar.edu.itba.pod.census.queries.AgeGroupQuery;
import ar.edu.itba.pod.census.queries.CensusQuery;
import ar.edu.itba.pod.census.queries.DepartmentPairQuery;
import ar.edu.itba.pod.census.queries.ProvinceDepartmentQuery;
import ar.edu.itba.pod.census.queries.HomeTypeQuery;
import ar.edu.itba.pod.census.queries.LiteracyQuery;

public class CensusClient {

    private static final String MAP_NAME = "52561-53346";
    private static final Logger LOGGER = LogManager.getLogger("CensusClient");


    private HazelcastInstance hazelcastInstance;
    private final String outPath;
    private final String inPath;
    private final String[] addresses;


    public static void main(String[] args) throws Exception {
        CensusClient client = new CensusClient();

        client.executeQuery();

        System.exit(0);
    }

    public CensusClient() {
        this.addresses = System.getProperty("addresses", "127.0.0.1").split("[,;]");
        this.hazelcastInstance = HazelcastClient.newHazelcastClient(getClientConfig());
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
        final String queryNumber = System.getProperty("query");
        final long startTime = System.currentTimeMillis();

        Optional<CensusQuery> query = Optional.empty();

        switch (queryNumber) {
            case "1": query = Optional.of(new AgeGroupQuery()); break;
            case "2": query = Optional.of(new HomeTypeQuery()); break;
            case "3": query = Optional.of(new LiteracyQuery(Integer.valueOf(System.getProperty("n")))); break;
            case "4": query = Optional.of(new ProvinceDepartmentQuery(System.getProperty("prov"), Integer.valueOf(System.getProperty("tope")))); break;
            case "5": query = Optional.of(new DepartmentPairQuery()); break;
            default: LOGGER.warn("Unknown query");
        }

        if (query.isPresent()) {
            Job<String, CensusData> job = getCensusJob();
            String result = query.get().submit(job);

            try (PrintWriter out = new PrintWriter(outPath)) {
                out.print(result);
            }
        }

        LOGGER.info("Query took {} ms", (System.currentTimeMillis() - startTime));
    }

    private Job<String, CensusData> getCensusJob() throws Exception {
        // Ahora el JobTracker y los Workers!
        JobTracker tracker = hazelcastInstance.getJobTracker("default");

        LOGGER.info(hazelcastInstance.getCluster());

        IMap<String, CensusData> myMap = hazelcastInstance.getMap(MAP_NAME);
        CensusDataParser.populateDataMap(myMap, inPath);

        // Ahora el Job desde los pares(key, Value) que precisa MapReduce
        KeyValueSource<String, CensusData> source = KeyValueSource.fromMap(myMap);
        return tracker.newJob(source);
    }
}
