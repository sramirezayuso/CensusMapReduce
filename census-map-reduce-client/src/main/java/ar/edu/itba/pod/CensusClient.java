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
    private String outPath;
    private final String inPath;
    private final String[] addresses;
    private final KeyValueSource<String, CensusData> source;
    private final JobTracker tracker;

    public static void main(String[] args) throws Exception {
        CensusClient client = new CensusClient();
        if (System.getProperty("benchmark", "false").equals("true")) {
            benchmarkAllQueries(client);
        } else {
            final String queryNumber = System.getProperty("query");
            client.executeQuery(getQuery(Integer.valueOf(queryNumber)));
        }
        System.exit(0);
    }

    private static void benchmarkAllQueries(CensusClient client) throws Exception {
        for (int i = 1; i <= 5; i++) {
            client.setOutPath("query" + i + "output.txt");
            LOGGER.info("-------- Query {} --------", i);
            client.executeQuery(getQuery(i));
        }
    }

    public CensusClient() {
        this.addresses = System.getProperty("addresses", "127.0.0.1").split("[,;]");
        this.hazelcastInstance = HazelcastClient.newHazelcastClient(getClientConfig());
        this.inPath = System.getProperty("inPath");
        this.outPath = System.getProperty("outPath", String.format("%s.txt", inPath));
        this.tracker = hazelcastInstance.getJobTracker("default");
        this.source = initializeDataSource();
    }

    private ClientConfig getClientConfig() {
        ClientConfig ccfg = new ClientConfig();
        if (addresses != null) {
            String[] arrayAddresses = addresses;
            ClientNetworkConfig net = new ClientNetworkConfig();
            net.addAddress(arrayAddresses);
            ccfg.setNetworkConfig(net);
            ccfg.getGroupConfig().setName(MAP_NAME);
            ccfg.getGroupConfig().setPassword("dev-pass");
        }
        return ccfg;
    }

    private void executeQuery(Optional<CensusQuery> query) throws Exception {
        LOGGER.info("Started Map/Reduce");
        final long startTime = System.currentTimeMillis();

        if (query.isPresent()) {
            Job<String, CensusData> job = getCensusJob();
            String result = query.get().submit(job);
            LOGGER.info("Query took {} ms", (System.currentTimeMillis() - startTime));

            try (PrintWriter out = new PrintWriter(outPath)) {
                out.print(result);
                LOGGER.info("Query finished. Result written to {}.", outPath);
            }
        }

        LOGGER.info("Finished Map/Reduce");
    }

    private static Optional<CensusQuery> getQuery(int queryNumber) {
        Optional<CensusQuery> query = Optional.empty();

        switch (queryNumber) {
            case 1: query = Optional.of(new AgeGroupQuery()); break;
            case 2: query = Optional.of(new HomeTypeQuery()); break;
            case 3: query = Optional.of(new LiteracyQuery(Integer.valueOf(System.getProperty("n", "5")))); break;
            case 4: query = Optional.of(new ProvinceDepartmentQuery(System.getProperty("prov", "Buenos Aires"), Integer.valueOf(System.getProperty("tope", "1000")))); break;
            case 5: query = Optional.of(new DepartmentPairQuery()); break;
            default: LOGGER.error("Unknown query");
        }
        return query;
    }

    private KeyValueSource<String, CensusData> initializeDataSource() {
        // Ahora el JobTracker y los Workers!
        LOGGER.info(hazelcastInstance.getCluster());

        IMap<String, CensusData> myMap = hazelcastInstance.getMap(MAP_NAME);
        try {
            CensusDataParser.populateDataMap(myMap, inPath);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Ahora el Job desde los pares(key, Value) que precisa MapReduce
        return KeyValueSource.fromMap(myMap);
    }
    private Job<String, CensusData> getCensusJob() throws Exception {
        return tracker.newJob(source);
    }

    public void setOutPath(String outPath) {
        this.outPath = outPath;
    }
}
