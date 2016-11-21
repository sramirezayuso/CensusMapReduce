package ar.edu.itba.pod;

import java.util.Map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import ar.edu.itba.pod.census.CensusData;
import ar.edu.itba.pod.census.ageGroup.AgeGroupMapperFactory;
import ar.edu.itba.pod.census.ageGroup.AgeGroupReducerFactory;

public class CensusClient {

    private static final String MAP_NAME = "52561";

    public static void main(String[] args) throws Exception {

        ClientConfig ccfg = new ClientConfig();

        String addresses = System.getProperty("addresses");
        if (addresses != null) {
            String[] arrayAddresses = addresses.split("[,;]");
            ClientNetworkConfig net = new ClientNetworkConfig();
            net.addAddress(arrayAddresses);
            ccfg.setNetworkConfig(net);
        }
        HazelcastInstance client = HazelcastClient.newHazelcastClient(ccfg);

        System.out.println(client.getCluster());

        IMap<Long, CensusData> myMap = client.getMap(MAP_NAME);
        CensusDataParser.populateDataMap(myMap, args[0]);

        // Ahora el JobTracker y los Workers!

        JobTracker tracker = client.getJobTracker("default");

        // Ahora el Job desde los pares(key, Value) que precisa MapReduce
        KeyValueSource<Long, CensusData> source = KeyValueSource.fromMap(myMap);
        Job<Long, CensusData> job = tracker.newJob(source);

        // // Orquestacion de Jobs y lanzamiento
        ICompletableFuture<Map<Integer, Long>> future = job
                .mapper(new AgeGroupMapperFactory())
                .reducer(new AgeGroupReducerFactory())
                .submit();

        // Tomar resultado e Imprimirlo
        Map<Integer, Long> rta = future.get();

        for (Map.Entry<Integer, Long> e : rta.entrySet()) {
            System.out.println(String.format("Age Group %s => Number Of Members %s", e.getKey(), e.getValue()));
        }

        System.exit(0);

    }
}
