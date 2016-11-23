package ar.edu.itba.pod;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CensusServer {

    private static final Logger LOGGER = LogManager.getLogger("CensusServer");

    public static void main(String[] args) {

        Config cfg = new Config();
        cfg.getGroupConfig().setName("52561-53346");

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);

        LOGGER.info("Server initialized");
    }
}
