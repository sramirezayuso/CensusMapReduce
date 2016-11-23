package ar.edu.itba.pod.census.keypredicates;

import com.hazelcast.mapreduce.KeyPredicate;

public class ProvinceKeyPredicate implements KeyPredicate<String> {

    private final String desiredProvince;

    public ProvinceKeyPredicate(String desiredProvince) {
        this.desiredProvince = desiredProvince;
    }

    @Override
    public boolean evaluate(String key) {
        return key.contains(desiredProvince);
    }
}
