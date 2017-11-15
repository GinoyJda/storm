package storm.state;

import org.apache.storm.trident.state.State;

public class LocationDB implements State {
    public void beginCommit(Long txid) {
    }

    public void commit(Long txid) {
    }

    public void setLocation(long userId, String location) {
        // code to access database and set location
    }

    public String getLocation(long userId) {
        // code to get location from database
        return "localhost";
    }
}