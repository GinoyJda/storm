package storm.state;

import org.apache.storm.trident.state.State;

public class LocationDB implements State {
    Long tx;
    public void beginCommit(Long txid) {
         tx = txid;
    }

    public void commit(Long txid) {
    }

    public void getTxid(){
        System.out.println("txid:"+tx);
    }

    public void setLocation(long userId, String location) {
        // code to access database and set location
    }

    public String getLocation(String userId) {
        // code to get location from database
        return "localhost";
    }
}