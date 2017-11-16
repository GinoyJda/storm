package storm.state;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class QueryLocation extends BaseQueryFunction<LocationDB, String> {
    public List<String> batchRetrieve(LocationDB state, List<TridentTuple> inputs) {
        List<String> ret = new ArrayList();
        for(TridentTuple input: inputs) {
            ret.add(state.getLocation(input.getString(0)));
        }
//        state.getTxid();
        return ret;
    }

    public void execute(TridentTuple tuple, String location, TridentCollector collector) {

        collector.emit(new Values(location, getRandom()));
    }

    private int getRandom() {
         int max=10;
         int min=0;
         Random random = new Random();
         return  random.nextInt(max)%(max-min+1) + min;
     }
}