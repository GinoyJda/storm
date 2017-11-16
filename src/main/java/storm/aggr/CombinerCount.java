package storm.aggr;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

public class CombinerCount implements CombinerAggregator<Integer> {
    @Override
    public Integer init(TridentTuple tuple) {
        return 1;
    }
    @Override
    public Integer combine(Integer val1, Integer val2) {

        return val1 + val2;
    }
    @Override
    public Integer zero() {
        return 0;
    }
}