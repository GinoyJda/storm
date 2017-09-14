package storm.trident;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 17-9-1
 * Time: 上午10:36
 * To change this template use File | Settings | File Templates.
 */
public  class WordAggregat extends BaseAggregator<Map<String, Integer>> {

    public static  Map<String, Integer> map =  new HashMap<String, Integer>();

    @Override
    public Map<String, Integer> init(Object batchId, TridentCollector collector) {
        return new HashMap<String, Integer>();
    }

    @Override
    public void aggregate(Map<String, Integer> val, TridentTuple tuple,
                          TridentCollector collector) {
        String location = tuple.getString(0);
        Integer i = map.get(location);
        if(null == i){
               i = 0;
        }else{
            i = i+1;
        }
        map.put(location, i);
    }

    @Override
    public void complete(Map<String, Integer> val, TridentCollector collector) {
        for (String key : map.keySet()) {
            System.out.println("key= "+ key + " and value= " + map.get(key));
        }
        collector.emit(new Values(map));
    }
}
