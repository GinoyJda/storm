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
 * Time: 下午2:26
 * To change this template use File | Settings | File Templates.
 */
public class  LogAggr  extends BaseAggregator<Map<String, Integer>> {
    private  int i  = 0;
    @Override
    public Map<String, Integer> init(Object o, TridentCollector tridentCollector) {
        return new HashMap<String, Integer>();
    }

    @Override
    public void aggregate(Map<String, Integer> stringIntegerMap, TridentTuple objects, TridentCollector tridentCollector) {
        String location = objects.getString(0);
        System.out.println(location);
        if(location.split(";").length > 3) {
            System.out.println(location.split(";")[1]);
            if(location.split(";")[1].split(":")[0].equals("danger_degree")){
                Integer i = stringIntegerMap.get(location.split(";")[1].split(":")[1]);
                if(null == i){
                    i = 0;
                }else{
                    i = i+1;
                }
                stringIntegerMap.put(location.split(";")[1].split(":")[1], i);
            }
        }
    }

    @Override
    public void complete(Map<String, Integer> stringIntegerMap, TridentCollector collector) {
        for (String key : stringIntegerMap.keySet()) {
            System.out.println("key= "+ key + " and value= " + stringIntegerMap.get(key));
        }
        collector.emit(new Values(stringIntegerMap));
    }
}
