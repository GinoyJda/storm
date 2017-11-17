package storm.aggr;

import org.apache.storm.trident.operation.*;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

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
public class  AggrTest  extends BaseAggregator<Map<String, Integer>> {
    private String txid;
    private int count;
    private int  numPartitions;
    private int  partitionId;
    @Override

    public void prepare(Map conf, TridentOperationContext context) {
        partitionId = context.getPartitionIndex();
        numPartitions = context.numPartitions();
    }


    @Override
    public Map<String, Integer> init(Object o, TridentCollector tridentCollector) {
//        System.out.println("start batch txid is "+o.toString());
        txid = o.toString();
        Map<String, Integer> m = new HashMap<String, Integer>();
        count = 0;
        System.out.println("numPartitions["+numPartitions+"] "+"partitionId["+partitionId+"] "+"Txid["+txid+"] "+"Thread["+Thread.currentThread().getId()+"] "+" INIT");
        return  m;
    }

    @Override
    public void aggregate(Map<String, Integer> stringIntegerMap, TridentTuple objects, TridentCollector tridentCollector) {
       System.out.println("numPartitions["+numPartitions+"] "+"partitionId["+partitionId+"] "+"Txid["+txid+"] "+"Thread["+Thread.currentThread().getId()+"] "+" Str["+objects.getStringByField("a")+"] ");
//        System.out.println("middle patch txid is "+txid+" batch size is "+objects.size());
        count = count + 1;
        stringIntegerMap.put("size",count);
    }

    @Override
    public void complete(Map<String, Integer> stringIntegerMap, TridentCollector collector) {
        System.out.println("numPartitions["+numPartitions+"] "+"partitionId["+partitionId+"] "+"Txid["+txid+"] "+"Thread["+Thread.currentThread().getId()+"] "+" Count["+stringIntegerMap.get("size")+"] ");
        collector.emit(new Values(stringIntegerMap.get("size")));
    }
}
