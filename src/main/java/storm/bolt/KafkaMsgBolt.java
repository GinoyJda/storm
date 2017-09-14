package storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class KafkaMsgBolt extends BaseRichBolt{
    private OutputCollector collector;
    @Override
    public void execute(Tuple arg0) {
        String message = (String) arg0.getValue(0);
        System.out.print("kafka message is:"+message);
        collector.emit(new Values("forward",message));
 
    }

    @Override
    public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
        collector = arg2;  
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        arg0.declare(new Fields("key", "message"));
        
    }
    
}
