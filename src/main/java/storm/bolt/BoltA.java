package storm.bolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import storm.util.Utils;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 17-8-21
 * Time: 下午1:16
 * To change this template use File | Settings | File Templates.
 */
public class BoltA extends BaseRichBolt {

    private OutputCollector collector;
    @Override
    public void execute(Tuple arg0) {
        String word = (String) arg0.getValue(0);
        String out = " ########source is######### '" + "{\"id\":1,\"ide\":\"eclipse\",\"name\":\"Java\"}" + "'!";
        String uuid = Utils.getUUID();
        System.out.println(out + "  out:"+out);
        collector.emit(new Values("{\"id\":1,\"ide\":\"eclipse\",\"name\":\"Java\"}","test","test", uuid));
    }

    @Override
    public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
        collector = arg2;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
//        source, index, type, id
        arg0.declare(new Fields("source", "index","type","id"));

    }
}
