package storm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 17-8-21
 * Time: 下午1:15
 * To change this template use File | Settings | File Templates.
 */
public class SpoutA extends BaseRichSpout {
        private SpoutOutputCollector collector;

        public void nextTuple() {
            String b = "{test a hdfs values}";
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            collector.emit(new Values("yjd",b));
        }

        public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("key","message"));
        }
}
