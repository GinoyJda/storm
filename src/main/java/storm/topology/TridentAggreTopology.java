package storm.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import storm.spout.FixedBatchSpout;
import storm.trident.Split;
import storm.trident.WordAggregat;

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 17-9-20
 * Time: 上午10:48
 * To change this template use File | Settings | File Templates.
 */
public class TridentAggreTopology {

    public static void main(String args[]){
        TridentTopology topology = new TridentTopology();
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 1,
                new Values("the cow jumped "),
                new Values("cow jumped"),
                new Values("jumped"));
//        spout.setCycle(true);

        topology.newStream("batch-spout",spout)
                .each(new Fields("sentence"), new Split(), new Fields("word"))    //分割
                .partitionBy(new Fields("word"))
                .partitionAggregate(new Fields("word"), new WordAggregat(), new Fields("agg"));


        StormTopology stormTopology = topology.build();
        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        conf.setDebug(true);
        cluster.submitTopology("soc", conf,stormTopology);

    }

}
