package storm.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import storm.spout.FixedBatchSpout;
import storm.state.LocationDBFactory;
import storm.state.QueryLocation;
import storm.trident.OutPrint;
import storm.trident.Split;

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 17-11-15
 * Time: 下午4:34
 * To change this template use File | Settings | File Templates.
 */
public class TridentStateToplogy {
    public static void main(String[] args){
        TridentTopology topology = new TridentTopology();
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 1,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"));
        spout.setCycle(true);

        TridentState locations = topology.newStaticState(new LocationDBFactory());

        topology.newStream("sentence", spout)
                .stateQuery(locations, new Fields("sentence"), new QueryLocation(), new Fields("location"))
                .each(new Fields("location"), new OutPrint(),new Fields("sysout"));

        StormTopology stormTopology = topology.build();
        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        config.setDebug(false);
        cluster.submitTopology("test", config,stormTopology);
    }

}
