package storm.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import storm.aggr.AggrTest;
import storm.aggr.CombinerCount;
import storm.spout.FixedBatchSpout;
import storm.state.LocationDBFactory;
import storm.state.QueryLocation;
import storm.trident.OutPrint;
import storm.trident.Split;
import storm.util.OutPrintUtil;

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
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"),10,
                new Values("the"),
                new Values("man"),
                new Values("four"),
                new Values("how"),

                new Values("the"),
                new Values("man"),
                new Values("four"),
                new Values("how"),

                new Values("the"),
                new Values("man"),
                new Values("four"),
                new Values("how"),

                new Values("the"),
                new Values("man"),
                new Values("four"),
                new Values("how")
                );
//        spout.setCycle(true);

        TridentState locations = topology.newStaticState(new LocationDBFactory());
        topology.newStream("sentence", spout).each(new Fields("sentence"),new OutPrint(),new Fields("a","b")) //a int b str
//        .shuffle()
        .partitionBy(new Fields("a"))
//        .groupBy(new Fields("a"))
//        .aggregate(new Fields("a"), new AggrTest(), new Fields("b")).parallelismHint(3) ; //正常聚合 不分组
        .partitionAggregate(new Fields("a","b"), new AggrTest(), new Fields("c")).parallelismHint(4);

//                .each(new Fields("a"),new OutPrintUtil(),new Fields(""));
//                .groupBy(new Fields("sentence"))
//                .stateQuery(locations, new Fields("sentence"), new QueryLocation(), new Fields("location", "ids"))
//                .groupBy(new Fields("ids"));
//                .partitionAggregate(new Fields("ids"),new CombinerCount(),new Fields("ids"));
//                .each(new Fields("location"), new OutPrint(),new Fields("sysout")).partitionBy(new Fields("sysout"));

        StormTopology stormTopology = topology.build();
        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        config.setDebug(false);
        cluster.submitTopology("test", config,stormTopology);
    }

}
