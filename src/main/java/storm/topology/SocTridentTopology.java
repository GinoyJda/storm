package storm.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;
import storm.trident.OutPrint;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 17-9-1
 * Time: 下午1:34
 * To change this template use File | Settings | File Templates.
 */
public class SocTridentTopology {

    public static void main(String[] args) throws AlreadyAliveException,InvalidTopologyException, AuthorizationException {
        TridentTopology topology = new TridentTopology();
        BrokerHosts brokerHosts = new ZkHosts("localhost:2181");
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(brokerHosts, "test1");

        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        OpaqueTridentKafkaSpout opaqueTridentKafkaSpout = new OpaqueTridentKafkaSpout(kafkaConfig);
        topology.newStream("kafka-trident", opaqueTridentKafkaSpout)
                .parallelismHint(3)
//                .aggregate(new Fields("str"),new LogAggr(),new Fields("sysout"));
                .each(new Fields("str"), new OutPrint(), new Fields("sysout"));
//                .each(new Fields("str"), new SpilterFunction(), new Fields("sentence"))
//                .groupBy(new Fields("sentence"))
//                .aggregate(new Fields("sentence"), new SumWord(),new Fields("sum")).parallelismHint(5)
//                .each(new Fields("sum"), new PrintFilter_partition());
        StormTopology stormTopology = topology.build();

//        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(new String[]{"127.0.0.1"}));
        config.put(Config.STORM_ZOOKEEPER_PORT,2181);
        config.put(Config.STORM_ZOOKEEPER_ROOT,"/storm/kafka");
        config.setDebug(false);

        StormSubmitter.submitTopology(args[0],config,stormTopology);
//        cluster.submitTopology("test", config,stormTopology);
    }
}
