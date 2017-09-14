package storm.topology;

import storm.trident.LogAggr;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
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

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 17-9-1
 * Time: 下午1:34
 * To change this template use File | Settings | File Templates.
 */
public class TridentKafkaTopology {

    public static void main(String[] args) throws AlreadyAliveException,InvalidTopologyException, AuthorizationException {
        TridentTopology topology = new TridentTopology();
        BrokerHosts brokerHosts = new ZkHosts("10.2.4.12:2181,10.2.4.13:2181,10.2.4.14:2181");
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(brokerHosts, "test_rce_yjd");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        OpaqueTridentKafkaSpout opaqueTridentKafkaSpout = new OpaqueTridentKafkaSpout(kafkaConfig);
        topology.newStream("kafka-trident", opaqueTridentKafkaSpout)
                .parallelismHint(3)
                .aggregate(new Fields("str"),new LogAggr(),new Fields("sysout"));
//                .each(new Fields("str"), new OutPrint(), new Fields("sysout"));
//                .each(new Fields("str"), new SpilterFunction(), new Fields("sentence"))
//                .groupBy(new Fields("sentence"))
//                .aggregate(new Fields("sentence"), new SumWord(),new Fields("sum")).parallelismHint(5)
//                .each(new Fields("sum"), new PrintFilter_partition());
        StormTopology stormTopology = topology.build();
        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        config.setDebug(false);
        cluster.submitTopology("test", config,stormTopology);
    }
}
