package storm.topology;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import storm.spout.FixedBatchSpout;
import storm.trident.Split;
import storm.trident.WordAggregat;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

public class TTopology {

    public static void main(String[] args){
        TridentTopology topology = new TridentTopology();
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 1,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"));
        spout.setCycle(true);
//        RedisClusterState.Factory factory = new RedisClusterState.Factory(redisConfig());
        topology.newStream("batch-spout",spout).parallelismHint(2)
                .each(new Fields("sentence"), new Split(), new Fields("word")).project(new Fields("word")) //project方法只发送word字段
                .groupBy(new Fields("word"))
//                .each(new Fields("word"), new WordFilter("the")).project((new Fields("word"))).partitionBy(new Fields("word")).parallelismHint(3) //parallelismHint 并行度
                .aggregate(new Fields("word"),new WordAggregat(),new Fields("agg"));
        StormTopology stormTopology = topology.build();
        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        conf.setDebug(true);
        cluster.submitTopology("soc", conf,stormTopology);
    }


    private static JedisClusterConfig redisConfig(){
        String redisHostPort = "10.2.4.12:6379,10.2.4.13:6379,10.2.4.14:6379";
        Set<InetSocketAddress> nodes = new HashSet<InetSocketAddress>();
        for (String hostPort : redisHostPort.split(",")) {
            String[] host_port = hostPort.split(":");
            nodes.add(new InetSocketAddress(host_port[0], Integer.valueOf(host_port[1])));
            System.out.println("host_port[0]:"+host_port[0] +"host_port[1] :"+host_port[1]);
        }
        JedisClusterConfig clusterConfig = new JedisClusterConfig.Builder().setNodes(nodes).build();
        return clusterConfig;

    }


}
