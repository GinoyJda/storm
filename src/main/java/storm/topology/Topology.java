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
import storm.bolt.BoltA;
import storm.bolt.KafkaMsgBolt;
import storm.es.NewEsConfig;
import storm.es.NewEsIndexBolt;
import storm.redis.RedisBolt;
import storm.spout.SpoutA;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.elasticsearch.common.DefaultEsTupleMapper;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 17-8-21
 * Time: 上午10:54
 * To change this template use File | Settings | File Templates.
 */
public class Topology {
    public static boolean  kafka = false;
    public static boolean  redis = false;

    public static void main(String args[]) throws AuthorizationException, AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();

        if(kafka){
            //KafkaSpout
            builtKafkaSpout(builder);
            builder.setBolt("BoltA",new BoltA(),1).shuffleGrouping("kafka-spout");
//            builtEsIndexBolt(builder);
        }else if(redis){
            //KafkaSpout
            builtKafkaSpout(builder);
            builder.setBolt("BoltA",new BoltA(),1).shuffleGrouping("kafka-spout");
            builtRedisBolt(builder);
        }else{
            //普通Spout
            builder.setSpout("SpoutA",new SpoutA(),1);
//            builder.setBolt("BoltA",new BoltA(),1).shuffleGrouping("SpoutA");
            builder.setBolt("Hdfs", HdfsConfig()).shuffleGrouping("SpoutA");
            //构建KafkaBolt
//            builtKafkaBolt(builder);
        }



        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
//            Utils.sleep(20000);
//            cluster.killTopology("soc");
//            cluster.shutdown();
        }
    }

    /**
     * 构建KafkaBolt
     * metadata.broker.list=10.2.4.13:9092,10.2.4.14:9092,10.2.4.12:9092
     * bootstrap.servers=10.2.4.13:9092,10.2.4.14:9092,10.2.4.12:9092
     * producer.type=async
     * request.required.ack=1
     * serializer.class=kafka.serializer.StringEncoder
     * key.serializer=org.apache.kafka.common.serialization.StringSerializer
     * value.serializer=org.apache.kafka.common.serialization.StringSerializer
     * sendtopic=test
     */
    private static void builtKafkaBolt(TopologyBuilder builder){
        //kafka producer config
        Properties prop = new Properties();
        prop.put("metadata.broker.list", "10.240.1.233:9092");
        prop.put("bootstrap.servers", "10.240.1.233:9092");
        prop.put("producer.type","async");
        prop.put("request.required.acks","1");
        prop.put("serializer.class", "kafka.serializer.StringEncoder");
        prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //kafkaBolt
        KafkaBolt bolt = new KafkaBolt();
        bolt.withTopicSelector(new DefaultTopicSelector("test"));
        bolt.withProducerProperties(prop);
        bolt.withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
        //构建KafkaBolt
        builder.setBolt("msgSentenceBolt", new KafkaMsgBolt()).shuffleGrouping("BoltA");
        //转发kafka消息
        builder.setBolt("forwardToKafka", bolt).shuffleGrouping("msgSentenceBolt");

    }

    /**
     * 构建KafkaSpout
     */
    private static void builtKafkaSpout(TopologyBuilder builder) {
        try {
            BrokerHosts brokerHosts = new ZkHosts("10.240.1.233:2181");
            SpoutConfig spoutConf = new SpoutConfig(brokerHosts, "test", "/consumer/data", "group_yjd");
            spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
            spoutConf.zkPort = 2181;
            spoutConf.ignoreZkOffsets = false; // 每次消费都从头开始，用于性能测试
            List<String> servers = new ArrayList<String>();
            String ZK_SERVERS = "10.240.1.233";
            if (ZK_SERVERS != null) {
                String[] arr = ZK_SERVERS.split(",");
                for (int i = 0; i < arr.length; i++) {
                    if (!("".equals(arr[i]))) {
                        servers.add(arr[i]);
                    }
                }
            }
            spoutConf.zkServers = servers;
            builder.setSpout("kafka-spout", new KafkaSpout(spoutConf),1);
        } catch (Exception e) {

        }
    }

    /**
     * 构建ElasticBolt
     */
    private static void builtEsIndexBolt(TopologyBuilder builder){
        NewEsConfig esConfig = new NewEsConfig("SOC-15", new String[]{"10.2.4.15","10.2.4.42","10.2.4.43"});
        EsTupleMapper tupleMapper = new DefaultEsTupleMapper();
        NewEsIndexBolt indexBolt = new NewEsIndexBolt(esConfig, tupleMapper);
        builder.setBolt("es-bolt",indexBolt,1).shuffleGrouping("BoltA");
    }

    /**
     * 构建RedisBolt
     */
    private static void builtRedisBolt(TopologyBuilder builder){
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder().setHost("10.2.4.12").setPort(6379).build();
        RedisBolt rb = new  RedisBolt(poolConfig);
        builder.setBolt("redis-bolt",rb,1).shuffleGrouping("BoltA");
    }


    private static HdfsBolt HdfsConfig(){
        // 输出字段分隔符
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");

        // 每1000个tuple同步到HDFS一次
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // 每个写出文件的大小为100MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(100.0f, FileSizeRotationPolicy.Units.MB);

        // 设置输出目录
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/soc/hadoop/tmp/dfs/data");

        // 执行HDFS地址
        HdfsBolt hdfsBolt = new HdfsBolt()
                .withFsUrl("hdfs://10.176.63.105:9000")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);
        return  hdfsBolt;

    }

}
