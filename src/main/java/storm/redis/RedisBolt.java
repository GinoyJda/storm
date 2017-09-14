package storm.redis;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.JedisCommands;

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 17-8-23
 * Time: 下午2:00
 * To change this template use File | Settings | File Templates.
 */
public class RedisBolt extends AbstractRedisBolt {
    public RedisBolt(JedisPoolConfig config) {
        super(config);
    }

    @Override
    public void execute(Tuple input) {
        JedisCommands jedisCommands = null;
        try {
            jedisCommands = getInstance();
            String wordName = input.getStringByField("index");
            String countStr = jedisCommands.get(wordName);
            if (countStr != null) {
                System.out.print("#######countStr#########:"+countStr);
            } else {

            }
        } finally {
            if (jedisCommands != null) {
                returnInstance(jedisCommands);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
