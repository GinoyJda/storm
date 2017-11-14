import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA. 
 * User: yl3395017 
 * Date: 17-7-27 
 * Time: 上午9:00 
 * To change this template use File | Settings | File Templates. 
 */
public class RedisUtils {
    private static String master_ip;
    private static JedisPoolConfig config;
    private static JedisPool pool;
    private static RedisUtils redisUtil;

    static{
        redisUtil = new RedisUtils();
        InputStream is = RedisUtils.class.getResourceAsStream("/redis.properties");
        Properties props = new Properties();
        try {
            props.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }

        master_ip =    props.getProperty("master-ip","10.2.4.12");
    }

    /**
     * 单例模式 
     * */
    public static RedisUtils getInstance(){
        if(null == redisUtil){
            return new RedisUtils();
        }else{
            return  redisUtil;
        }
    }

    /**
     * 获取化连接池配置 
     * @return JedisPoolConfig
     * */
    private JedisPoolConfig getPoolConfig(){
        if(config == null){
            config = new JedisPoolConfig();
            //最大连接数  
            config.setMaxTotal(100);
            //最大空闲连接数  
            config.setMaxIdle(5);
        }
        return config;
    }

    /**
     * 初始化JedisPool 
     * */
    private void initJedisPool(){
        if(pool == null){
            //获取服务器IP地址  
            String ipStr = master_ip;
            //获取服务器端口  
            int portStr = 6379;
            //初始化连接池  
            pool = new JedisPool(getPoolConfig(), ipStr,portStr);
        }
    }

    private void setValue(){
        Jedis jedis = pool.getResource();
        jedis.set("test","world");
    }

    private void getValue(){
        System.out.println( pool.getResource().get("test"));
    }

    public static void main(String args[]){
        RedisUtils util = getInstance();
        util.initJedisPool();
        util.setValue();
        util.getValue();
    }
}