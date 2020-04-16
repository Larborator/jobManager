package com.cug.intellM.jobManager.util;

import com.cug.intellM.jobManager.core.PluginLoader;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Properties;

/**
 * Created by zsm on 2018/10/24.
 */
public class RedisUtil {

    private static JedisPool pool = null;

    static {
        Properties props = PropertyUtil.getProperty(PluginLoader.getConfPath("db.properties"));
        String ip = (String) props.get("redis.server");
        int port = Integer.valueOf((String) props.get("redis.port"));
        int timeout = Integer.valueOf((String) props.get("redis.timeOut"));
        String password = (String) props.get("redis.password");
        int maxTotal = Integer.valueOf((String) props.get("redis.maxTotal"));
        int maxIdle = Integer.valueOf((String) props.get("redis.maxIdle"));
        //创建jedis连接池配置
        JedisPoolConfig config = new JedisPoolConfig();
        //最大连接数
        config.setMaxTotal(maxTotal);
        //最大空闲连接
        config.setMaxIdle(maxIdle);
        //创建redis连接池
        pool = new JedisPool(config,ip,port,timeout,password);
    }

    /**
     * 获取jedis连接池的连接
     * */
    public static Jedis getPoolConn() {
        return pool.getResource();
    }

}
