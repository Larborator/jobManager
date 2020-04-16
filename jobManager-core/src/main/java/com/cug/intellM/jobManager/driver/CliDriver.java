package com.cug.intellM.jobManager.driver;

import com.cug.intellM.jobManager.core.ETLResult;
import com.cug.intellM.jobManager.core.JobInfo;
import com.cug.intellM.jobManager.core.PluginLoader;
import com.cug.intellM.jobManager.plugin.JobManager;
import com.cug.intellM.jobManager.runner.ETLRunner;
import com.cug.intellM.jobManager.util.Log4jUtil;
import com.cug.intellM.jobManager.util.PluginUtils;
import com.cug.intellM.jobManager.util.PropertyUtil;
import com.cug.intellM.jobManager.util.RedisUtil;
import com.google.gson.Gson;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zsm on 2018/9/30.
 * 程序入口
 */
public class CliDriver {

    public static Logger logger = Log4jUtil.getLogger(CliDriver.class);
    public final static int THREAD_NUM = 128;

    public static void main(String[] args) throws Exception {

        //CliDriver cliDriver = new CliDriver();
        Properties props = PropertyUtil.getProperty(PluginLoader.getConfPath("db.properties"));
        String queneName = (String)props.get("redis.jobQueue.name");
        Jedis jedis = RedisUtil.getPoolConn();

        //查看服务是否运行
        //System.out.println("服务正在运行: "+jedis.ping());
        Gson gson = new Gson();
        ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_NUM);

        logger.info("程序开始执行...");
        while(true){
            //以阻塞的方式监听etl任务消息队列
            List<String> job = jedis.brpop(0, queneName);
            logger.info("得到任务请求: "+job.get(1));
            String jobInfoStr = job.get(1);
            JobInfo jobInfo = gson.fromJson(jobInfoStr, JobInfo.class);
            threadPool.submit(new ETLRunner(jobInfo));
        }
    }
}
