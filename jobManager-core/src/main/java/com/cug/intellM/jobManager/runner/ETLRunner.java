package com.cug.intellM.jobManager.runner;

import com.cug.intellM.jobManager.core.ETLResult;
import com.cug.intellM.jobManager.core.JobInfo;
import com.cug.intellM.jobManager.core.PluginLoader;
import com.cug.intellM.jobManager.plugin.JobManager;
import com.cug.intellM.jobManager.util.Log4jUtil;
import com.cug.intellM.jobManager.util.PluginUtils;
import com.cug.intellM.jobManager.util.RedisUtil;
import com.google.gson.Gson;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zsm on 2018/10/21.
 */
public class ETLRunner implements Runnable{
    public static Logger logger = Log4jUtil.getLogger(ETLRunner.class);

    private JobInfo jobInfo;
    public ETLRunner(JobInfo jobInfo){
        this.jobInfo = jobInfo;
    }

    public void run() {
        //设置策略执行状态，放入redis中，key为‘running-’+strategyId，value为0/1，1表示正在执行
        String runningState = "running-" + jobInfo.getTaskId();
        Jedis jedis = RedisUtil.getPoolConn();
        jedis.set(runningState, "1");

        Date nowDate = new Date();
        Long dataTime = nowDate.getTime();
        //dataX配置文件名称，
        String confFileName = "datax_job-" + dataTime + ".json";

        List<String> ls = new ArrayList<String>(2);
        //根据写入的数据源类别确定任务管理插件名称
        String pluginName = PluginLoader.getJobManagerName(jobInfo.getWriteDataSourceType());
        String pluginClassName = PluginLoader.getPluginClassName(pluginName);
        Class<?> cls = null;
        try {
            cls = PluginUtils.loadClass(pluginName, pluginClassName);
        } catch (Exception e) {
            logger.error("插件加载失败！");
        }
        JobManager jobManager = null;
        try {
            assert cls != null;
            jobManager = (JobManager)cls.newInstance();
        } catch (Exception e) {
            logger.error("JobManager实例化失败！");
        }
        //检查消息对象jobInfo中的sql，根据sql个数将其拆分为多个job进行执行
        String sql = jobInfo.getReadSql();
        String[] sqls = sql.split("#");
        String[] writeTables = jobInfo.getWriteTable().split(";");
        String[] writeTablesColumns = jobInfo.getWriterColumns().split(";");
        String[] writerColumnSizes = jobInfo.getWriterColumnSize().split(";");
        String[] writerColumnTypes = jobInfo.getWriterColumnTypes().split(";");
        String[] logArray = new String[sqls.length];
        String[] prepareLogArray = new String[sqls.length];
        String[] checkLogArray = new String[sqls.length];
        String isSuccess = "1";
        for(int i=0; i<sqls.length; i++){
            jobInfo.setReadSql(sqls[i]);
            jobInfo.setWriteTable(writeTables[i]);
            jobInfo.setWriterColumns(writeTablesColumns[i]);
            jobInfo.setWriterColumnTypes(writerColumnTypes[i]);
            jobInfo.setWriterColumnSize(writerColumnSizes[i]);

            try {
                jobManager.prepare(jobInfo);
            } catch (Exception e) {
                logger.error(e.toString());
                prepareLogArray[i] = e.toString();
            }
            try {
                jobManager.check(jobInfo);
            } catch (Exception e) {
                logger.error(e.toString());
                checkLogArray[i] = e.toString();
            }

            //生成调用具体插件，生成json格式的配置文件
            String conf = jobManager.getDataXConf(jobInfo);
            //将conf写入文件
            try {
                writeConfFile(conf, confFileName);
            } catch (IOException e) {
                logger.error(e.toString());
            }
            //执行任务，需要在服务器运行
            Process process = null;
            try {
                process = Runtime.getRuntime().exec("python datax/bin/datax.py conf/"+confFileName);
            } catch (IOException e) {
                logger.error(e.toString());
            }
            logger.info("正在执行etl任务，请稍后...");
            int returnCode = 0;
            try {
                //
                returnCode = process.waitFor();
            } catch (InterruptedException e) {
                logger.error(e.toString());
            }
            try{
                //取得命令结果的输出流
                InputStream fis=process.getInputStream();
                //用一个读输出流类去读
                InputStreamReader isr=new InputStreamReader(fis);
                //用缓冲器读行
                BufferedReader br=new BufferedReader(isr);
                String line;
                //直到读完为止
                StringBuilder log = new StringBuilder();
                while((line=br.readLine())!=null)
                {
                    log.append(line);
                }
                logArray[i] = formatLog(log.toString(), returnCode, writeTables[i]);
            }catch (Exception e){logger.error("打印成功日志失败...");}
            //执行失败
            if(returnCode != 0){
                isSuccess = "0";
            }
        }
        ls.add(isSuccess);
        String finalLog = "";
        for(int i=0; i<logArray.length; i++){
            if(prepareLogArray[i] != null && !prepareLogArray[i].equals("")){
                finalLog += "数据库连接失败："+prepareLogArray[i]+"\r\n"+"--------------------------" + "\r\n";
            }
            if(checkLogArray[i] != null && !checkLogArray[i].equals("")){
                finalLog += "建立数据库表单失败："+checkLogArray[i]+"\r\n"+"--------------------------" + "\r\n";
            }
            finalLog += logArray[i] + "\r\n" + "***************************" + "\r\n";
        }
        ls.add("执行日志，" + finalLog);
        setResult(ls);
        deleteConfFile(confFileName);
        //策略执行结束
        jedis.set(runningState, "0");
    }

    //处理datax输出日志，格式化、过滤
    public String formatLog(String log, int resultCode, String writeTable){
        String formatLog = "";
        String inputDate = "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}";
        Pattern p1 = Pattern.compile(inputDate);
        Matcher m1 = p1.matcher(log);
        //start用来表示最后一个匹配项的起始index
        int start = 0;
        while(m1.find()){
            start =m1.start();
        }
        //日志信息从最后一个index截取
        //空格替换
        log = log.substring(start);
        log=log.replaceAll(" +"," ");
        StringBuilder stringBuilder = new StringBuilder(log);
        if (resultCode == 0){
            int startIndex = stringBuilder.indexOf("任务启动时刻");
            stringBuilder.insert(startIndex, "\r\n");
            int endIndex = stringBuilder.indexOf("任务结束时刻");
            stringBuilder.insert(endIndex, "\r\n");
            int totalTimeIndex = stringBuilder.indexOf("任务总计耗时");
            stringBuilder.insert(totalTimeIndex, "\r\n");
            int avgFlowIndex = stringBuilder.indexOf("任务平均流量");
            stringBuilder.insert(avgFlowIndex, "\r\n");
            int writeSpeedIndex = stringBuilder.indexOf("记录写入速度");
            stringBuilder.insert(writeSpeedIndex, "\r\n");
            int readCountIndex = stringBuilder.indexOf("读出记录总数");
            stringBuilder.insert(readCountIndex, "\r\n");
            int failCountIndex = stringBuilder.indexOf("读写失败总数");
            stringBuilder.insert(failCountIndex, "\r\n");
        }
        formatLog = stringBuilder.toString();
        formatLog = "写入数据表单：" + writeTable + "\r\n" + formatLog;
        return formatLog;
    }

    //将执行结果日志写入消息队列
    public void setResult(List<String> result){

        Jedis jedis = RedisUtil.getPoolConn();
        //策略执行完毕的时间点
        Date nowDate = new Date();
        Long dataTime = nowDate.getTime();
        //执行结果
        ETLResult etlResult = null;

        String strategyId = jobInfo.getTaskId();
        Gson gson = new Gson();
        //查询redis中是否存在key为strategyId的键值对
        if(jedis.exists(strategyId)){
            //得到历史的etlResult
            String etlResultStr = jedis.get(strategyId);
            etlResult = gson.fromJson(etlResultStr, ETLResult.class);
            int index = (etlResult.getIndex()+1)%20;
            Integer[] labelArray = etlResult.getLabelArray();
            Long[] executeTimeArray = etlResult.getExecuteTimeArray();
            String[] logsArray = etlResult.getLogsArray();
            executeTimeArray[index] = dataTime;
            labelArray[index] = Integer.parseInt(result.get(0));
            logsArray[index] = result.get(1);
            etlResult.setIndex(index);
        }else{
            etlResult = new ETLResult();
            Integer[] labelArray = etlResult.getLabelArray();
            Long[] executeTimeArray = etlResult.getExecuteTimeArray();
            String[] logsArray = etlResult.getLogsArray();
            labelArray[0] = Integer.parseInt(result.get(0));
            executeTimeArray[0] = dataTime;
            logsArray[0] = result.get(1);
        }
        jedis.set(strategyId, gson.toJson(etlResult));
        if(result.get(0).equals("1")){
            logger.info(result.get(1));
            //将执行结果写入redis，
        }else{
            logger.error(result.get(1));
        }
    }


    public void writeConfFile(String conf, String confFileName) throws IOException {
        String confPath = PluginLoader.getConfPath(confFileName);
        //默认为覆盖写，BufferedWriter out = new BufferedWriter(new FileWriter("filename", true));追加写
        BufferedWriter out = new BufferedWriter(new FileWriter(confPath));
        out.write(conf);
        out.close();
    }

    public void deleteConfFile(String confFileName){
        File file = new File(PluginLoader.getConfPath(confFileName));
        // 如果文件路径所对应的文件存在，并且是一个文件，则直接删除
        if (file.exists() && file.isFile()) {
            if (file.delete()) {
                logger.info("删除配置文件成功！");
            } else {
                logger.error("删除配置文件失败！");
            }
        }
    }

}
