package com.cug.intellM.jobManager.core;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cug.intellM.jobManager.util.Configuration;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zsm on 2018/9/30.
 */

//读取配置文件，加载插件配置信息
public class PluginLoader {

    public static final String PLUGIN_JSON = "core.json";
    //public static final String FILE_SEPARATOR = File.separator;
    private static Map<String, String> pluginMap;
    //存储数据源类别及其对应的任务管理插件名称
    private static Map<String, String> dataSourceJobMangerMap;
    //存储数据源类别及其对应的DataX读取插件名称
    private static Map<String, String> dataSourceDataxReaderMap;
    //存储数据源类别及其对应的DataX写入插件名称
    private static Map<String, String> dataSourceDataxWriterMap;


    public static String getPluginClassName(String name){
        if (!pluginMap.containsKey(name)){
            throw new RuntimeException("该插件jobManager插件名称和类名未在core.json中配置，请仔细核对插件配置文件");
        }
        return pluginMap.get(name);
    }

    public static String getJobManagerName(String dataSource){
        if (!dataSourceJobMangerMap.containsKey(dataSource)){
            throw new RuntimeException("该数据源类别的JobManager插件未在core.json中配置，请仔细核对插件配置文件");
        }
        return dataSourceJobMangerMap.get(dataSource);
    }

    public static String getDataxReaderName(String dataSource){
        if (!dataSourceDataxReaderMap.containsKey(dataSource)){
            throw new RuntimeException("该数据源类别的Writer插件未在core.json中配置，请仔细核对插件配置文件");
        }
        return dataSourceDataxReaderMap.get(dataSource);
    }

    public static String getDataxWriterName(String dataSource){
        if (!dataSourceDataxWriterMap.containsKey(dataSource)){
            throw new RuntimeException("该数据源类别的Reader插件未在core.json中配置，请仔细核对插件配置文件");
        }
        return dataSourceDataxWriterMap.get(dataSource);
    }

    static {
        pluginMap = new HashMap<String, String>();
        dataSourceJobMangerMap = new HashMap<String, String>();
        dataSourceDataxReaderMap = new HashMap<String, String>();
        dataSourceDataxWriterMap = new HashMap<String, String>();
        String confPath = getConfPath(PLUGIN_JSON);
        File coreFile = new File(confPath);
        Configuration conf = Configuration.from(coreFile);
        JSONArray ls = (JSONArray)conf.get("plugins");
        //将所有的插件信息存入pluginMap中
        for (Object o : ls){
            JSONObject json = (JSONObject)o;
            String pluginName = json.getString("pluginName");
            String pluginClass = json.getString("pluginClass");
            pluginMap.put(pluginName, pluginClass);
        }
        JSONArray ls2 = (JSONArray)conf.get("dataSource_JobManagerName");
        for (Object o : ls2){
            JSONObject json = (JSONObject)o;
            String pluginName = json.getString("dataType");
            String pluginClass = json.getString("JobManagerName");
            dataSourceJobMangerMap.put(pluginName, pluginClass);
        }
        JSONArray ls3 = (JSONArray)conf.get("dataSource_DataxReader");
        for (Object o : ls3){
            JSONObject json = (JSONObject)o;
            String pluginName = json.getString("dataType");
            String pluginClass = json.getString("dataxReaderPlugin");
            dataSourceDataxReaderMap.put(pluginName, pluginClass);
        }
        JSONArray ls4 = (JSONArray)conf.get("dataSource_DataxWriter");
        for (Object o : ls4){
            JSONObject json = (JSONObject)o;
            String pluginName = json.getString("dataType");
            String pluginClass = json.getString("dataxWriterPlugin");
            dataSourceDataxWriterMap.put(pluginName, pluginClass);
        }
    }

    //得到插件的配置文件完整的路径
    public static String getConfPath(String filename){
        String dir = PluginLoader.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        try {
            dir=URLDecoder.decode(dir,"utf-8");//转化成utf-8编码
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        String confDir;
        //通过jar运行时路径的路径处理
        if ("jar".equals(dir.substring(dir.length()-3))){
            dir = dir.substring(0,dir.lastIndexOf('/'));
            confDir = dir+"/conf/" + filename;
        }else{
            int num = 3;
            for (int i=0; i < num; i++){
                dir = dir.substring(0,dir.lastIndexOf('/'));
            }
            confDir = dir + "/src/"+"main/"+"conf/" + filename;
        }
        return confDir;
    }

}
