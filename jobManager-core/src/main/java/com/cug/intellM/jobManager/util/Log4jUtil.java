package com.cug.intellM.jobManager.util;

import com.cug.intellM.jobManager.core.PluginLoader;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Created by zsm on 2018/10/27.
 * 写日志文件工具类
 */
public class Log4jUtil {

    private static final String LOG_PROPERTY_FILENAME = "log4j.properties";


    static {
        String filepath = PluginLoader.getConfPath(LOG_PROPERTY_FILENAME);
        //使用conf目录下的配置信息
        PropertyConfigurator.configure(filepath);
    }

    public static Logger getLogger(Class clazz){
        return Logger.getLogger(clazz);
    }


}
