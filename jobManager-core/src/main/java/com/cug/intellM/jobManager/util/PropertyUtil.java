package com.cug.intellM.jobManager.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by zsm on 2018/10/25.
 */
public class PropertyUtil {

    public static Properties getProperty(Class clazz, String filename){
        Properties props = new Properties();
        try {
            props.load(clazz.getClassLoader().getResourceAsStream(filename));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }

    public static Properties getProperty(String filepath){
        Properties props = new Properties();
        try {
            InputStream inputStream = new FileInputStream(filepath);
            props.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }

}
