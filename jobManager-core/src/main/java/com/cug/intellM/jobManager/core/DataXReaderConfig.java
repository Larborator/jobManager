package com.cug.intellM.jobManager.core;

import com.cug.intellM.jobManager.util.Configuration;
import com.cug.intellM.jobManager.util.DataXConfigConstants;
import com.cug.intellM.jobManager.util.JDBCUtils;

import java.util.ArrayList;

/**
 * Created by zsm on 2018/10/27.
 * 生成DataX配置文件的reader部分
 */
public class DataXReaderConfig {

    //生成mysql的reader配置，方法名必须与core.json中的dataSource_DataxReader下的dataxReaderPlugin对应的值相同
    public static Configuration mysqlreader(final JobInfo jobInfo){

        final Configuration readerConnConf = Configuration.from("{}");
        readerConnConf.set(DataXConfigConstants.READER_QUERYSQL, new ArrayList<String>(){{
//            add(JDBCUtils.getQuerySqlByParams(jobInfo.getReadTableColumns(),
//                    jobInfo.getJoinCondition(),jobInfo.getDataClean()));
//            add(JDBCUtils.getQuerySqlByParams2(jobInfo.getReadTables(),jobInfo.getReadColumns(),
//                    jobInfo.getJoinCondition(),jobInfo.getDataClean()));
            add(jobInfo.getReadSql());
        }});
        readerConnConf.set(DataXConfigConstants.READER_URL, new ArrayList<String>(){{add(jobInfo.getReadUrl());}});

        Configuration readerParamConf = Configuration.from("{}");
        readerParamConf.set(DataXConfigConstants.READER_CONN, new ArrayList<Object>(){{
            add(readerConnConf.getInternal());
        }});
        readerParamConf.set(DataXConfigConstants.READER_USERNAME, jobInfo.getReadUsername());
        readerParamConf.set(DataXConfigConstants.READER_PASSWORD, jobInfo.getReadPassword());

        final Configuration rwConf = Configuration.from("{}");
        rwConf.set(DataXConfigConstants.READER_PARAM, readerParamConf.getInternal());
        rwConf.set(DataXConfigConstants.READER_NAME, PluginLoader.getDataxReaderName(jobInfo.getReadDataSourceType()));

        return rwConf;

    }

    public static Configuration oraclereader(final JobInfo jobInfo){
        final Configuration readerConnConf = Configuration.from("{}");
        readerConnConf.set(DataXConfigConstants.READER_QUERYSQL, new ArrayList<String>(){{
            add(jobInfo.getReadSql());
        }});
        readerConnConf.set(DataXConfigConstants.READER_URL, new ArrayList<String>(){{add(jobInfo.getReadUrl());}});
        Configuration readerParamConf = Configuration.from("{}");
        readerParamConf.set(DataXConfigConstants.READER_CONN, new ArrayList<Object>(){{
            add(readerConnConf.getInternal());
        }});
        readerParamConf.set(DataXConfigConstants.READER_USERNAME, jobInfo.getReadUsername());
        readerParamConf.set(DataXConfigConstants.READER_PASSWORD, jobInfo.getReadPassword());

        final Configuration rwConf = Configuration.from("{}");
        rwConf.set(DataXConfigConstants.READER_PARAM, readerParamConf.getInternal());
        rwConf.set(DataXConfigConstants.READER_NAME, PluginLoader.getDataxReaderName(jobInfo.getReadDataSourceType()));
        return rwConf;
    }

    public static Configuration hivereader(final JobInfo jobInfo){
        final Configuration readerConnConf = Configuration.from("{}");
        readerConnConf.set(DataXConfigConstants.READER_QUERYSQL, new ArrayList<String>(){{
            add(jobInfo.getReadSql());
        }});
        readerConnConf.set(DataXConfigConstants.READER_URL, new ArrayList<String>(){{add(jobInfo.getReadUrl());}});

        Configuration readerParamConf = Configuration.from("{}");
        readerParamConf.set(DataXConfigConstants.READER_CONN, new ArrayList<Object>(){{
            add(readerConnConf.getInternal());
        }});

        readerParamConf.set(DataXConfigConstants.READER_USERNAME, jobInfo.getReadUsername());
        readerParamConf.set(DataXConfigConstants.READER_PASSWORD, jobInfo.getReadPassword());

        final Configuration rwConf = Configuration.from("{}");
        rwConf.set(DataXConfigConstants.READER_NAME, PluginLoader.getDataxReaderName(jobInfo.getReadDataSourceType()));
        rwConf.set(DataXConfigConstants.READER_PARAM, readerParamConf.getInternal());
        return rwConf;
    }

}
