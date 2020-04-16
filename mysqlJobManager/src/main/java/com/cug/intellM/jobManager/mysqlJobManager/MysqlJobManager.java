package com.cug.intellM.jobManager.mysqlJobManager;

import com.cug.intellM.jobManager.core.DataXReaderConfig;
import com.cug.intellM.jobManager.core.JobInfo;
import com.cug.intellM.jobManager.core.PluginLoader;
import com.cug.intellM.jobManager.plugin.JobManager;
import com.cug.intellM.jobManager.util.Configuration;
import com.cug.intellM.jobManager.util.DataXConfigConstants;
import com.cug.intellM.jobManager.util.JDBCUtils;
import com.cug.intellM.jobManager.util.Log4jUtil;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * Created by zsm on 2018/9/30.
 */

//包外继承抽象类，抽象类的抽象方法必须声明为public，否则子类无法实现抽象方法
public class MysqlJobManager extends JobManager{

    private static final String DRIVER_CLASSNAME = "com.mysql.jdbc.Driver";
    private static final String WRITER_MODE = "insert";
    private static final int JOB_CHANNEL = 3;
    private static final String DRIVER_PARAMS = "?useUnicode=true&characterEncoding=gbk";
    private Connection connection;
    public static Logger logger = Log4jUtil.getLogger(MysqlJobManager.class);

    public Boolean prepare(JobInfo jobInfo) throws Exception {

        String url = jobInfo.getWriteUrl();
        String username = jobInfo.getWriteUsername();
        String password = jobInfo.getWritePassword();
        try {
            Class.forName(DRIVER_CLASSNAME);
        } catch (ClassNotFoundException e) {
            logger.error("数据库连接失败，找不到DRIVER_CLASSNAME: "+DRIVER_CLASSNAME);
            throw new Exception("数据库连接失败，找不到DRIVER_CLASSNAME: "+DRIVER_CLASSNAME);
        }
        try
        {
            this.connection = DriverManager.getConnection(url, username, password);
        } catch (SQLException e2)
        {
        logger.error("数据库连接失败，请检查数据库url、用户名和密码-_-!");
        throw new Exception("数据库连接失败，请检查数据库url、用户名和密码-_-!");
    }
        logger.info("连接Mysql数据库成功！");
        return true;
    }

    public void check(JobInfo jobInfo) throws Exception {
        //查询数据库已有表名
        Set<String> tables = getTables();
        String table = jobInfo.getWriteTable();
        if (!tables.contains(table)){
            logger.info("目标数据库中不存在表："+ table);
            createTable(table, JDBCUtils.strToList(jobInfo.getWriterColumns(),1),
                    JDBCUtils.strToList(jobInfo.getWriterColumnTypes(),0),
                    JDBCUtils.strToList(jobInfo.getWriterColumnSize(),0));
        }
        connection.close();
    }

    //得到写入数据源中所有的表名
    private Set<String> getTables() throws Exception {
        Set<String> tables = new HashSet<String>();
        DatabaseMetaData meta = connection.getMetaData();
        ResultSet rs = meta.getTables(null, "%", null,
                new String[] { "TABLE" });
        while (rs.next()) {
            tables.add(rs.getString("TABLE_NAME"));
        }
        return tables;
    }


    //根据表名得到表的字段和字段类型
    private void getColumns(String tablename) throws Exception {
        String columnName;
        String columnType;
        DatabaseMetaData meta = connection.getMetaData();
        ResultSet colRet = meta.getColumns(null, "%", tablename, "%");
        while(colRet.next()) {
            columnName = colRet.getString("COLUMN_NAME");
            columnType = colRet.getString("TYPE_NAME");
            int datasize = colRet.getInt("COLUMN_SIZE");
            System.out.println(columnName+" "+columnType+" "+datasize);
        }
    }


    //创建数据库表
    private Boolean createTable(String tablename, List<String> cols, List<String> type, List<String> size) throws Exception {
        String sql = "create table " + tablename + " (";
        int len = cols.size();
        for (int i=0; i < len; i++){
            sql += cols.get(i) + " " + type.get(i) + "(" + size.get(i) + "), ";
        }
        sql = sql.substring(0, sql.length()-2) + " )";
        logger.info("sql: " + sql);
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            stmt.execute(sql);
            logger.info("数据表创建成功");
        } catch (SQLException e) {
            throw new Exception(e);
        }
        return true;
    }


    public String getDataXConf(final JobInfo jobInfo) {
        logger.info("生成DataX配置文件");
        //生成mysql的DataX配置文件，配置文件中有列表时需要嵌套生成
        Configuration configuration = Configuration.from("{}");

        //reader配置部分
        DataXReaderConfig dataXReaderConfig = new DataXReaderConfig();
        Class clazz = dataXReaderConfig.getClass();
        Method m = null;
        //根据reader的数据源类别，确定反射调用的方法名
        String methodName = PluginLoader.getDataxReaderName(jobInfo.getReadDataSourceType());
        try {
            m = clazz.getDeclaredMethod(methodName, JobInfo.class);
        } catch (NoSuchMethodException e) {
            logger.error(e.toString());
        }
        Configuration rwConf = null;
        try {
            rwConf = (Configuration) m.invoke(dataXReaderConfig,jobInfo);
        } catch (IllegalAccessException e) {
            logger.error(e.toString());
        } catch (InvocationTargetException e) {
            logger.error(e.toString());
        }
        //writer配置部分
        final Configuration writerConnConf = Configuration.from("{}");
        writerConnConf.set(DataXConfigConstants.WRITER_TABLE, new ArrayList<String>(){{
            add(jobInfo.getWriteTable());
        }});
        writerConnConf.set(DataXConfigConstants.WRITER_URL, jobInfo.getWriteUrl()+DRIVER_PARAMS);

        Configuration writerParamConf = Configuration.from("{}");
        writerParamConf.set(DataXConfigConstants.WRITER_CONN, new ArrayList<Object>(){{
                add(writerConnConf.getInternal());
        }});
        writerParamConf.set(DataXConfigConstants.WRITER_USERNAME, jobInfo.getWriteUsername());
        writerParamConf.set(DataXConfigConstants.WRITER_PASSWORD, jobInfo.getWritePassword());
        writerParamConf.set(DataXConfigConstants.WRITER_MODE, WRITER_MODE);
        writerParamConf.set(DataXConfigConstants.WRITER_COLUMN, JDBCUtils.strToList(jobInfo.getWriterColumns(),1));

        writerParamConf.set(DataXConfigConstants.WRITER_PRESQL, new ArrayList<Object>(){{ add("delete from " + jobInfo.getWriteTable()); }});

        rwConf.set(DataXConfigConstants.WRITER_PARAM, writerParamConf.getInternal());
        rwConf.set(DataXConfigConstants.WRITER_NAME, PluginLoader.getDataxWriterName(jobInfo.getWriteDataSourceType()));

        final Configuration finalRwConf = rwConf;
        configuration.set(DataXConfigConstants.JOB_CONTENT, new ArrayList<Object>(){{
            add(finalRwConf.getInternal());
        }});
        configuration.set(DataXConfigConstants.JOB_CHANNEL, JOB_CHANNEL);

        String str = configuration.beautify();

        return str;
    }

}
