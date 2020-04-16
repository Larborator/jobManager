package com.cug.intellM.jobManager.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by zsm on 2018/10/16.
 */
public class JDBCUtils {
    public static Logger logger = Log4jUtil.getLogger(JDBCUtils.class);

    //获取数据库连接
    public static Connection getDBConnection(String className, String url, String username, String password){
        try {
            Class.forName(className);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    //根据查询表名、字段名以及关联条件拼接sql,tableColumns为json格式字符串
    public static String getQuerySqlByParams(String tableColumns, String joinCondition, String dataClean){
        Configuration cfg = Configuration.from(tableColumns);
        JSONObject jsonMap = (JSONObject) cfg.get("");
        Set<String> tableNames = jsonMap.keySet();
        String sql = "SELECT ";
        for (String table : tableNames){
            String str = "";
            JSONArray columns = (JSONArray) jsonMap.get(table);
            for (Object col : columns){
                str += table + '.' + col + ",";
            }
            sql += str;
        }
        //去掉字符串最后的,
        sql = sql.substring(0, sql.length()-1) + " FROM ";
        //如果没有关联条件，为单表查询
        if(joinCondition.equals("")){
            for (String table : tableNames){
                sql += table;
            }
        }else {
            sql += joinCondition;
        }
        //添加dataClean条件
        if (!dataClean.equals("")){
            sql += " WHERE " + dataClean;
        }
        return sql;
    }

    /*
    根据查询表名、字段名以及关联条件拼接sql,tableColumns为json格式字符串
    Tables = "PLATFORM_USER_TBL;ETL_STRATEGY_TBL";
    Columns = "User_ID,Name;ExtratDetails";
     */
    public static String getQuerySqlByParams2(String tables, String columns, String joinCondition, String dataClean){
        String[] ts = tables.split(";");
        String[] cs = columns.split(";");
        int len = ts.length;
        String sql = "SELECT ";
        for (int i=0; i < len; i++){
            String str = "";
            String t = ts[i];
            String[] c = cs[i].split(",");
            for (String cc : c){
                str += t + '.' + cc + ",";
            }
            sql += str;
        }

        //去掉字符串最后的,
        sql = sql.substring(0, sql.length()-1) + " FROM ";
        //如果没有关联条件，为单表查询
        if(joinCondition.equals("")){
            for (String table : ts){
                sql += table;
            }
        }else {
            sql += joinCondition;
        }
        //添加dataClean条件
        if (!dataClean.equals("")){
            sql += " WHERE " + dataClean;
        }
        return sql;
    }

    /**
     * 根据查询表名、字段名以及关联条件拼接sql,tableColumns为json格式字符串
     Tables = "PLATFORM_USER_TBL;ETL_STRATEGY_TBL";
     Columns = "PLATFORM_USER_TBL.User_ID,PLATFORM_USER_TBL.Name,ETL_STRATEGY_TBL.ExtratDetails";
     * @param tables
     * @param columns
     * @param joinCondition
     * @param dataClean
     * @return
     */
    public static String getQuerySqlByParams3(String tables, String columns, String joinCondition, String dataClean){
        String sql = "SELECT " ;

        return sql;
    }

    //根据转换条件拼接数据更新sql
//    public static String getPostSqlByParams(String dataTransform){
//
//    }

    public static List<String> strToList(String columns,int type){
        if(columns == null || "".equals(columns)){
            logger.error("写入数据源表的字段不能为空");
            return null;
        }else{
            ArrayList<String> ls = new ArrayList<String>();
            String[] strs = columns.split(",");
            for(String str : strs){
                if(type == 1)
                    str = "`" + str + "`";
                ls.add(str);
            }
            return ls;
        }
    }

}
