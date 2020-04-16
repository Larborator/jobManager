package com.cug.intellM.jobManager.util;

/**
 * Created by zsm on 2018/10/13.
 * DataX需要配置的参数
 */
public interface DataXConfigConstants {

    public static final String JOB_CONTENT = "job.content";
    public static final String JOB_CHANNEL = "job.setting.speed.channel";

    public static final String READER_PARAM = "reader.parameter";
    public static final String READER_NAME = "reader.name";
    public static final String READER_CONN = "connection";
    public static final String READER_URL = "jdbcUrl";
    public static final String READER_USERNAME = "username";
    public static final String READER_PASSWORD = "password";
    public static final String READER_QUERYSQL = "querySql";

    public static final String WRITER_PARAM = "writer.parameter";
    public static final String WRITER_NAME = "writer.name";
    public static final String WRITER_URL = "jdbcUrl";
    public static final String WRITER_CONN = "connection";
    public static final String WRITER_USERNAME = "username";
    public static final String WRITER_PASSWORD = "password";
    public static final String WRITER_TABLE = "table";
    public static final String WRITER_COLUMN = "column";
    public static final String WRITER_MODE = "writeMode";
    public static final String WRITER_PRESQL = "preSql";
    public static final String WRITER_FS = "defaultFS";
    public static final String WRITER_FILENAME = "fileName";
    public static final String WRITER_FILETYPE = "fileType";
    public static final String WRITER_FILEDELIM = "fieldDelimiter";
    public static final String WRITER_PATH = "path";

    public static final String COLUMN_NAME = "name";
    public static final String COLUMN_TYPE = "type";

    public static final String DEFAULT_FS = "hdfs://master:9000";
    public static final String DEFAULT_TYPE = "text";
    public static final String DEFAULT_NAME = "xxxx";
    public static final String DEFAULT_MODE_APPEND = "append";
    public static final String DEFAULT_FILE_DELIM = "\t";
    public static final String DEFAULT_DB = "/user/hive/warehouse/mydb.db/";

}
