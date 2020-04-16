package com.cug.intellM.jobManager.core;

/**
 * Created by zsm on 2018/10/11.
 * ETL任务信息类，数据从消息队列中获取
 */
public class JobInfo {
    //任务id
    private String taskId;
    //读取数据源的相关信息
    private String readDataSourceType;
    private String readUrl;
    private String readUsername;
    private String readPassword;
    //写入数据源的相关信息
    private String writeDataSourceType;
    private String writeUrl;
    private String writeUsername;
    private String writePassword;

    //数据表单和表单对应的字段.json格式
    private String readTableColumns;
    private String readTables;
    private String readColumns;
    //需要写入的数据表单和字段,目标表单为单表，字段和readTableColumns中字段总数相同
    private String writeTable;
    private String writerColumns;
    //数据表字段对应的数据类型
    private String writerColumnTypes;
    //数据表字段长度
    private String writerColumnSize;
    //多表关联时的关联条件，用户手写关联条件
    private String joinCondition;

    //数据清洗，sql where后面的条件表达式
    private String dataClean;

    //数据转换，sql update核心语句，UPDATE table1 a,table2 b set a.col = 1, b.col = 2 where a.col = 0
    private String dataTransform;

    private String readSql;

    public String getReadDataSourceType() {
        return readDataSourceType;
    }

    public void setReadDataSourceType(String readDataSourceType) {
        this.readDataSourceType = readDataSourceType;
    }

    public String getReadUrl() {
        return readUrl;
    }

    public void setReadUrl(String readUrl) {
        this.readUrl = readUrl;
    }



    public String getReadPassword() {
        return readPassword;
    }

    public void setReadPassword(String readPassword) {
        this.readPassword = readPassword;
    }

    public String getWriteDataSourceType() {
        return writeDataSourceType;
    }

    public void setWriteDataSourceType(String writeDataSourceType) {
        this.writeDataSourceType = writeDataSourceType;
    }

    public String getWriteUrl() {
        return writeUrl;
    }

    public void setWriteUrl(String writeUrl) {
        this.writeUrl = writeUrl;
    }

    public String getWriteUsername() {
        return writeUsername;
    }

    public void setWriteUsername(String writeUsername) {
        this.writeUsername = writeUsername;
    }

    public String getWritePassword() {
        return writePassword;
    }

    public void setWritePassword(String writePassword) {
        this.writePassword = writePassword;
    }

    public String getReadUsername() {
        return readUsername;
    }

    public void setReadUsername(String readUsername) {
        this.readUsername = readUsername;
    }

    public String getDataClean() {
        return dataClean;
    }

    public void setDataClean(String dataClean) {
        this.dataClean = dataClean;
    }

    public String getDataTransform() {
        return dataTransform;
    }

    public void setDataTransform(String dataTransform) {
        this.dataTransform = dataTransform;
    }

    public String getReadTableColumns() {
        return readTableColumns;
    }

    public void setReadTableColumns(String readTableColumns) {
        this.readTableColumns = readTableColumns;
    }

    public String getJoinCondition() {
        return joinCondition;
    }

    public void setJoinCondition(String joinCondition) {
        this.joinCondition = joinCondition;
    }

    public String getWriteTable() {
        return writeTable;
    }

    public void setWriteTable(String writeTable) {
        this.writeTable = writeTable;
    }

    public String getWriterColumns() {
        return writerColumns;
    }

    public void setWriterColumns(String writerColumns) {
        this.writerColumns = writerColumns;
    }

    public String getWriterColumnTypes() {
        return writerColumnTypes;
    }

    public void setWriterColumnTypes(String writerColumnTypes) {
        this.writerColumnTypes = writerColumnTypes;
    }

    public String getWriterColumnSize() {
        return writerColumnSize;
    }

    public void setWriterColumnSize(String writerColumnSize) {
        this.writerColumnSize = writerColumnSize;
    }

    public String getReadTables() {
        return readTables;
    }

    public void setReadTables(String readTables) {
        this.readTables = readTables;
    }

    public String getReadColumns() {
        return readColumns;
    }

    public void setReadColumns(String readColumns) {
        this.readColumns = readColumns;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getReadSql() {
        return readSql;
    }

    public void setReadSql(String readSql) {
        this.readSql = readSql;
    }
}
