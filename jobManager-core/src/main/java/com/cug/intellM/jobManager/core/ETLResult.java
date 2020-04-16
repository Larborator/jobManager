package com.cug.intellM.jobManager.core;

import java.util.Date;

/**
 * Created by zsm on 2018/11/9.
 */
public class ETLResult {

    public static final int LOG_SIZE = 20;
    //etl策略id
    private int id;
    //etl执行结果状态表，0失败，1成功
    private Integer[] labelArray = new Integer[LOG_SIZE];
    //etl执行时间列表
    private  Long[] executeTimeArray = new Long[LOG_SIZE];
    //执行日志列表
        private String[] logsArray = new String[LOG_SIZE];
    //当前更新到第index项
    private int index = 0;


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Integer[] getLabelArray() {
        return labelArray;
    }

    public void setLabelArray(Integer[] labelArray) {
        this.labelArray = labelArray;
    }

    public String[] getLogsArray() {
        return logsArray;
    }

    public void setLogsArray(String[] logsArray) {
        this.logsArray = logsArray;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }


    public Long[] getExecuteTimeArray() {
        return executeTimeArray;
    }

    public void setExecuteTimeArray(Long[] executeTimeArray) {
        this.executeTimeArray = executeTimeArray;
    }
}
