package com.cug.intellM.jobManager.plugin;

import com.cug.intellM.jobManager.core.JobInfo;

/**
 * Created by zsm on 2018/9/30.
 */
public abstract class JobManager extends AbstractPlugin{

    public abstract Boolean prepare(JobInfo jobInfo) throws Exception;

    public abstract void check(JobInfo jobInfo) throws Exception;

    public abstract String getDataXConf(JobInfo jobInfo);

}
