/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.model;

import org.apache.avro.reflect.Nullable;

/**
 * 存储爬取任务的类，是WebCollector的核心类，记录了一个url的爬取信息，同样也
 * 可以作为一个爬取任务
 * @author hu
 */
public class CrawlDatum {
    /**
     * 爬取状态常量-未定义
     */
    public static final int STATUS_DB_UNDEFINED=-1;
    /**
     * 爬取状态常量-未爬取
     */
    public static final int STATUS_DB_UNFETCHED=1;
    /**
     * 爬取状态常量-已爬取
     */
    public static final int STATUS_DB_FETCHED=2;
    
    
    /**
     * 爬取时间常量-未定义
     */
    public static final int FETCHTIME_UNDEFINED=1;
    
    
    
    @Nullable private String url;
    @Nullable private int status=CrawlDatum.STATUS_DB_UNDEFINED;
    @Nullable private long fetchTime=CrawlDatum.FETCHTIME_UNDEFINED;

    /**
     *  获取爬取任务的url
     * @return 爬取任务的url
     */
    public String getUrl() {
        return url;
    }

    /**
     * 设置爬取任务的url
     * @param url 爬取任务的url
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * 获取爬取任务的状态
     * @return 爬取任务的状态
     */
    public int getStatus() {
        return status;
    }

    /**
     * 设置爬取任务的状态
     * @param status 爬取任务的状态
     */
    public void setStatus(int status) {
        this.status = status;
    }

    /**
     * 获取爬取时间
     * @return 爬取时间
     */
    public long getFetchTime() {
        return fetchTime;
    }

    /**
     * 设置爬取时间
     * @param fetchTime 爬取时间
     */
    public void setFetchTime(long fetchTime) {
        this.fetchTime = fetchTime;
    }

    
    
    
    
}
