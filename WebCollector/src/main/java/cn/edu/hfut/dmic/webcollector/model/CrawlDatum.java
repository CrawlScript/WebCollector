/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.model;

import org.apache.avro.reflect.Nullable;

/**
 *
 * @author hu
 */
public class CrawlDatum {
    
    public static final int STATUS_DB_UNDEFINED=-1;
    public static final int STATUS_DB_UNFETCHED=1;
    public static final int STATUS_DB_FETCHED=2;
    
    public static final int FETCHTIME_UNDEFINED=1;
    
    @Nullable private String url;
    @Nullable private int status=CrawlDatum.STATUS_DB_UNDEFINED;
    @Nullable private long fetchTime=CrawlDatum.FETCHTIME_UNDEFINED;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public long getFetchTime() {
        return fetchTime;
    }

    public void setFetchTime(long fetchTime) {
        this.fetchTime = fetchTime;
    }

    
    
    
    
}
