/*
 * Copyright (C) 2014 hu
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package cn.edu.hfut.dmic.webcollector.plugin.redis;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.Link;
import cn.edu.hfut.dmic.webcollector.parser.ParseData;
import java.util.Set;
import redis.clients.jedis.Jedis;

/**
 *
 * @author hu
 */
public class RedisHelper {

    public Jedis jedis;
    public static int REDIS_TIME_OUT=1000*60*3;

    public Jedis getJedis() {
        return jedis;
    }

    public void setJedis(Jedis jedis) {
        this.jedis = jedis;
    }
    private String ip;
    private int port;
    private String tableName;
    
    public static final String suffix_crawldb="_crawldb";
    public static final String suffix_fetch="_fetch";
    public static final String suffix_parse="_parselink";
    public static final String suffix_lock="_lock";

    public RedisHelper(String tableName,String ip, int port) {
        this.tableName=tableName;
        this.ip = ip;
        this.port = port;
        jedis=new Jedis(ip, port,RedisHelper.REDIS_TIME_OUT);
    }
    
    

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public synchronized Set getCrawlDb(){
        return jedis.hkeys(tableName+suffix_crawldb);
    }
    
   
    
    public CrawlDatum getCrawlDatumByKey(String key){
        String value = jedis.hget(tableName+suffix_crawldb,key);
        if(value==null)
            return null;
        int status = Integer.valueOf(value.charAt(0) + "");
        long fetchTime = Long.valueOf(value.substring(1));

        CrawlDatum datum = new CrawlDatum();
        datum.setUrl(key);
        datum.setStatus(status);
        datum.setFetchTime(fetchTime);
        return datum;
    }
    
    public synchronized void deleteTable() {
        jedis.del(tableName+suffix_crawldb);
        jedis.del(tableName+suffix_fetch);
        jedis.del(tableName+suffix_parse);
        jedis.del(tableName+suffix_lock);
    }

    public synchronized void inject(String url, boolean append) {
        if(!append){
            jedis.del(tableName+suffix_crawldb);
        }
        jedis.hset(tableName+suffix_crawldb, url, CrawlDatum.STATUS_DB_UNFETCHED + "" + CrawlDatum.FETCHTIME_UNDEFINED);
    }

    
    public synchronized void lock(){
        jedis.set(tableName+suffix_lock, "1");
    }
    public synchronized void unlock(){
        jedis.set(tableName+suffix_lock, "0");
    }
    
    public synchronized boolean isLocked(){
        String lock=jedis.get(tableName+suffix_lock);
        if(lock==null)
            return false;
        if(lock.equals("0"))
            return false;
        else
            return true;
    }
    

    
    public synchronized void addFetch(CrawlDatum datum) {
        jedis.hset(tableName+suffix_fetch, datum.getUrl(), datum.getStatus() + "" + datum.getFetchTime());
    }

    public synchronized void addParse(ParseData parseData) {
        if (parseData == null || parseData.getLinks() == null) {
            return;
        }
        for (Link link : parseData.getLinks()) {
            if (jedis.hget(tableName+suffix_parse, link.getUrl()) == null) {
                CrawlDatum datum = new CrawlDatum();
                datum.setUrl(link.getUrl());
                datum.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
                datum.setFetchTime(CrawlDatum.FETCHTIME_UNDEFINED);
                jedis.hset(tableName+suffix_parse, datum.getUrl(), datum.getStatus() + "" + datum.getFetchTime());

            }
        }
    }

}
