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

import cn.edu.hfut.dmic.webcollector.fetcher.SegmentWriter;
import cn.edu.hfut.dmic.webcollector.generator.DbUpdater;
import cn.edu.hfut.dmic.webcollector.util.LogUtils;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author hu
 */
public class RedisDbUpdater implements DbUpdater{
    
    private String tableName;
    private RedisHelper redisHelper;
    
   
    
    
    private SegmentWriter segmentWriter;
    public RedisDbUpdater(String tableName, String redisIP, int redisPort) {
        this.tableName=tableName;
        redisHelper=new RedisHelper(tableName, redisIP, redisPort);
    }

    @Override
    public void lock() throws Exception {
        redisHelper.lock();
    }

    @Override
    public boolean isLocked() throws Exception {
        return redisHelper.isLocked();
    }

    @Override
    public void unlock() throws IOException {
        redisHelper.unlock();
    }

   

   
    @Override
    public void merge() throws Exception {
        LogUtils.getLogger().info("merge "+tableName);
        AtomicInteger mergeCount=new AtomicInteger(0);
        String crawldb=tableName+RedisHelper.suffix_crawldb;
        String fetch=tableName+RedisHelper.suffix_fetch;
        String parse=tableName+RedisHelper.suffix_parse;
        Set fetchSet=redisHelper.jedis.hkeys(fetch);
        Iterator fetchIte=fetchSet.iterator();
        while(fetchIte.hasNext()){
            String url=fetchIte.next().toString();
            String value=redisHelper.jedis.hget(fetch,url);
            redisHelper.jedis.hset(crawldb, url, value);
            if(mergeCount.incrementAndGet()%10000==0){
                LogUtils.getLogger().info(mergeCount.get()+"crawldatums merged from fetch");
            }
        }
        
        redisHelper.jedis.del(fetch);
        
        Set parseSet=redisHelper.jedis.hkeys(parse);
        Iterator parseIte=parseSet.iterator();
        while(parseIte.hasNext()){
            String url=parseIte.next().toString();
            if(redisHelper.jedis.hget(crawldb, url)==null){
                String value=redisHelper.jedis.hget(parse, url);
                redisHelper.jedis.hset(crawldb, url, value);
            }
            if(mergeCount.incrementAndGet()%10000==0){
                LogUtils.getLogger().info(mergeCount.get()+"crawldatums merged from parse");
            }        
        }
        redisHelper.jedis.del(parse);
    }

    @Override
    public SegmentWriter getSegmentWriter() {
        return segmentWriter;
    }

    

    @Override
    public void initSegmentWriter() throws Exception {
        segmentWriter=new RedisSegmentWriter(redisHelper);
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public void clearHistory() {
        redisHelper.deleteTable();
    }
    
    
    
}
