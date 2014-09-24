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

package cn.edu.hfut.dmic.webcollector.plugin.dbupdater;

import cn.edu.hfut.dmic.webcollector.generator.DbReader;
import cn.edu.hfut.dmic.webcollector.generator.DbWriter;
import cn.edu.hfut.dmic.webcollector.generator.FSDbUpdater;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.Link;
import cn.edu.hfut.dmic.webcollector.parser.ParseData;
import cn.edu.hfut.dmic.webcollector.util.Config;
import cn.edu.hfut.dmic.webcollector.util.LogUtils;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import redis.clients.jedis.Jedis;

/**
 * 使用redis来进行海量URL去重的插件
 * 
 * 如果使用该插件，请安装redis数据库并开启
 * @author hu
 */
public class RedisMergeFSDbupdater  extends FSDbUpdater {

        private String ip = "127.0.0.1";
        private int port = 6379;

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

        Jedis jedis;

        public RedisMergeFSDbupdater(String crawlPath, String segmentName) {
            super(crawlPath, segmentName);
            jedis = new Jedis("127.0.0.1", 6379);
            updateCount=new AtomicInteger(0);
            writeCount=new AtomicInteger(0);
        }

        protected void addToRedis(CrawlDatum datum) {
            try {
                String key = datum.getUrl();
                if (key.trim().isEmpty()) {
                    return;
                }
                //LogUtils.getLogger().info("redis :"+key+" -totalCount="+updateCount.incrementAndGet());
                if(updateCount.incrementAndGet()%5000==0){
                    LogUtils.getLogger().info(updateCount.get()+" crawlDatum add to redis");
                }

                String value = jedis.hget(getCrawlPath(),key);
                if (value == null) {
                    update(datum);
                    return;
                }

                int status = Integer.valueOf(value.charAt(0) + "");
                long fetchTime = Long.valueOf(value.substring(1));

                if (datum.getStatus() == CrawlDatum.STATUS_DB_FETCHED && datum.getStatus() == status) {
                    if (datum.getFetchTime() > fetchTime) {
                        update(datum);
                    }
                } else if (datum.getStatus() > status) {
                    update(datum);
                }
            } catch (Exception ex) {
                LogUtils.getLogger().info("Exception", ex);
            }

        }

        AtomicInteger updateCount;
        AtomicInteger writeCount;
        protected void update(CrawlDatum datum) {
            jedis.hset(getCrawlPath(),datum.getUrl(), datum.getStatus() + "" + datum.getFetchTime());
           
        }

        @Override
        public void merge() throws IOException {      
            backup();
            LogUtils.getLogger().info("merge "+getSegmentPath());
            jedis.del(getCrawlPath());
            LogUtils.getLogger().info("Delete all data in redis");
            File crawldbFile = new File(getCrawlPath(), Config.old_info_path);
            File fetchFile = new File(getSegmentPath(), "fetch/info.avro");
            File parseFile = new File(getSegmentPath(), "parse_data/info.avro");

            DbReader<CrawlDatum> reader = new DbReader<CrawlDatum>(CrawlDatum.class, crawldbFile);
            while (reader.hasNext()) {
                CrawlDatum datum = reader.readNext();
                addToRedis(datum);
            }

            if (fetchFile.exists()) {
                reader = new DbReader<CrawlDatum>(CrawlDatum.class, fetchFile);
                while (reader.hasNext()) {
                    CrawlDatum datum = reader.readNext();
                    addToRedis(datum);
                }
            }

            reader.close();
            if (parseFile.exists()) {
                DbReader<ParseData> parseReader = new DbReader<ParseData>(ParseData.class, parseFile);
                while (parseReader.hasNext()) {
                    ParseData parseData = parseReader.readNext();
                    if (parseData.getLinks() == null) {
                        continue;
                    }
                    for (Link link : parseData.getLinks()) {
                        CrawlDatum datum = new CrawlDatum();
                        datum.setUrl(link.getUrl());
                        datum.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
                        datum.setFetchTime(CrawlDatum.FETCHTIME_UNDEFINED);
                        addToRedis(datum);
                    }
                }
                parseReader.close();

            }

            DbWriter<CrawlDatum> writer = new DbWriter<CrawlDatum>(CrawlDatum.class, new File(getCrawlPath(), Config.current_info_path));

            Set set = jedis.hkeys(getCrawlPath());
            

            Iterator ite = set.iterator();
            while (ite.hasNext()) {
                String key = ite.next().toString();
                String value = jedis.hget(getCrawlPath(),key);
                int status = Integer.valueOf(value.charAt(0) + "");
                long fetchTime = Long.valueOf(value.substring(1));

                CrawlDatum datum = new CrawlDatum();
                datum.setUrl(key);
                datum.setStatus(status);
                datum.setFetchTime(fetchTime);
                writer.write(datum);
                if(writeCount.incrementAndGet()%5000==0){
                    LogUtils.getLogger().info(writeCount.get()+" crawlDatum write to crawldb");
                }
                //LogUtils.getLogger().info("write "+datum.getUrl());

            }
            writer.close();
        }
        
        public static void main(String[] args){

        }

}

    

