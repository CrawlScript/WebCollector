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

import cn.edu.hfut.dmic.webcollector.crawler.*;
import cn.edu.hfut.dmic.webcollector.fetcher.FSFetcher;
import cn.edu.hfut.dmic.webcollector.fetcher.FSSegmentWriter;
import cn.edu.hfut.dmic.webcollector.fetcher.Fetcher;
import cn.edu.hfut.dmic.webcollector.generator.DbUpdater;
import cn.edu.hfut.dmic.webcollector.generator.FSDbUpdater;
import cn.edu.hfut.dmic.webcollector.plugin.dbupdater.RedisMergeFSDbupdater;
import cn.edu.hfut.dmic.webcollector.util.CommonConnectionConfig;

/**
 *
 * @author hu
 */
public class RedisMergeBreadthCrawler extends BreadthCrawler {

    private String redisIP;
    private int redisPort;

    public RedisMergeBreadthCrawler(String redisIP, int redisPort) {
        this.redisIP = redisIP;
        this.redisPort = redisPort;
    }

    public static class RedisMergeFetcher extends FSFetcher {

        public RedisMergeFetcher(String crawlPath,String redisIP, int redisPort) {
            super(crawlPath);
            this.redisIP = redisIP;
            this.redisPort = redisPort;
        }

       
        private String redisIP;
        private int redisPort;

        @Override
        protected DbUpdater createDbUpdater() {
            RedisMergeFSDbupdater updater = new RedisMergeFSDbupdater(getCrawlPath(), getSegmentName());
            updater.setIp(redisIP);
            updater.setPort(redisPort);
            updater.setSegmentWriter(new FSSegmentWriter(getCrawlPath(), getSegmentName()));
            return updater;
        }

        @Override
        protected DbUpdater createRecoverDbUpdater() {
            RedisMergeFSDbupdater updater = new RedisMergeFSDbupdater(getCrawlPath(), getLastSegmentName());
            updater.setIp(redisIP);
            updater.setPort(redisPort);
            return updater;
        }

    }

    @Override
    protected Fetcher createFecther() {

        RedisMergeFetcher fetcher = new RedisMergeFetcher(getCrawlPath(),redisIP,redisPort);
        fetcher.setProxy(getProxy());
        fetcher.setIsContentStored(getIsContentStored());
        fetcher.setHandler(createFetcherHandler());
        setConconfig(new CommonConnectionConfig(getUseragent(), getCookie()));
        fetcher.setThreads(getThreads());
        fetcher.setConconfig(getConconfig());
        return fetcher;
    }
    
    
    

}
