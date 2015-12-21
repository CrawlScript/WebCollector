/*
 * Copyright (C) 2015 hu
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
package cn.edu.hfut.dmic.webcollector.lazy;

import cn.edu.hfut.dmic.webcollector.model.Links;
import cn.edu.hfut.dmic.webcollector.net.Proxys;
import cn.edu.hfut.dmic.webcollector.util.Config;
import cn.edu.hfut.dmic.webcollector.util.FileUtils;
import cn.edu.hfut.dmic.webcollector.util.RegexRule;
import java.util.HashMap;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hu
 */
public class LazyConfig {
    
     public static final Logger LOG=LoggerFactory.getLogger(LazyConfig.class);

    protected String taskName = null;
    protected boolean resumable = false;

    protected String mongoIP = "127.0.0.1";
    protected int mongoPort = 27017;

    protected String pageDB = null;
    protected String pageCollection = null;
    protected Links seeds = new Links();
    protected Links forcedSeeds = new Links();
    protected HashMap<String, String> headerMap = new HashMap<String, String>();
    protected RegexRule regexRule = new RegexRule();
    protected int topN = -1;
    protected Proxys proxys = null;
    protected int depth = Integer.MAX_VALUE;
    protected long visitInterval = 0;
    protected long retryInterval = 0;
    protected int threads = 50;
    protected int retry = 3;
    protected int maxReceiveSize=Config.MAX_RECEIVE_SIZE;
    protected int maxRetry=Config.MAX_RETRY;

    public LazyConfig(String confFileName) throws Exception {
        String jsonStr = FileUtils.readFile(confFileName, "utf-8");
        JSONObject confJson = new JSONObject(jsonStr);
        taskName = confJson.getString("task_name");
        LOG.info("set task_name="+taskName);
        resumable = confJson.getBoolean("resumable");
        LOG.info("set resumable="+resumable);
        mongoIP = confJson.getString("mongo_ip");
        mongoPort = confJson.getInt("mongo_port");
        LOG.info("set mongo address="+mongoIP+":"+mongoPort);
        pageDB = confJson.getString("page_db");
        pageCollection = confJson.getString("page_collection");
        LOG.info("webpage will be saved to db:"+pageDB+" collection:"+pageCollection);
        if (confJson.has("seeds")) {
            JSONArray seedsJA = confJson.getJSONArray("seeds");
            for (int i = 0; i < seedsJA.length(); i++) {
                String seed=seedsJA.getString(i);
                seeds.add(seed);
                LOG.info("add seed:"+seed);
            }
        }

        if (confJson.has("forced_seeds")) {
            JSONArray forcedSeedsJA = confJson.getJSONArray("forced_seeds");
            for (int i = 0; i < forcedSeedsJA.length(); i++) {
                String forcedSeed=forcedSeedsJA.getString(i);
                forcedSeeds.add(forcedSeed);
                LOG.info("add forced_seed:"+forcedSeed);
            }
        }

        if (confJson.has("regex_rule")) {
            JSONArray regexRuleJA = confJson.getJSONArray("regex_rule");
            for (int i = 0; i < regexRuleJA.length(); i++) {
                String rule=regexRuleJA.getString(i);
                regexRule.addRule(rule);
                LOG.info("add regex_rule:"+rule);
            }
        }

        if (confJson.has("topN")) {
            topN = confJson.getInt("topN");
            LOG.info("set topN="+topN);
        }

        if (confJson.has("proxys")) {
            JSONArray proxysJA = confJson.getJSONArray("proxys");
            if (proxysJA.length() == 0) {
                proxys = null;
                LOG.info("set proxys=no proxy");
            } else if (proxysJA.length() == 1 && proxysJA.getString(0).equals("direct")) {
                proxys = null;
                LOG.info("set proxys=no proxy");
            } else {
                proxys=new Proxys();
                for (int i = 0; i < proxysJA.length(); i++) {
                    
                    String proxy=proxysJA.getString(i);
                    if(proxy.equals("direct")){
                        proxys.addEmpty();
                    }else{
                        proxys.add(proxy);
                    }
                    LOG.info("add proxy:"+proxy);
                }
            }
        }

        if (confJson.has("depth")) {
            depth = confJson.getInt("depth");
            LOG.info("set depth="+depth);
        }

        if (confJson.has("visit_interval")) {
            visitInterval = confJson.getLong("visit_interval");
            LOG.info("set visit_interval="+visitInterval);
        }
        if (confJson.has("retry_interval")) {
            retryInterval = confJson.getLong("retry_interval");
            LOG.info("set retry_interval="+retryInterval);
        }

        if (confJson.has("threads")) {
            threads = confJson.getInt("threads");
            LOG.info("set threads="+threads);
        }

        if (confJson.has("retry")) {
            retry = confJson.getInt("retry");
            LOG.info("set retry="+retry);
        }
        
          if (confJson.has("max_retry")) {
            maxRetry = confJson.getInt("max_retry");
            LOG.info("set max_retry="+maxRetry);
        }
        
        if(confJson.has("max_receive_size")){
            maxReceiveSize=confJson.getInt("max_receive_size");
            LOG.info("set max_receive_size="+maxReceiveSize);
            
        }

    }

    public String getCrawlPath() {
        return "crawl_" + taskName;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public int getMaxReceiveSize() {
        return maxReceiveSize;
    }

    public void setMaxReceiveSize(int maxReceiveSize) {
        this.maxReceiveSize = maxReceiveSize;
    }

    public int getMaxRetry() {
        return maxRetry;
    }

    public void setMaxRetry(int maxRetry) {
        this.maxRetry = maxRetry;
    }

    

    
    
    public boolean isResumable() {
        return resumable;
    }

    public void setResumable(boolean resumable) {
        this.resumable = resumable;
    }

    public String getMongoIP() {
        return mongoIP;
    }

    public void setMongoIP(String mongoIP) {
        this.mongoIP = mongoIP;
    }

    public int getMongoPort() {
        return mongoPort;
    }

    public void setMongoPort(int mongoPort) {
        this.mongoPort = mongoPort;
    }

    public String getPageDB() {
        return pageDB;
    }

    public void setPageDB(String pageDB) {
        this.pageDB = pageDB;
    }

    public String getPageCollection() {
        return pageCollection;
    }

    public void setPageCollection(String pageCollection) {
        this.pageCollection = pageCollection;
    }

    public Links getSeeds() {
        return seeds;
    }

    public void setSeeds(Links seeds) {
        this.seeds = seeds;
    }

    public Links getForcedSeeds() {
        return forcedSeeds;
    }

    public void setForcedSeeds(Links forcedSeeds) {
        this.forcedSeeds = forcedSeeds;
    }

    public HashMap<String, String> getHeaderMap() {
        return headerMap;
    }

    public void setHeaderMap(HashMap<String, String> headerMap) {
        this.headerMap = headerMap;
    }

    public RegexRule getRegexRule() {
        return regexRule;
    }

    public void setRegexRule(RegexRule regexRule) {
        this.regexRule = regexRule;
    }

    public int getTopN() {
        return topN;
    }

    public void setTopN(int topN) {
        this.topN = topN;
    }

    public Proxys getProxys() {
        return proxys;
    }

    public void setProxys(Proxys proxys) {
        this.proxys = proxys;
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

   

    public long getVisitInterval() {
        return visitInterval;
    }

    public void setVisitInterval(long visitInterval) {
        this.visitInterval = visitInterval;
    }

    public long getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(long retryInterval) {
        this.retryInterval = retryInterval;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public int getRetry() {
        return retry;
    }

    public void setRetry(int retry) {
        this.retry = retry;
    }

}
