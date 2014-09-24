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

package cn.edu.hfut.dmic.webcollector.crawler;

import cn.edu.hfut.dmic.webcollector.fetcher.BasicFetcher;
import cn.edu.hfut.dmic.webcollector.fetcher.Fetcher;
import cn.edu.hfut.dmic.webcollector.generator.DbUpdater;
import cn.edu.hfut.dmic.webcollector.generator.Generator;
import cn.edu.hfut.dmic.webcollector.generator.Injector;
import cn.edu.hfut.dmic.webcollector.handler.Handler;
import cn.edu.hfut.dmic.webcollector.handler.Message;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.util.LogUtils;
import java.util.ArrayList;

/**
 *
 * @author hu
 */
public abstract class BasicCrawler implements Crawler{
    private int status;
    public final static int RUNNING=1;
    public final static int STOPED=2;
    
    private boolean resumable=false;
    private int threads=10;

    public boolean isResumable() {
        return resumable;
    }

    public void setResumable(boolean resumable) {
        this.resumable = resumable;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public ArrayList<String> getRegexs() {
        return regexs;
    }

    public void setRegexs(ArrayList<String> regexs) {
        this.regexs = regexs;
    }

    public ArrayList<String> getSeeds() {
        return seeds;
    }

    public void setSeeds(ArrayList<String> seeds) {
        this.seeds = seeds;
    }

    
    private ArrayList<String> regexs = new ArrayList<String>();
    private ArrayList<String> seeds = new ArrayList<String>();
    

    
    private Fetcher fetcher;
    
    @Override
    public void start(int depth) throws Exception {
        
        if (!resumable) {
            DbUpdater clearDbUpdater=createDbUpdater();
            clearDbUpdater.clearHistory();
            
            if (seeds.isEmpty()) {
                LogUtils.getLogger().info("error:Please add at least one seed");
                return;
            }
           
        }
        if (regexs.isEmpty()) {
                LogUtils.getLogger().info("error:Please add at least one regex rule");
                return;
        }
        inject();
        
        
        status=RUNNING;
        for (int i = 0; i < depth; i++) {
           if(status==STOPED){
               break;
            }
            LogUtils.getLogger().info("starting depth "+(i+1));
            Generator generator=createGenerator();
            fetcher=createFetcher();
            DbUpdater dbUpdater=createDbUpdater();            
            if(dbUpdater!=null)
                fetcher.setDbUpdater(createDbUpdater());
            fetcher.fetchAll(generator);
        }
    }

    @Override
    public void stop() throws Exception {
         fetcher.stop();
       status=STOPED;
    }

   
    
    public void inject() throws Exception{
        Injector injector = createInjector();
        injector.inject(seeds,resumable);
    }
    
    
    public  void visit(Page page){
        
    }
    
    public void failed(Page page){
        
    }
    
    /**
     * 生成处理抓取消息的Handler，默认通过BreadthCrawler的visit方法来处理成功抓取的页面，
     * 通过failed方法来处理失败抓取的页面
     * @return 处理抓取消息的Handler
     */
    public Handler createFetcherHandler(){
        Handler fetchHandler = new Handler() {
            @Override
            public void handleMessage(Message msg) {
                Page page = (Page) msg.obj;
                switch(msg.what){
                    case BasicFetcher.FETCH_SUCCESS:
                        
                        visit(page);
                        break;
                    case BasicFetcher.FETCH_FAILED:
                        failed(page);
                        break;
                    default:
                        break;
                       
                }
            }
        };
        return fetchHandler;
    }
    
    /**
     * 添加一个种子url
     * @param seed 种子url
     */
    public void addSeed(String seed) {
        seeds.add(seed);
    }
    
    /**
     * 添加一个正则过滤规则
     * @param regex 正则过滤规则
     */
    public void addRegex(String regex) {
        regexs.add(regex);
    }
    
}
