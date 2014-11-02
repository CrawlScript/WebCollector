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


import cn.edu.hfut.dmic.webcollector.crawler.CommonCrawler;
import cn.edu.hfut.dmic.webcollector.generator.DbUpdater;
import cn.edu.hfut.dmic.webcollector.generator.Generator;
import cn.edu.hfut.dmic.webcollector.generator.Injector;
import cn.edu.hfut.dmic.webcollector.generator.filter.IntervalFilter;
import cn.edu.hfut.dmic.webcollector.generator.filter.URLRegexFilter;
import cn.edu.hfut.dmic.webcollector.model.Page;


/**
 * 基于Redis的广度遍历器
 * @author hu
 */
public class RedisCrawler extends CommonCrawler{

    private String tableName;
    private String ip;
    private int port;
    
    /**
     * 构建一个基于redis的广度遍历器
     * @param tableName 任务名
     * @param ip redis的ip
     * @param port redis的端口
     */
    public RedisCrawler(String tableName, String ip, int port) {
        this.tableName = tableName;
        this.ip = ip;
        this.port = port;
    }
    
    
    @Override
    public Generator createGenerator() {
        Generator generator=new RedisGenerator(tableName, ip, port);
        return new URLRegexFilter(new IntervalFilter(generator),getRegexRule());
    }

    @Override
    public Injector createInjector() {
        return new RedisInjector(tableName, ip, port);
    }

    @Override
    public DbUpdater createDbUpdater() {
        return new RedisDbUpdater(tableName, ip, port);
    }
    
    /**
     * 返回redis的IP
     * @return redis的IP
     */
    public String getIp() {
        return ip;
    }

    /**
     * 设置redis的IP
     * @param ip redis的IP
     */
    public void setIp(String ip) {
        this.ip = ip;
    }

    /**
     * 返回redis的端口
     * @return redis的端口
     */
    public int getPort() {
        return port;
    }

    /**
     * 设置redis的端口
     * @param port redis的端口
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * 返回任务名
     * @return 任务名
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 设置任务名
     * @param tableName 任务名
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    /*
    public static void main(String[] args) throws Exception{
        RedisCrawler crawler=new RedisCrawler("mytest", "127.0.0.1", 6379){
            @Override
            public void visit(Page page){
                System.out.println(page.getDoc().title());
            }
        };
        crawler.addSeed("http://news.hfut.edu.cn/");
        crawler.addRegex("http://news.hfut.edu.cn/.*");
        crawler.addRegex("-.*#.*");
        crawler.addRegex("-.*png.*");
        crawler.addRegex("-.*jpg.*");
        crawler.addRegex("-.*gif.*");
        crawler.addRegex("-.*js.*");
        crawler.addRegex("-.*css.*");
        
        crawler.setThreads(30);
        crawler.setResumable(false);
        crawler.start(5);
    }
    
   */
    
    
}
