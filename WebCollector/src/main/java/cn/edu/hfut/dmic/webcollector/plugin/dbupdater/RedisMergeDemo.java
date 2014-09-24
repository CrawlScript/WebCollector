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

import cn.edu.hfut.dmic.webcollector.model.Page;

/**
 *
 * @author hu
 */
public class RedisMergeDemo extends RedisMergeBreadthCrawler{

    @Override
    protected void visit(Page page) {
        System.out.println(page.getDoc().title());
    }

    
    
    public RedisMergeDemo(String redisIP, int redisPort) {
        super(redisIP, redisPort);
    }
    
    
    public static void main(String[] args) throws Exception{
        RedisMergeDemo crawler=new RedisMergeDemo("127.0.0.1", 6379);
        String crawlPath = "/home/hu/data/crawl_hfut1";
        crawler.addSeed("http://news.hfut.edu.cn/");
        crawler.addRegex("http://news.hfut.edu.cn/.*");
        crawler.addRegex("-.*#.*");
        crawler.addRegex("-.*png.*");
        crawler.addRegex("-.*jpg.*");
        crawler.addRegex("-.*gif.*");
        crawler.addRegex("-.*js.*");
        crawler.addRegex("-.*css.*");
        
       
        crawler.setCrawlPath(crawlPath);
        crawler.setThreads(60);
        crawler.setResumable(false);      
        crawler.start(4);
    }
    
}
