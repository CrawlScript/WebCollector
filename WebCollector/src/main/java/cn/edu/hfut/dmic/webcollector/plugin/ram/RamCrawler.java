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
package cn.edu.hfut.dmic.webcollector.plugin.ram;

import cn.edu.hfut.dmic.webcollector.crawler.BasicCrawler;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Links;
import cn.edu.hfut.dmic.webcollector.model.Page;

/**
 * 基于内存的Crawler插件，适合一次性爬取，并不具有断点爬取功能
 * 长期任务请使用BreadthCrawler
 * 
 * @author hu
 */
public abstract class RamCrawler extends BasicCrawler {
    
    public RamCrawler(){
        this(true);
    }

    public RamCrawler(boolean autoParse) {
        super(autoParse);
        RamDB ramDB = new RamDB();
        this.dbManager = new RamDBManager(ramDB);
        this.generator = new RamGenerator(ramDB);
    }
    
    public void start() throws Exception{
        start(Integer.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {
        RamCrawler crawler = new RamCrawler(true) {
            @Override
            public void visit(Page page, CrawlDatums next) {

                if (page.getMetaData("type") != null) {
                    Links links = page.getLinks("div.infos.media-body a");
                    CrawlDatums datums=new CrawlDatums(links)
                            .putMetaData("refer", page.getUrl());
                    next.add(datums);
                } else {
                    System.out.println(page.getUrl()+" refer:"+page.getMetaData("refer"));
                    System.out.println(page.getDoc().title());
                }
            }
        };
        for (int i = 0; i <= 2; i++) {
            String url = "https://ruby-china.org/topics?page=" + (i + 1);
            CrawlDatum seed = new CrawlDatum(url)
                    .putMetaData("type", "nav");
            crawler.addSeed(seed);
        }
        crawler.setRetryInterval(3000);
        crawler.start(3);

    }

}
