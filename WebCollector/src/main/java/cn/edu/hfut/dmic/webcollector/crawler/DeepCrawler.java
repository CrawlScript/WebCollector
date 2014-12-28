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

import cn.edu.hfut.dmic.webcollector.fetcher.Visitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hu
 */
public abstract class DeepCrawler extends Crawler implements Visitor {

    public static final Logger LOG = LoggerFactory.getLogger(DeepCrawler.class);

    public DeepCrawler(String crawlPath) {
        super(crawlPath);
    }


    public static void main(String[] args) throws Exception {
        //DeepCrawler crawler = new DeepCrawler("/home/hu/data/bdbcrawl");
       // crawler.addSeed("http://www.xinhuanet.com/");
        //crawler.addSeed("http://www.sina.com");
       // crawler.start(3);
    }

    @Override
    public Visitor createVisitor(String url, String contentType) {
        return this;
    }

}
