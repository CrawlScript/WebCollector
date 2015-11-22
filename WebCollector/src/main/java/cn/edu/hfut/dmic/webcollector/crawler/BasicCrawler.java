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
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Links;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.net.HttpRequest;
import cn.edu.hfut.dmic.webcollector.net.HttpResponse;
import cn.edu.hfut.dmic.webcollector.net.Requester;
import cn.edu.hfut.dmic.webcollector.util.RegexRule;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hu
 */
public abstract class BasicCrawler extends Crawler  implements Visitor,Requester{

    public static final Logger LOG = LoggerFactory.getLogger(BasicCrawler.class);
    
       /**
     * URL正则约束
     */
    protected RegexRule regexRule = new RegexRule();
    
     /**
     * 是否自动抽取符合正则的链接并加入后续任务
     */
    protected boolean autoParse=true;

    public BasicCrawler(boolean autoParse) {
        this.visitor=this;
        this.requester=this;
        this.autoParse=autoParse;
    }

   

    @Override
    public void afterVisit(Page page, CrawlDatums next) {
         if (autoParse && !regexRule.isEmpty()) {
             
            String conteType = page.getResponse().getContentType();
            if (conteType != null && conteType.contains("text/html")) {
                Document doc = page.getDoc();
                if (doc != null) {
                   Links links=new Links().addByRegex(doc, regexRule);
                   next.add(links);
                }
            }
        }
    }

    @Override
    public void fail(Page page, CrawlDatums next) {
    }

    @Override
    public HttpResponse getResponse(CrawlDatum crawlDatum) throws Exception {
        HttpRequest request=new HttpRequest(crawlDatum);
        return request.getResponse();
    }
    
    
    /**
     * 添加URL正则约束
     *
     * @param urlRegex
     */
    public void addRegex(String urlRegex) {
        regexRule.addRule(urlRegex);
    }

    /**
     *
     * @return 返回是否自动抽取符合正则的链接并加入后续任务
     */
    public boolean isAutoParse() {
        return autoParse;
    }

    /**
     * 设置是否自动抽取符合正则的链接并加入后续任务
     *
     * @param autoParse
     */
    public void setAutoParse(boolean autoParse) {
        this.autoParse = autoParse;
    }

    /**
     *
     * @return
     */
    public RegexRule getRegexRule() {
        return regexRule;
    }

    /**
     *
     * @param regexRule
     */
    public void setRegexRule(RegexRule regexRule) {
        this.regexRule = regexRule;
    }
    
    

}
