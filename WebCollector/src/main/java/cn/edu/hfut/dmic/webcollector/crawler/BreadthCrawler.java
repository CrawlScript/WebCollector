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
package cn.edu.hfut.dmic.webcollector.crawler;

import cn.edu.hfut.dmic.webcollector.model.Links;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.util.RegexRule;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BreadthCrawler是基于DeepCrawler的广度遍历器,于2.05版加入
 * BreadthCrawler可以设置正则规律，让遍历器自动根据URL的正则遍历网站，可以关闭这个功能，自定义遍历
 * 如果autoParse设置为true，遍历器会自动解析页面中符合正则的链接，加入后续爬取任务，否则不自动解析链接。
 * 注意，爬虫会保证URL的唯一性，也就是会自动进行URL去重，所以用户在编写爬虫时完全不必考虑生成重复URL的问题。
 * 断点爬取中，爬虫仍然会保证爬取任务的唯一性。
 *
 * @author hu
 */
public abstract class BreadthCrawler extends DeepCrawler {


    public static final Logger LOG = LoggerFactory.getLogger(BreadthCrawler.class);

    /**
     * 是否自动抽取符合正则的链接并加入后续任务
     */
    protected boolean autoParse;

    /**
     * URL正则约束
     */
    protected RegexRule regexRule = new RegexRule();

    /**
     *
     * @param crawlPath 维护URL信息的文件夹，如果爬虫需要断点爬取，每次请选择相同的crawlPath
     * @param autoParse 是否自动抽取符合正则的链接并加入后续任务
     */
    public BreadthCrawler(String crawlPath, boolean autoParse) {
        super(crawlPath);
        this.autoParse = autoParse;
    }

    /**
     *
     * @param page
     * @return
     */
    @Override
    public Links visitAndGetNextLinks(Page page) {
        Links nextLinks = new Links();
        if (autoParse) {
            String conteType = page.getResponse().getContentType();
            if (conteType != null && conteType.contains("text/html")) {
                Document doc = page.getDoc();
                if (doc != null) {
                    nextLinks.addAllFromDocument(page.getDoc(), regexRule);
                }
            }
        }
        try {
            visit(page, nextLinks);
        } catch (Exception ex) {
            LOG.info("Exception", ex);
        }
        return nextLinks;
    }

    /**
     * 用户自定义对每个页面的操作，一般将抽取、持久化等操作写在visit方法中。
     *
     * @param page
     * @param nextLinks 需要后续爬取的URL。如果autoParse为true，爬虫会自动抽取符合正则的链接并加入nextLinks。
     */
    public abstract void visit(Page page, Links nextLinks);

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
