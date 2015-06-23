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
package cn.edu.hfut.dmic.webcollector.extract;

import cn.edu.hfut.dmic.webcollector.model.Links;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.net.HttpResponse;
import cn.edu.hfut.dmic.webcollector.util.RegexRule;
import java.util.List;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 可以针对每一种抽取业务（例如抽取用户信息、新闻内容）分别编写抽取器，最后将抽取器统一
 * 加载到支持抽取器的爬虫MultiExtractorCrawler上，这样方便爬虫代码的分工和维护，并且一些
 * 通用的抽取器可以作为组件重复使用。
 * 
 * 关于抽取器的使用，可以参考WebCollectorExample中的TutorialExtractor
 * (cn.edu.hfut.dmic.webcollector.example.TutorialExtractor)
 * 
 * @author hu
 */
public abstract class Extractor {

    public static final Logger LOG = LoggerFactory.getLogger(Extractor.class);

    protected Page page = null;
    protected Links nextLinks = null;
    protected boolean output = true;

    public Extractor(Page page) {
        this.page = page;
        nextLinks = new Links();
    }

    public abstract boolean shouldExecute();

    public abstract void extract() throws Exception;

    public abstract void output() throws Exception;

    public void execute() {
        execute(null);
    }

    public void execute(Links nextLinks) {
        try {
            if (!shouldExecute()) {
                LOG.debug("Extractor " + this.getClass() + " should not execute on " + getUrl());
                return;
            }
            extract();
            if (nextLinks != null) {
                nextLinks.addAll(this.nextLinks);
            }
            if (output) {
                output();
            }
        } catch (Exception ex) {
            LOG.info("Exception on " + getUrl(), ex);
        }
    }

    public Elements select(String cssSelector) throws Exception {
        return page.getDoc().select(cssSelector);
    }

    public Element selectElement(String cssSelector, int index) throws Exception {

        Elements elements = page.getDoc().select(cssSelector);
        int realIndex;
        if (index < 0) {
            realIndex = elements.size() + index;
        } else {
            realIndex = index;
        }
        return elements.get(realIndex);
    }

    public Element selectElement(String cssSelector) throws Exception {
        return selectElement(cssSelector, 0);
    }

    /**
     * 根据css选择器cssSelector，返回满足选择器的第index个元素的文本 如果index为负，表示获取从后向前第index个元素
     *
     * @param cssSelector
     * @param index
     */
    public String selectText(String cssSelector, int index) throws Exception {

        Elements elements = page.getDoc().select(cssSelector);
        int realIndex;
        if (index < 0) {
            realIndex = elements.size() + index;
        } else {
            realIndex = index;
        }
        return elements.get(realIndex).text();
    }

    public String selectText(String cssSelector, String defaultResult) {
        return selectText(cssSelector, 0, defaultResult);
    }

    public String selectText(String cssSelector, int index, String defaultResult) {
        try {
            return selectText(cssSelector, index);
        } catch (Exception ex) {
            return defaultResult;
        }
    }

    public String selectText(String cssSelector) throws Exception {
        return selectText(cssSelector, 0);
    }

    public Page getPage() {
        return page;
    }

    public void setPage(Page page) {
        this.page = page;
    }

    public boolean getOutput() {
        return output;
    }

    public void setOutput(boolean output) {
        this.output = output;
    }

    public Links getNextLinks() {
        return nextLinks;
    }

    public void setNextLinks(Links nextLinks) {
        this.nextLinks = nextLinks;
    }

    public void addNextLinks(String url){
        nextLinks.add(url);
    }
    
    public void addNextLinks(List<String> urls){
        nextLinks.addAll(urls);
    }
    
    public void addNextLinksByRegex(String regex) {
        RegexRule regexRule = new RegexRule();
        regexRule.addRule(regex);
        nextLinks.addAllFromDocument(page.getDoc(), regexRule);
    }

    public void addNextLinksByRegex(RegexRule regexRule) {
        nextLinks.addAllFromDocument(page.getDoc(), regexRule);
    }

    public void addNextLinksByCssSelector(String cssSelector) {
        nextLinks.addAllFromDocument(page.getDoc(), cssSelector);
    }

    public String getUrl() {
        return page.getUrl();
    }

    public String getHtml() {
        return page.getHtml();
    }

    public Document getDoc() {
        return page.getDoc();
    }

    public byte[] getContent() {
        return page.getContent();
    }

    public HttpResponse getResponse() {
        return page.getResponse();
    }

}
