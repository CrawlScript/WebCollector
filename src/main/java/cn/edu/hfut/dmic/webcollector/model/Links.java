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
package cn.edu.hfut.dmic.webcollector.model;

import cn.edu.hfut.dmic.webcollector.util.RegexRule;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * 用于存储多个URL的数据结构，继承于ArrayList
 *
 * @author hu
 */
public class Links implements Iterable<String> {

    protected LinkedList<String> dataList = new LinkedList<String>();

    public Links() {

    }

    public Links(Iterable<String> links) {
        add(links);
    }

    public Links(Collection<String> urls) {
        add(urls);
    }

    public CrawlDatums toCrawlDatums(){
        return new CrawlDatums(this);
    }

    public Links add(String url) {
        dataList.add(url);
        return this;
    }

    public Links add(Iterable<String> links) {
        for (String url : links) {
            dataList.add(url);
        }
        return this;
    }

    public Links add(Collection<String> urls) {
        dataList.addAll(urls);
        return this;
    }

    @Override
    public Iterator<String> iterator() {
        return dataList.iterator();
    }

    public Links filterByRegex(RegexRule regexRule) {
        Iterator<String> ite = iterator();
        while(ite.hasNext()){
            String url = ite.next();
            if (!regexRule.satisfy(url)) {
                ite.remove();
            }
        }
        return this;
    }

    public Links filterByRegex(String regex) {
        RegexRule regexRule = new RegexRule();
        regexRule.addRule(regex);
        return filterByRegex(regexRule);
    }

    public Links addFromElement(Element ele) {
        addFromElement(ele,false);
        return this;
    }

    public Links addFromElement(Element ele, boolean parseImg) {
        add(ele.select("a[href]").eachAttr("abs:href"));
        if(parseImg){
            add(ele.select("img[src]").eachAttr("abs:src"));
        }
        return this;
    }



    /**
     * 添加ele中，满足选择器的元素中的链接 选择器cssSelector必须定位到具体的超链接
     * 例如我们想抽取id为content的div中的所有超链接，这里 就要将cssSelector定义为div[id=content] a
     *

     */
    public Links addBySelector(Element ele, String cssSelector, boolean parseSrc) {
        Elements as = ele.select(cssSelector);
        for (Element a : as) {
            if (a.hasAttr("href")) {
                String href = a.attr("abs:href");
                this.add(href);
            }
            if(parseSrc){
                if(a.hasAttr("src")){
                    String src = a.attr("abs:src");
                    this.add(src);
                }
            }
        }
        return this;
    }
    public Links addBySelector(Element ele, String cssSelector){
        return addBySelector(ele ,cssSelector,false);
    }

    public Links addByRegex(Element ele, RegexRule regexRule, boolean parseSrc) {
        for(String href: ele.select("a[href]").eachAttr("abs:href")){
            if (regexRule.satisfy(href)) {
                this.add(href);
            }
        }
        if(parseSrc) {
            for (String src : ele.select("*[src]").eachAttr("abs:src")){
                if(regexRule.satisfy(src)){
                    this.add(src);
                }
            }
        }
        return this;
    }

    public Links addByRegex(Element ele, RegexRule regexRule) {
        return addByRegex(ele, regexRule, false);
    }

    public Links addByRegex(Element ele, String regex, boolean parseSrc) {
        RegexRule regexRule = new RegexRule(regex);
        return addByRegex(ele, regexRule, parseSrc);
    }
    public Links addByRegex(Element ele, String regex) {
        RegexRule regexRule = new RegexRule(regex);
        return addByRegex(ele,regexRule,false);
    }





    public String get(int index) {
        return dataList.get(index);
    }

    public int size() {
        return dataList.size();
    }

    public String remove(int index) {
        return dataList.remove(index);
    }

    public boolean remove(String url) {
        return dataList.remove(url);
    }

    public void clear() {
        dataList.clear();
    }

    public boolean isEmpty() {

        return dataList.isEmpty();
    }

    public int indexOf(String url) {
        return dataList.indexOf(url);
    }

    @Override
    public String toString() {
        return dataList.toString();
    }

}
