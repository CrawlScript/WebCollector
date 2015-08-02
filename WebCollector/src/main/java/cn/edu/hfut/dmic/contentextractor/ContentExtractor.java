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
package cn.edu.hfut.dmic.contentextractor;

import cn.edu.hfut.dmic.webcollector.net.HttpRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;
import org.jsoup.select.NodeVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ContentExtractor could extract content,title,time from news webpage
 * @author hu
 */
public class ContentExtractor {

    public static final Logger LOG = LoggerFactory.getLogger(ContentExtractor.class);

    protected Document doc;

    ContentExtractor(Document doc) {
        this.doc = doc;
    }

    protected HashMap<Element, CountInfo> infoMap = new HashMap<Element, CountInfo>();

    class CountInfo {

        int textCount = 0;
        int linkTextCount = 0;
        int tagCount = 0;
        int linkTagCount = 0;
        double density = 0;
        double densitySum = 0;
        double score = 0;
        ArrayList<Integer> leafList = new ArrayList<Integer>();

    }

    public void clean() {
        doc.select("script,noscript,style,iframe").remove();
    }

    public CountInfo computeInfo(Node node) {

        if (node instanceof Element) {
            Element tag = (Element) node;
            String tagName = tag.tagName();
            CountInfo countInfo = new CountInfo();
            for (Node childNode : tag.childNodes()) {
                CountInfo childCountInfo = computeInfo(childNode);
                countInfo.textCount += childCountInfo.textCount;
                countInfo.linkTextCount += childCountInfo.linkTextCount;
                countInfo.tagCount += childCountInfo.tagCount;
                countInfo.linkTagCount += childCountInfo.linkTagCount;
                countInfo.leafList.addAll(childCountInfo.leafList);
                countInfo.densitySum += childCountInfo.density;
            }
            countInfo.tagCount++;
            if (tag.tagName().equals("a")) {
                countInfo.linkTextCount = countInfo.textCount;
                countInfo.linkTagCount++;
            }

            int len = countInfo.tagCount - countInfo.linkTagCount;
            if (len == 0) {
                countInfo.density = 0;
            } else {
                countInfo.density = (countInfo.textCount + 0.0) / len;
            }

            infoMap.put(tag, countInfo);
            return countInfo;
        } else if (node instanceof TextNode) {
            TextNode tn = (TextNode) node;
            CountInfo countInfo = new CountInfo();
            int len = tn.text().length();
            countInfo.textCount = len;
            countInfo.leafList.add(len);
            return countInfo;
        } else {
            return new CountInfo();
        }
    }

    public double computeScore(Element tag) {
        CountInfo countInfo = infoMap.get(tag);
        double var = Math.sqrt(computeVar(countInfo.leafList) + 1);
        double score = var * countInfo.densitySum * Math.sqrt(countInfo.textCount - countInfo.linkTextCount);
        return score;
    }

    public double computeVar(ArrayList<Integer> data) {
        if (data.size() == 0) {
            return 0;
        }
        if (data.size() == 1) {
            return data.get(0);
        }
        double sum = 0;
        for (Integer i : data) {
            sum += i;
        }
        double ave = sum / data.size();
        sum = 0;
        for (Integer i : data) {
            sum += (i - ave) * (i - ave);
        }
        sum = sum / data.size();
        return sum;
    }

    public Element getContentElement() throws Exception {
        clean();
        computeInfo(doc.body());
        double maxScore = 0;
        Element content = null;
        for (Map.Entry<Element, CountInfo> entry : infoMap.entrySet()) {
            Element tag = entry.getKey();
            double score = computeScore(tag);
            if (score > maxScore) {
                maxScore = score;
                content = tag;
            }
        }
        if (content == null) {
            throw new Exception("extraction failed");
        }
        return content;
    }

    public News getNews() throws Exception {
        News news = new News();
        Element contentElement;
        try {
            contentElement = getContentElement();
            news.setContentElement(contentElement);
        } catch (Exception ex) {
            LOG.info("news content extraction failed,extraction abort", ex);
            throw new Exception(ex);
        }

        if (doc.baseUri() != null) {
            news.setUrl(doc.baseUri());
        }

        try {
            news.setTime(getTime(contentElement));
        } catch (Exception ex) {
            LOG.info("news title extraction failed", ex);
        }

        try {
            news.setTitle(getTitle(contentElement));
        } catch (Exception ex) {
            LOG.info("title extraction failed", ex);
        }
        return news;
    }

    protected String getTime(Element contentElement) throws Exception {
        String regex = "([0-9]{4}).*?([0-9]{1,2}).*?([0-9]{1,2}).*?([0-9]{1,2}).*?([0-9]{1,2}).*?([0-9]{1,2})";
        Pattern pattern = Pattern.compile(regex);
        Element current = contentElement.parent();
        for (int i = 0; i < 3; i++) {
            if (current == null) {
                break;
            }
            String currentHtml = current.outerHtml();
            Matcher matcher = pattern.matcher(currentHtml);
            if (matcher.find()) {
                return matcher.group(1) + "-" + matcher.group(2) + "-" + matcher.group(3) + " " + matcher.group(4) + ":" + matcher.group(5) + ":" + matcher.group(6);
            }
        }

        throw new Exception("time not fount");

    }

    protected String getTitle(Element contentElement) throws Exception {
        Element current = contentElement;
        for (int i = 0; i < 3; i++) {
            Elements hs = current.select("h1,h2,h3,h4,h5,h6");
            if (hs.size() > 0) {
                return hs.first().text();
            } else {
                current = current.parent();
                if (current == null) {
                    break;
                }
            }
        }
        Elements titles = doc.select("title");
        if (titles.size() > 0) {
            return titles.first().text();
        } else {
            return getTitleByEditDistance(contentElement);
        }
    }

    protected String getTitleByEditDistance(Element contentElement) throws Exception {
        final String metaTitle = doc.title();
        Element current = contentElement;
        for (int i = 0; i < 3; i++) {
            if (current!=doc.body()&&current.parent() != null) {
                current = current.parent();
            }
        }
        final StringBuilder sb = new StringBuilder();
        final AtomicInteger minDis = new AtomicInteger(10000);
        current.traverse(new NodeVisitor() {

            public void head(Node node, int i) {
                if (node instanceof TextNode) {
                    TextNode tn = (TextNode) node;
                    String text = tn.text();
                    int dis = editDistance(text, metaTitle);
                    if (dis < minDis.get()) {
                        minDis.set(dis);
                        sb.setLength(0);
                        sb.append(text);
                    }
                }
            }

            public void tail(Node node, int i) {
            }
        });
        return sb.toString();
    }

    public static int editDistance(String word1, String word2) {
        int len1 = word1.length();
        int len2 = word2.length();

        int[][] dp = new int[len1 + 1][len2 + 1];

        for (int i = 0; i <= len1; i++) {
            dp[i][0] = i;
        }

        for (int j = 0; j <= len2; j++) {
            dp[0][j] = j;
        }

        for (int i = 0; i < len1; i++) {
            char c1 = word1.charAt(i);
            for (int j = 0; j < len2; j++) {
                char c2 = word2.charAt(j);

                if (c1 == c2) {
                    dp[i + 1][j + 1] = dp[i][j];
                } else {
                    int replace = dp[i][j] + 1;
                    int insert = dp[i][j + 1] + 1;
                    int delete = dp[i + 1][j] + 1;

                    int min = replace > insert ? insert : replace;
                    min = delete > min ? min : delete;
                    dp[i + 1][j + 1] = min;
                }
            }
        }

        return dp[len1][len2];
    }

    /*输入Jsoup的Document，获取正文所在Element*/
    public static Element getContentElementByDoc(Document doc) throws Exception {
        ContentExtractor ce = new ContentExtractor(doc);
        return ce.getContentElement();
    }

    /*输入HTML，获取正文所在Element*/
    public static Element getContentElementByHtml(String html) throws Exception {
        Document doc = Jsoup.parse(html);
        return getContentElementByDoc(doc);
    }

    /*输入HTML和URL，获取正文所在Element*/
    public static Element getContentElementByHtml(String html, String url) throws Exception {
        Document doc = Jsoup.parse(html, url);
        return getContentElementByDoc(doc);
    }

    /*输入URL，获取正文所在Element*/
    public static Element getContentElementByUrl(String url) throws Exception {
        HttpRequest request = new HttpRequest(url);
        String html = request.getResponse().getHtmlByCharsetDetect();
        return getContentElementByHtml(html, url);
    }

    /*输入Jsoup的Document，获取正文文本*/
    public static String getContentByDoc(Document doc) throws Exception {
        ContentExtractor ce = new ContentExtractor(doc);
        return ce.getContentElement().text();
    }

    /*输入HTML，获取正文文本*/
    public static String getContentByHtml(String html) throws Exception {
        Document doc = Jsoup.parse(html);
        return getContentElementByDoc(doc).text();
    }

    /*输入HTML和URL，获取正文文本*/
    public static String getContentByHtml(String html, String url) throws Exception {
        Document doc = Jsoup.parse(html, url);
        return getContentElementByDoc(doc).text();
    }

    /*输入URL，获取正文文本*/
    public static String getContentByUrl(String url) throws Exception {
        HttpRequest request = new HttpRequest(url);
        String html = request.getResponse().getHtmlByCharsetDetect();
        return getContentByHtml(html, url);
    }

    /*输入Jsoup的Document，获取结构化新闻信息*/
    public static News getNewsByDoc(Document doc) throws Exception {
        ContentExtractor ce = new ContentExtractor(doc);
        return ce.getNews();
    }

    /*输入HTML，获取结构化新闻信息*/
    public static News getNewsByHtml(String html) throws Exception {
        Document doc = Jsoup.parse(html);
        return getNewsByDoc(doc);
    }

    /*输入HTML和URL，获取结构化新闻信息*/
    public static News getNewsByHtml(String html, String url) throws Exception {
        Document doc = Jsoup.parse(html, url);
        return getNewsByDoc(doc);
    }

    /*输入URL，获取结构化新闻信息*/
    public static News getNewsByUrl(String url) throws Exception {
        HttpRequest request = new HttpRequest(url);
        String html = request.getResponse().getHtmlByCharsetDetect();
        return getNewsByHtml(html, url);
    }

    public static void main(String[] args) throws Exception {

        News news = ContentExtractor.getNewsByUrl("http://www.huxiu.com/article/121959/1.html");
        System.out.println(news.getUrl());
        System.out.println(news.getTitle());
        System.out.println(news.getTime());
        System.out.println(news.getContent());
        
        //System.out.println(news);

    }

}
