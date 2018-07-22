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

import cn.edu.hfut.dmic.webcollector.util.CharsetDetector;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cn.edu.hfut.dmic.webcollector.util.GsonUtils;
import cn.edu.hfut.dmic.webcollector.util.ListUtils;
import cn.edu.hfut.dmic.webcollector.util.RegexRule;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Page是爬取过程中，内存中保存网页爬取信息的一个容器，Page只在内存中存 放，用于保存一些网页信息，方便用户进行自定义网页解析之类的操作。
 *
 * @author hu
 */
public class Page implements MetaGetter, MetaSetter<Page>{

    public static final Logger LOG = LoggerFactory.getLogger(Page.class);

    private CrawlDatum crawlDatum = null;

    private String contentType;



    private Exception exception = null;

    private String html = null;
    private Document doc = null;

    private String charset = null;
    private byte[] content = null;

    private Object obj = null;

    /**
     * 判断当前Page的URL是否和输入正则匹配
     *
     * @param urlRegex
     * @return
     */
    public boolean matchUrl(String urlRegex) {
        return crawlDatum.matchUrl(urlRegex);
    }

    /**
     * 判断当前Page的URL是否和输入正则规则匹配
     * @param urlRegexRule
     * @return
     */
    public boolean matchUrlRegexRule(RegexRule urlRegexRule) {
        return crawlDatum.matchUrlRegexRule(urlRegexRule);
    }

    /**
     * 判断当前Page(CrawlDatum)的type是否为type
     *
     * @param type
     * @return 是否相等
     */
    public boolean matchType(String type) {
        return crawlDatum.matchType(type);
    }

    /**
     * 判断当前Page的Http响应头的Content-Type是否符合正则
     *
     * @param contentTypeRegex
     * @return
     */
    public boolean matchContentType(String contentTypeRegex) {
        if (contentTypeRegex == null) {
            return contentType() == null;
        }
        return Pattern.matches(contentTypeRegex, contentType());
    }

    public JsonObject jsonObject(){
        return GsonUtils.parse(html()).getAsJsonObject();
    }

    public JsonArray jsonArray(){
        return GsonUtils.parse(html()).getAsJsonArray();
    }

    public JsonObject regexJSONObject(String regex){
        return GsonUtils.parse(regex(regex)).getAsJsonObject();
    }

    public JsonObject regexJSONObject(String regex, int group){
        return GsonUtils.parse(regex(regex, group)).getAsJsonObject();
    }

    public JsonArray regexJSONArray(String regex){
        return GsonUtils.parse(regex(regex)).getAsJsonArray();
    }

    public JsonArray regexJSONArray(String regex, int group){
        return GsonUtils.parse(regex(regex, group)).getAsJsonArray();
    }


    /**
     * 获取网页中满足指定css选择器的所有元素的指定属性的集合
     * 例如通过attrs("img[src]","abs:src")可获取网页中所有图片的链接
     *
     * @param cssSelector
     * @param attrName
     * @return
     */
    public ArrayList<String> attrs(String cssSelector, String attrName) {
        ArrayList<String> result = new ArrayList<String>();
        Elements eles = select(cssSelector);
        for (Element ele : eles) {
            if (ele.hasAttr(attrName)) {
                result.add(ele.attr(attrName));
            }
        }
        return result;
    }

    /**
     * 获取网页中满足指定css选择器的所有元素的指定属性的集合
     * 例如通过attr("img[src]","abs:src")可获取网页中第一个图片的链接
     *
     * @param cssSelector
     * @param attrName
     * @return
     */
    public String attr(String cssSelector, String attrName) {
        return select(cssSelector).attr(attrName);
    }


    public Links links(boolean parseImg) {
        Links links = new Links().addFromElement(doc(),parseImg);
        return links;
    }

    public Links links() {
        return links(false);
    }


    
        /**
     * 获取满足选择器的元素中的链接 选择器cssSelector必须定位到具体的超链接 例如我们想抽取id为content的div中的所有超链接，这里
     * 就要将cssSelector定义为div[id=content] a
     *
     * @param cssSelector
     * @return
     */
    public Links links(String cssSelector, boolean parseSrc) {
        Links links = new Links().addBySelector(doc(), cssSelector,parseSrc);
        return links;
    }
    public Links links(String cssSelector) {
        return links(cssSelector,false);
    }




    public Links regexLinks(RegexRule regexRule, boolean parseSrc) {
        return new Links().addByRegex(doc(), regexRule, parseSrc);
    }
    public Links regexLinks(String regex, boolean parseSrc){
        return new Links().addByRegex(doc(),regex,parseSrc);
    }

    public Links regexLinks(RegexRule regexRule) {
        return regexLinks(regexRule, false);
    }
    public Links regexLinks(String regex){
        return regexLinks(regex,false);
    }


    public ArrayList<String> selectTextList(String cssSelector){
        ArrayList<String> result = new ArrayList<String>();
        Elements eles = select(cssSelector);
        for(Element ele:eles){
            result.add(ele.text());
        }
        return result;
    }

    public String selectText(String cssSelector, int index){
        return ListUtils.getByIndex(selectTextList(cssSelector),index);
    }
    public String selectText(String cssSelector) {
        return select(cssSelector).first().text();
    }

    public ArrayList<Integer> selectIntList(String cssSelector){
        ArrayList<Integer> result = new ArrayList<Integer>();
        for(String text:selectTextList(cssSelector)){
            result.add(Integer.valueOf(text.trim()));
        }
        return result;
    }

    public int selectInt(String cssSelector, int index){
        String text = selectText(cssSelector,index).trim();
        return Integer.valueOf(text);
    }

    public int selectInt(String cssSelector){
        return selectInt(cssSelector,0);
    }

    public ArrayList<Double> selectDoubleList(String cssSelector){
        ArrayList<Double> result = new ArrayList<Double>();
        for(String text:selectTextList(cssSelector)){
            result.add(Double.valueOf(text.trim()));
        }
        return result;
    }

    public double selectDouble(String cssSelector, int index){
        String text = selectText(cssSelector,index).trim();
        return Double.valueOf(text);
    }

    public double selectDouble(String cssSelector){
        return selectDouble(cssSelector,0);
    }

    public ArrayList<Long> selectLongList(String cssSelector){
        ArrayList<Long> result = new ArrayList<Long>();
        for(String text:selectTextList(cssSelector)){
            result.add(Long.valueOf(text.trim()));
        }
        return result;
    }

    public long selectLong(String cssSelector, int index){
        String text = selectText(cssSelector,index).trim();
        return Long.valueOf(text);
    }

    public long selectLong(String cssSelector){
        return selectLong(cssSelector,0);
    }


    public Elements select(String cssSelector) {
        return this.doc().select(cssSelector);
    }

    public Element select(String cssSelector, int index) {
        Elements eles = select(cssSelector);
        int realIndex = index;
        if (index < 0) {
            realIndex = eles.size() + index;
        }
        return eles.get(realIndex);
    }

    public String regex(String regex, int group, String defaultResult) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(html());
        if (matcher.find()) {
            return matcher.group(group);
        } else {
            return defaultResult;
        }
    }

    public String regex(String regex, int group) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(html());
        matcher.find();
        return matcher.group(group);
    }

    public String regexAndFormat(String regex, String format){
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(html());
        matcher.find();
        String[] strs = new String[matcher.groupCount()];
        for(int i=0;i<matcher.groupCount();i++){
            strs[i] = matcher.group(i+1);
        }
        return String.format(format, strs);
    }

    public String regex(String regex, String defaultResult) {
        return regex(regex, 0, defaultResult);
    }

    public String regex(String regex) {
        return regex(regex, 0);
    }

    public Page(CrawlDatum datum,
                String contentType,
                byte[] content){

        this.crawlDatum = datum;
        this.contentType = contentType;
        this.content = content;
    }



    /**
     * 返回网页/文件的内容
     *
     * @return 网页/文件的内容
     */
    public byte[] content() {
        return content;
    }

    public void content(byte[] content){
        this.content = content;
    }


    /**
     * 返回网页的url
     *
     * @return 网页的url
     */
    public String url() {
        return crawlDatum.url();
    }


    /**
     * 返回网页的源码字符串
     *
     * @return 网页的源码字符串
     */
    public String html() {
        if (html != null) {
            return html;
        }

        if (content == null) {
            return null;
        }
        if (charset == null) {
            charset = CharsetDetector.guessEncoding(content());
        }
        try {
            html = new String(content, charset);
        } catch (UnsupportedEncodingException e) {
            LOG.info("Exception when decoding "+ key(),e);
            return null;
        }
        return html;
    }

    /**
     * 设置网页的源码字符串
     *
     * @param html 网页的源码字符串
     */
    public void html(String html) {
        this.html = html;
    }



    public String contentType() {
        return contentType;
    }

    /**
     * 返回网页解析后的DOM树(Jsoup的Document对象) 已废弃，使用doc()方法代替
     *
     * @return 网页解析后的DOM树
     */
    public Document doc() {
        if (doc != null) {
            return doc;
        }
        this.doc = Jsoup.parse(html(), url());
        return doc;
//        try {
//            this.doc = Jsoup.parse(html(), url());
//            return doc;
//        } catch (Exception ex) {
//            LOG.info("Exception", ex);
//            return null;
//        }
    }

    /**
     * 设置网页解析后的DOM树(Jsoup的Document对象)
     *
     * @param doc 网页解析后的DOM树
     */
    public void doc(Document doc){
        this.doc = doc;
    }



    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public CrawlDatum crawlDatum() {
        return crawlDatum;
    }

    public void crawlDatum(CrawlDatum crawlDatum) {
        this.crawlDatum = crawlDatum;
    }




    public JsonObject meta() {
        return crawlDatum.meta();
    }

    @Override
    public String meta(String key) {
        return crawlDatum.meta(key);
    }

    @Override
    public int metaAsInt(String key) {
        return crawlDatum.metaAsInt(key);
    }

    @Override
    public boolean metaAsBoolean(String key) {
        return crawlDatum.metaAsBoolean(key);
    }

    @Override
    public double metaAsDouble(String key) {
        return crawlDatum.metaAsDouble(key);
    }

    @Override
    public long metaAsLong(String key) {
        return crawlDatum.metaAsLong(key);
    }

    @Override
    public JsonObject copyMeta() {
        return crawlDatum.copyMeta();
    }

//    public void meta(HashMap<String, String> metaData) {
//        this.crawlDatum.meta(metaData);
//    }
//
//    public void meta(String key, String value) {
//        this.crawlDatum.meta(key, value);
//    }




    public String charset() {
        if (charset == null) {
            charset = CharsetDetector.guessEncoding(content());
        }
        return charset;
    }

    public void charset(String charset) {
        this.charset = charset;
    }


    public String key() {
        return crawlDatum.key();
    }


    public int code() {
        return crawlDatum.code();
    }

    public String location(){
        return crawlDatum.location();
    }



    public <T> T obj() {
        return (T)obj;
    }

    public void obj(Object obj) {
        this.obj = obj;
    }

    @Override
    public Page meta(JsonObject metaData) {
        crawlDatum.meta(metaData);
        return this;
    }

    @Override
    public Page meta(String key, String value) {
        crawlDatum.meta(key,value);
        return this;
    }

    @Override
    public Page meta(String key, int value) {
        crawlDatum.meta(key,value);
        return this;
    }

    @Override
    public Page meta(String key, boolean value) {
        crawlDatum.meta(key,value);
        return this;
    }

    @Override
    public Page meta(String key, double value) {
        crawlDatum.meta(key,value);
        return this;
    }

    @Override
    public Page meta(String key, long value) {
        crawlDatum.meta(key,value);
        return this;
    }
}
