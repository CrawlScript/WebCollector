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

import cn.edu.hfut.dmic.webcollector.net.HttpResponse;
import cn.edu.hfut.dmic.webcollector.util.CharsetDetector;
import java.io.UnsupportedEncodingException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




/**
 * Page是爬取过程中，内存中保存网页爬取信息的一个容器，与CrawlDatum不同，Page只在内存中存
 * 放，用于保存一些网页信息，方便用户进行自定义网页解析之类的操作。在广度遍历器中，用户覆盖
 * 的visit(Page page)方法，就是通过Page将网页爬取/解析信息传递给用户的。经过http请求、解
 * 析这些流程之后，page内保存的内容会越来越多。
 * @author hu
 */
public class Page{
    
    public static final Logger LOG=LoggerFactory.getLogger(Page.class);
    
    private HttpResponse response=null;
    private String url=null;  
    private String html=null;
    private Document doc=null;  
    
    /**
     * 返回网页/文件的内容
     * @return 网页/文件的内容
     */
    public byte[] getContent() {
        if(response==null)
            return null;
        return response.getContent();
    }

    /**
     * 返回网页的url
     * @return 网页的url
     */
    public String getUrl() {
        return url;
    }

    /**
     * 设置网页的url
     * @param url 网页的url
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * 返回网页的源码字符串
     * @return 网页的源码字符串
     */
    public String getHtml() {
        if(html!=null){
            return html;
        }
        if(getContent()==null){
            return null;
        }
        String charset=CharsetDetector.guessEncoding(getContent());
        try {
            this.html = new String(getContent(),charset);
            return html;
        } catch (UnsupportedEncodingException ex) {
            LOG.info("Exception",ex);
            return null;
        }       
    }

    /**
     * 设置网页的源码字符串
     * @param html 网页的源码字符串
     */
    public void setHtml(String html) {
        this.html = html;
    }

    /**
     * 返回网页解析后的DOM树(Jsoup的Document对象)
     * @return 网页解析后的DOM树
     */
    public Document getDoc() {
        if(doc!=null){
            return doc;
        }
        try{
            
            this.doc=Jsoup.parse(getHtml(),url);
            return doc;
        }catch(Exception ex){
            LOG.info("Exception",ex);
            return null;
        }
        
    }
    

    
    

    /**
     * 设置网页解析后的DOM树(Jsoup的Document对象)
     * @param doc 网页解析后的DOM树
     */
    public void setDoc(Document doc) {
        this.doc = doc;
    }

    public HttpResponse getResponse() {
        return response;
    }

    public void setResponse(HttpResponse response) {
        this.response = response;
    }

    

   
    
}
