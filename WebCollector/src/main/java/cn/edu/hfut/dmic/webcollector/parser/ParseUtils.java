/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.parser;

import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.util.CharsetDetector;
import java.io.UnsupportedEncodingException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/**
 * 解析辅助类
 * @author hu
 */
public class ParseUtils {
    
    
    public static Document parseDocument(byte[] content,String url){
        String charset=CharsetDetector.guessEncoding(content);
        String html;
        try {
            html = new String(content,charset);
        } catch (UnsupportedEncodingException ex) {
            return null;
        }
        Document doc=Jsoup.parse(html);
        doc.setBaseUri(url);
        return doc;
    } 
    
    public static Page parseDocument(Page page){
        Document doc=parseDocument(page.getContent(), page.getUrl());
        page.setDoc(doc);
        return page;
    }
}
