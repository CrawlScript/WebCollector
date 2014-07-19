/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.parser;

import java.util.ArrayList;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.util.HttpUtils;

/**
 *
 * @author hu
 */
public class LinkParser {
    public static ArrayList<String> getLinks(Document doc) {
        ArrayList<String> outlinks = new ArrayList<String>();
        Elements links = doc.select("a");
        for (Element link : links) {
            if (link.hasAttr("href")) {
                outlinks.add(link.attr("abs:href"));
            }
        }
        return outlinks;
    }
    
    public static ArrayList<String> getImgs(Document doc) {
        ArrayList<String> outlinks = new ArrayList<String>();
        Elements links = doc.select("img");
        for (Element link : links) {
            if (link.hasAttr("src")) {
                outlinks.add(link.attr("abs:src"));
            }
        }
        return outlinks;
    }
    
    public static ArrayList<String> getCSS(Document doc) {
        ArrayList<String> outlinks = new ArrayList<String>();
        Elements links = doc.select("link");
        for (Element link : links) {
            if (link.hasAttr("href")) {
                outlinks.add(link.attr("abs:href"));
            }
        }
        return outlinks;
    }
    
    public static ArrayList<String> getJS(Document doc) {
        ArrayList<String> outlinks = new ArrayList<String>();
        Elements links = doc.select("script");
        for (Element link : links) {
            if (link.hasAttr("src")) {
                outlinks.add(link.attr("abs:src"));
            }
        }
        return outlinks;
    }

    public static ArrayList<String> getLinks(Page page) {
        try {
            return getLinks(page.doc);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }
    
    public static ArrayList<String> getAll(Page page) {
        try {
            ArrayList<String> result=getLinks(page.doc);
            result.addAll(getImgs(page.doc));
            result.addAll(getCSS(page.doc));
            result.addAll(getJS(page.doc));
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }
    
}
