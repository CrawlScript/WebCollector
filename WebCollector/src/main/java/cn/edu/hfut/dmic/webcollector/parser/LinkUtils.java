/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.parser;

import cn.edu.hfut.dmic.webcollector.model.Link;
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
public class LinkUtils {
    public static ArrayList<Link> getLinks(Document doc) {
        ArrayList<Link> links = new ArrayList<Link>();
        Elements link_elements = doc.select("a[href]");
        for (Element link : link_elements) {
                String anchor=link.text();
                String href=link.attr("abs:href");
                links.add(new Link(anchor, href));
        }
        return links;
    }
    
    public static ArrayList<Link> getImgs(Document doc) {
        ArrayList<Link> links = new ArrayList<Link>();
        Elements link_elements = doc.select("img[src]");
        for (Element link : link_elements) {
                String anchor=link.text();
                String href=link.attr("abs:src");
                links.add(new Link(anchor, href));
        }
        return links;
    }
    
    public static ArrayList<Link> getCSS(Document doc) {
      ArrayList<Link> links = new ArrayList<Link>();
        Elements link_elements = doc.select("link[href]");
        for (Element link : link_elements) {
                String anchor=link.text();
                String href=link.attr("abs:href");
                links.add(new Link(anchor, href));
        }
        return links;
    }
    
    public static ArrayList<Link> getJS(Document doc) {
       ArrayList<Link> links = new ArrayList<Link>();
        Elements link_elements = doc.select("script[src]");
        for (Element link : link_elements) {
                String anchor=link.text();
                String href=link.attr("abs:src");
                links.add(new Link(anchor, href));
        }
        return links;
    }
    
    
    public static ArrayList<Link> getLinks(Page page) {
        try {
            return getLinks(page.doc);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }
    
    public static ArrayList<Link> getAll(Page page) {
        try {
            ArrayList<Link> result=getLinks(page.doc);
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
