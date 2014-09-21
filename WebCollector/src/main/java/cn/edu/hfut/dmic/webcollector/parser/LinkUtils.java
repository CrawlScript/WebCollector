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

package cn.edu.hfut.dmic.webcollector.parser;

import cn.edu.hfut.dmic.webcollector.model.Link;
import java.util.ArrayList;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import cn.edu.hfut.dmic.webcollector.model.Page;


/**
 * 链接解析辅助类
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
            return getLinks(page.getDoc());
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }
    
    public static ArrayList<Link> getAll(Page page) {
        try {
            ArrayList<Link> result=getLinks(page.getDoc());
            result.addAll(getImgs(page.getDoc()));
            result.addAll(getCSS(page.getDoc()));
            result.addAll(getJS(page.getDoc()));
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }
    
}
