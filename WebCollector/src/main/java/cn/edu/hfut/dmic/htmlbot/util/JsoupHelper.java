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

package cn.edu.hfut.dmic.htmlbot.util;


import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;
import org.jsoup.select.NodeVisitor;

/**
 *
 * @author hu
 */
public class JsoupHelper {
    

    
    public static void makeAbsolute(Document doc) {
        doc.traverse(new NodeVisitor() {
            
            @Override
            public void head(Node node, int i) {
                if (node instanceof Element) {
                    Element tag = (Element) node;
                    if (tag.hasAttr("href")) {
                        String href = tag.attr("abs:href");
                        tag.attr("href", href);
                    } else if (tag.hasAttr("src")) {
                        String src = tag.attr("abs:src");
                        tag.attr("src", src);
                    }
                    
                }
            }
            
            @Override
            public void tail(Node node, int i) {
            }
        });
    }
    
    public static String defaultColor = "red";
    
    public static void mark(Element tag) {
        mark(tag, defaultColor);
    }

    public static void markAll(Elements tags) {
        markAll(tags, defaultColor);
    }    
    
    public static void markChildren(Element tag) {
        markChildren(tag, defaultColor);
    }
    
    public static void mark(Element tag, String color) {
        String style="border:2px solid " + color + ";";
        if(tag.hasAttr("style")){
            style=tag.attr("style")+";"+style;
        }
        
        tag.attr("mark", "true");
        tag.attr("style", style);
    }

    public static void markAll(Elements tags, String color) {
        for (Element tag : tags) {
            mark(tag, color);
        }
    }

    public static void markChildren(Element tag, final String color) {
        //mark(tag);
        tag.traverse(new NodeVisitor() {
            
            @Override
            public void head(Node node, int i) {
                if (node instanceof Element) {
                    Element tag = (Element) node;
                    mark(tag, color);
                }
            }
            
            @Override
            public void tail(Node node, int i) {
            }
        });
    }
    
    private static String getNodeName(Node node) {
        if (node instanceof TextNode) {
            return "text";
        } else {
            Element element = (Element) node;
            return element.tagName().toLowerCase();
        }
    }
    
    public static String getXpath(Node node) {
        String result = "";
        Node temp = node;
        while (temp != null) {
            String name = getNodeName(temp);
            result = "," + name + result;
            temp = temp.parent();
        }
        return result;
        
    }
    
    
}
