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

package cn.edu.hfut.dmic.webcollector.util;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.NodeVisitor;

/**
 *
 * @author hu
 */
public class JsoupUtils {
    public static void makeAbs(Document doc,String url){
        if(url!=null){
            doc.setBaseUri(url);
        }
        doc.traverse(new NodeVisitor() {

            @Override
            public void head(Node node, int i) {
                if(node instanceof Element){
                    Element tag=(Element) node;
                    if(tag.hasAttr("href")){
                        String absHref=tag.attr("abs:href");
                        tag.attr("href",absHref);
                    }
                    if(tag.hasAttr("src")){
                        String absSrc=tag.attr("abs:src");
                        tag.attr("src",absSrc);
                    }
                }
            }

            @Override
            public void tail(Node node, int i) {
            }
        });
    }
}
