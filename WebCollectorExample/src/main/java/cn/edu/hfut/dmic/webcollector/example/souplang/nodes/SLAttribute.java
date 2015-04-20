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
package cn.edu.hfut.dmic.webcollector.example.souplang.nodes;

import java.util.ArrayList;
import cn.edu.hfut.dmic.webcollector.example.souplang.Context;
import cn.edu.hfut.dmic.webcollector.example.souplang.LangNode;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author hu
 */
public class SLAttribute extends LangNode {
    public static final Logger LOG = LoggerFactory.getLogger(SLAttribute.class);
    public String attributeName = null;
    
    public void readAttribute(org.w3c.dom.Element xmlElement) {
        attributeName = xmlElement.getAttribute("attr");
        if (attributeName.isEmpty()) {
            attributeName = null;
        }
    }
    
    @Override
    public Object process(Object input,Context context) throws InputTypeErrorException {
        if (attributeName == null) {
            return null;
        }
        Element jsoupElement = null;
        Elements jsoupElements = null;
        
        if (input instanceof Element) {
            jsoupElement = (Element) input;
            if (!jsoupElement.hasAttr(attributeName)) {
                return null;
            }
            String result = jsoupElement.attr(attributeName);
            return result;
        } else {
            jsoupElements = (Elements) input;
            ArrayList<String> result = new ArrayList<String>();
            for (Element ele : jsoupElements) {
                if (ele.hasAttr(attributeName)) {
                    System.out.println("attr="+ele.attr(attributeName));
                    result.add(ele.attr(attributeName));
                }
            }
            return result;
        }
        
    }
    
    @Override
    public boolean validate(Object input) throws Exception {
        if (!(input instanceof Element) && !(input instanceof Elements)) {
            return false;
        }
        return true;
    }
    
}
