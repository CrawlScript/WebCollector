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
package cn.edu.hfut.dmic.webcollector.souplang.nodes;

import cn.edu.hfut.dmic.webcollector.souplang.Context;
import cn.edu.hfut.dmic.webcollector.souplang.LangNode;
import java.util.ArrayList;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author hu
 */
public class SLList extends LangNode {
    
    public static final Logger LOG = LoggerFactory.getLogger(SLList.class);

    public String cssSelector = null;
    

     public void readCSSSelector(org.w3c.dom.Element xmlElement){
        cssSelector=xmlElement.getAttribute("selector");
        if(cssSelector.isEmpty()){
            cssSelector=null;
        }
    }
     
    

    @Override
    public Object process(Object input,Context context) throws InputTypeErrorException {

        ArrayList<Element> elementList=new ArrayList<Element>();
        Element jsoupElement=null;
        Elements jsoupElements=null;
        
        if(input instanceof Element){
            jsoupElement = (Element) input;
        }else{
            jsoupElements=(Elements) input;
        }
        
        if (cssSelector != null&&!cssSelector.isEmpty()) {
            
            Elements result;
            if(jsoupElement!=null){
                result=jsoupElement.select(cssSelector);
            }else{
                result=jsoupElements.select(cssSelector);
            }
            for(Element item:result){
                elementList.add(item);
            }
            //System.out.println("this is element" + result);
            return elementList;
        } else {
            if(jsoupElement!=null){
                elementList.add(jsoupElement);
            }else{
                for(Element child:jsoupElements){
                    elementList.add(child);
                }
            }
            return elementList;
        }

    }

    @Override
    public boolean validate(Object input) throws Exception {
        if (!(input instanceof Element)&&!(input instanceof Elements)) {
            return false;
        }
        return true;
    }

}
