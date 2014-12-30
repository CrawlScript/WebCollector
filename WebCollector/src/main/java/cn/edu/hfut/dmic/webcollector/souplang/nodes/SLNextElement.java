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
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author hu
 */
public class SLNextElement extends LangNode {

    public static final Logger LOG = LoggerFactory.getLogger(SLNextElement.class);
    public int index = 0;

    public void readIndex(org.w3c.dom.Element xmlElement){
        String indexAttr=xmlElement.getAttribute("index");
        if(!indexAttr.isEmpty()){
            index=Integer.valueOf(indexAttr);
        }
    }

    @Override
    public Object process(Object input,Context context) throws InputTypeErrorException {
        Element jsoupElement=(Element) input;
        Element result=jsoupElement;
        for (int i = 0; i <= index; i++) {
            result = result.nextElementSibling();
        }
        LOG.debug(result.outerHtml());
        return result;
    }

    @Override
    public boolean validate(Object input) throws Exception {
        if (!(input instanceof Element)) {
            return false;
        }
        return true;
    }

}
