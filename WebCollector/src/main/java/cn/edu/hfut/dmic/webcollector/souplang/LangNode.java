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
package cn.edu.hfut.dmic.webcollector.souplang;

import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

/**
 *
 * @author hu
 */
public abstract class LangNode {

    public static final Logger LOG = LoggerFactory.getLogger(LangNode.class);

    public String name = null;

    public void readName(Element xmlElement) {
        name = xmlElement.getAttribute("name");
        if (name.isEmpty()) {
            name = null;
        }
    }

    public abstract Object process(Object input, Context context) throws Exception;

    public abstract boolean validate(Object input) throws Exception;

    public void save(Object output, Context context) {
        if (context != null && name != null) {
            context.output.put(name, output);
        }
    }

    public void processAll(Object input, Context context) throws Exception {

        if (!validate(input)) {
            return;
        }
        Object output = null;
        try {
            output = process(input, context);
        } catch (Exception ex) {
            LOG.info("Exception", ex);
            return;
        }
        save(output, context);
        /*
         if (output == null) {
         return;
         }
         for (LanguageNode child : children) {
         child.context=this.context;
         child.processAll(output);
         }
         */
    }
    public ArrayList<LangNode> children = new ArrayList<LangNode>();

    public void addChild(LangNode child) {
        this.children.add(child);
    }

    public void printTree(String prefix) {
        System.out.println(prefix + this.getClass());
        for (LangNode child : children) {
            child.printTree("   " + prefix);
        }
    }
}
