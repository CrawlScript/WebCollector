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

import cn.edu.hfut.dmic.webcollector.souplang.nodes.SLDocument;
import cn.edu.hfut.dmic.webcollector.souplang.nodes.SLList;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import javax.xml.parsers.ParserConfigurationException;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 *
 * @author hu
 */
public class SoupLang {

    public static final Logger LOG = LoggerFactory.getLogger(SoupLang.class);
    public Parser parser = null;

    public SoupLang(InputStream is) throws ParserConfigurationException, SAXException, IOException {
        //把xml编译成语义树
        parser = new Parser(is);
    }

    public SoupLang(String filePath) throws ParserConfigurationException, SAXException, IOException {
        FileInputStream fis = new FileInputStream(filePath);
        //把xml编译成语义树
        parser = new Parser(fis);
    }

    public SoupLang(File file) throws FileNotFoundException, ParserConfigurationException, SAXException, IOException {
        FileInputStream fis = new FileInputStream(file);
        //把xml编译成语义树
        parser = new Parser(fis);
    }

    public Context extract(Document doc) {
        Context context = new Context();
        context.output.put("url", doc.baseUri());
        try {
            run(context, parser.root, doc);
        } catch (Exception ex) {
            LOG.info("Exception", ex);
        }
        return context;
    }

    public void run(Context context, LangNode node, Object input) throws Exception {
        if (!node.validate(input)) {
            return;
        }
        Object output = node.process(input, context);
        LOG.debug("execute node "+node.getClass());
        if (node instanceof SLList) {
            ArrayList<Element> list=(ArrayList<Element>) output;
            ArrayList<Context> contextList=new ArrayList<Context>();
            if(node.name!=null){
                context.output.put(node.name, contextList);
            }
            for(int i=0;i<list.size();i++){
                Context newContext=new Context();
                contextList.add(newContext);
                for (int j = 0; j < node.children.size(); j++) {
                    try {
                        run(newContext, node.children.get(j), list.get(i));
                    } catch (Exception ex) {
                        LOG.info("Exception", ex);
                    }
                }
                
            }
        } else {
            node.save(output, context);
            if (output != null) {
                for (int i = 0; i < node.children.size(); i++) {
                    try {
                        run(context, node.children.get(i), output);
                    } catch (Exception ex) {
                        LOG.info("Exception", ex);
                    }
                }
            }
        }

    }

}
