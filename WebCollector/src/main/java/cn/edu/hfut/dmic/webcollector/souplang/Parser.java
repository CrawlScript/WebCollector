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

import java.io.IOException;
import java.io.InputStream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import cn.edu.hfut.dmic.webcollector.souplang.nodes.SLAttribute;
import cn.edu.hfut.dmic.webcollector.souplang.nodes.SLDocument;
import cn.edu.hfut.dmic.webcollector.souplang.nodes.SLDocuments;
import cn.edu.hfut.dmic.webcollector.souplang.nodes.SLElement;
import cn.edu.hfut.dmic.webcollector.souplang.nodes.SLElements;
import cn.edu.hfut.dmic.webcollector.souplang.nodes.SLList;
import cn.edu.hfut.dmic.webcollector.souplang.nodes.SLNext;
import cn.edu.hfut.dmic.webcollector.souplang.nodes.SLNextElement;
import cn.edu.hfut.dmic.webcollector.souplang.nodes.SLRoot;
import cn.edu.hfut.dmic.webcollector.souplang.nodes.SLSQL;
import cn.edu.hfut.dmic.webcollector.souplang.nodes.SLStr;
import cn.edu.hfut.dmic.webcollector.souplang.nodes.SLText;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 *
 * @author hu
 */
public class Parser {

   public static final Logger LOG = LoggerFactory.getLogger(Parser.class);

    public LangNode root = new SLRoot();

    public Parser(Element xmlRoot) throws ParserConfigurationException, SAXException, IOException {
        parse(xmlRoot, root);
    }

    public Parser(InputStream is) throws ParserConfigurationException, SAXException, IOException {
        DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = f.newDocumentBuilder();
        Document xmlDoc = builder.parse(is);
        Element xmlRoot = xmlDoc.getDocumentElement();
        parse(xmlRoot, root);
    }

    

    public void parse(Node xmlNode, LangNode lsNode) throws ParserConfigurationException, SAXException, IOException {
        if (xmlNode instanceof Element) {
            Element xmlElement = (Element) xmlNode;
            String tagName = xmlElement.getTagName().toLowerCase();
            LangNode childLSNode = null;

            childLSNode = createNode(xmlNode);

            if (childLSNode != null) {
                lsNode.addChild(childLSNode);
                NodeList xmlChildNodeList = xmlNode.getChildNodes();
                for (int i = 0; i < xmlChildNodeList.getLength(); i++) {
                    parse(xmlChildNodeList.item(i), childLSNode);
                }
            }

        }
    }

    public static LangNode createNode(Node node) {
        if (node instanceof Element) {
            Element element = (Element) node;
            String tagName = element.getTagName().toLowerCase();
            
            if (tagName.equals("list")) {

                SLList slList=new SLList();
                slList.readName(element);
                slList.readCSSSelector(element);
                return slList;
            }

            if (tagName.equals("element")||tagName.equals("el")) {

                SLElement slElement = new SLElement();
                slElement.readName(element);
                slElement.readCSSSelector(element);
                slElement.readIndex(element);

                return slElement;
            }
            
            if(tagName.equals("elements")||tagName.equals("els")){
                SLElements slelements=new SLElements();
                slelements.readName(element);
                slelements.readCSSSelector(element);
                return slelements;
            }
            
            if(tagName.equals("attribute")||tagName.equals("attr")){
                SLAttribute slAttribute=new SLAttribute();
                slAttribute.readName(element);
                slAttribute.readAttribute(element);
                return slAttribute;
            }
            
            if(tagName.equals("sql")){
                SLSQL slSQL=new SLSQL();
                slSQL.readName(element);
                slSQL.readParams(element);
                slSQL.readTemplate(element);
                slSQL.readSql(element);
                return slSQL;
            }
            
            if(tagName.equals("docs")){
                SLDocuments sLDocuments=new SLDocuments();
                return sLDocuments;
            }
            
            if(tagName.equals("str")){
                SLStr slStr=new SLStr();
                slStr.readName(element);
                slStr.readValue(element);
                return slStr;
            }

            if (tagName.equals("next")) {

                SLNext slNext = new SLNext();
                slNext.readName(element);
                slNext.readIndex(element);

                return slNext;
            }
            
            if (tagName.equals("nextelement")||tagName.equals("nexte")) {
                SLNextElement slNextElement=new SLNextElement();
                slNextElement.readName(element);
                slNextElement.readIndex(element);

                return slNextElement;
            }

            if (tagName.equals("text")) {
                SLText slText = new SLText();
                slText.readName(element);
                slText.readRegex(element);
                slText.readGroup(element);
                return slText;
            }

            if (tagName.equals("doc")) {

                SLDocument sLDocument = new SLDocument();
                sLDocument.readName(element);
                sLDocument.readUrlRegex(element);
                return sLDocument;
            }
            if (tagName.equals("root")) {
                return new SLRoot();
            }
            return null;

        } else {
            return null;
        }
    }
}
