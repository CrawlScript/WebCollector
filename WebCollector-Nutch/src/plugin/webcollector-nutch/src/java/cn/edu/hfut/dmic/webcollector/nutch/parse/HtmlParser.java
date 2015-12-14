/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.edu.hfut.dmic.webcollector.nutch.parse;


import java.util.ArrayList;
import java.util.Map;
import java.net.URL;
import java.net.MalformedURLException;
import java.io.*;
import java.util.regex.*;
import java.util.ArrayList;

import org.jsoup.*;
import org.jsoup.nodes.*;
import org.jsoup.select.*;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.protocol.Content;
import org.apache.hadoop.conf.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.util.*;

public class HtmlParser implements Parser {
  public static final Logger LOG = LoggerFactory.getLogger("cn.edu.hfut.dmic.webcollector.nutch.parse");
  private Configuration conf;
  private HtmlParseFilters htmlParseFilters;
  

  
  public ParseResult getParse(Content content) {
  
    URL base;
    String url=content.getUrl();
    try {
      base = new URL(content.getBaseUrl());
    } catch (MalformedURLException e) {
      return new ParseStatus(e).getEmptyParseResult(url, getConf());
    }

   
    try{
	    byte[] contentBytes=content.getContent();
	    String encoding=CharsetDetector.guessEncoding(contentBytes);
	    String html=new String(contentBytes,encoding);
	    Document doc=Jsoup.parse(html,url);
	    
	    ArrayList<Outlink> outlinkList=new ArrayList<Outlink>();
            Elements linkEles=doc.select("a[href]");
            for(Element linkEle:linkEles){
	    	String href=linkEle.attr("abs:href");
                String anchor=linkEle.text();
                outlinkList.add(new Outlink(href,anchor));	
   	    }
   	    
   	    
   	    Metadata metadata = new Metadata();
   	    
   	    String title=doc.title();
            String text=doc.text();
            
            /* visit */
            visit(url,html,doc,outlinkList);
            
            
            Outlink[] outlinks=outlinkList.toArray(new Outlink[outlinkList.size()]);
            
            ParseStatus status = new ParseStatus(ParseStatus.SUCCESS);
   	    ParseData parseData = new ParseData(status, title, outlinks,
                                        content.getMetadata(), metadata);                       
            ParseResult parseResult = ParseResult.createParseResult(url, 
                                                 new ParseImpl(text, parseData));
            return parseResult;
    }catch(Exception e){
    	    LOG.error("Error: ", e);
            return new ParseStatus(e).getEmptyParseResult(url, getConf());
    }


  }
  
  /* your code */
  public void visit(String url,String html,Document doc,ArrayList<Outlink> next){
  	
  	System.out.println("title:"+doc.title());
  }


  
  
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.htmlParseFilters = new HtmlParseFilters(getConf());
  }

  public Configuration getConf() {
    return this.conf;
  }
}
