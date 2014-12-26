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

package cn.edu.hfut.dmic.webcollector.deepcrawler.example;

import cn.edu.hfut.dmic.webcollector.deepcrawler.DeepCrawler;
import cn.edu.hfut.dmic.webcollector.deepcrawler.DeepLinks;
import cn.edu.hfut.dmic.webcollector.deepcrawler.Visitor;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.net.HttpRequest;
import cn.edu.hfut.dmic.webcollector.net.Request;
import cn.edu.hfut.dmic.webcollector.parser.ParseUtils;
import cn.edu.hfut.dmic.webcollector.util.ConnectionConfig;
import cn.edu.hfut.dmic.webcollector.util.FileUtils;
import cn.edu.hfut.dmic.webcollector.util.LogUtils;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 *
 * @author hu
 */
/*
  用DeepCrawler实现一个爬取JSON数据的爬虫
  由于找不到特别稳定的提供JSON服务的网站，这里做个假设：
  假设一个社交网站，shejiao.com
  网站提供根据用户名查询好友的功能,已知一个用户叫A，通过http请求
  http://shejiao.com/api.jsp?user=A
  可以获得A的好友信息{'friends':['B','C','E']}
  我们获取数据后，又可以拼接成新的http请求:
  http://shejiao.com/api.jsp?user=B
  http://shejiao.com/api.jsp?user=C
  http://shejiao.com/api.jsp?user=E
  这样我们就可以分别获得用户B、C、E的好友列表。

  在很多情况下，我们可能需要一些html页面来为我们提供一些数据。
  比如开发者想先获取100个用户，然后从这100个用户开始完成上面的操作，而不是只从A用户开始。
  假设http://shejiao.com/hot.jsp页面包含一个用户列表，列表有100个用户。

  很多JSON数据需要通过post得到，所以有些情况下，我们还需要自定义createRequest方法。
  例子中我们假设http://shejiao.com/api.jsp?user=X必须用post方法才能获得结果,
  而且访问时必须附带cookies
  自定义createRequest可以解决代理切换的问题。

*/
public class DemoJsonCrawler extends DeepCrawler{
    
    /*定义一个SogouVisitor，这是爬虫遍历的关键*/
    public static class DemoJsonVisitor implements Visitor{
        
        /*
          设计DeepCrawler的时候，要能够分清楚Navigational Page和Target Page(Page可以是html、JSON、XML等)
          Navigational Page并不一定包含我们要的数据，但是通过Navigational Page中的链接，我们
          可以直接或间接地找到待爬取页面。
          Target Page就是包含我们想要的数据的页面。
          往往爬取
        */
        @Override
        public DeepLinks visitAndGetNextLinks(Page page) {
            String url=page.getUrl();
            if(Pattern.matches("http://shejiao.com/api.jsp\\?user.*", url)){
                
                String jsonResult;
                try {
                    
                    //jsonResult类似{'friends':['B','C','E']}
                    jsonResult = new String(page.getContent(),"utf-8");
                    JSONObject friendsJson=new JSONObject(jsonResult);
                    JSONArray friends=friendsJson.getJSONArray("friends");
                    
                    DeepLinks nextLinks=new DeepLinks();
                    for(int i=0;i<friends.length();i++){
                        String friendName=friends.getString(i);
                        String nextLink="http://shejiao.com/api.jsp?user="+friendName;
                        nextLinks.add(nextLink);
                    }
                    return nextLinks;
                    
                } catch (Exception ex) {
                    LogUtils.getLogger().info("Exception",ex);
                }

                
                
            }else if(Pattern.matches("http://shejiao.com/hot.jsp", url)){
                /*如果当前访问的页面，用户列表页面，抽取页面中的用户名，返回*/
                page=ParseUtils.parseDocument(page);
                DeepLinks nextLinks=new DeepLinks();
                Elements links=page.getDoc().select("CSS选择器");
                for(Element link:links){
                    String friendName=link.text();
                    String nextLink="http://shejiao.com/api.jsp?user="+friendName;
                    nextLinks.add(nextLink);
                }
                return nextLinks;
               
            }
            
            return null;
        }
        
    }

    @Override
    public Visitor createVisitor(String url, String contentType) {
        return new DemoJsonVisitor();
    }

    /*自定义生成http请求*/
    @Override
    public Request createRequest(String url) throws Exception {
        HttpRequest request = new HttpRequest();
        URL _URL = new URL(url);
        request.setURL(_URL);
        final StringBuilder requestMethod=new StringBuilder();
        
        /*根据URL来判断使用GET还是POST*/
        if(Pattern.matches("http://shejiao.com/api.jsp\\?user.*", url)){
            requestMethod.append("POST");
        }else{
            requestMethod.append("GET");
        }
        
        
        /*如果需要设置代理，可以在这里,null表示不设置代理*/
        request.setProxy(null);
        ConnectionConfig conConfig=new ConnectionConfig() {

            @Override
            public void config(HttpURLConnection con) {
                try {
                    con.addRequestProperty("User-Agent", "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:27.0) Gecko/20100101 Firefox/27.0");
                    con.addRequestProperty("Cookie", "你的Cookie");
                    
                    //设置请求方法为GET或者POST
                    con.setRequestMethod(requestMethod.toString());
                } catch (ProtocolException ex) {
                    LogUtils.getLogger().info("Exception",ex);
                }
            }
        };
        request.setConnectionConfig(conConfig);
        return request; 
    }
    
    
    
    public static void main(String[] args) throws Exception{
        DemoJsonCrawler crawler=new DemoJsonCrawler();
        crawler.addSeed("http://shejiao.com/hot.jsp");
        crawler.setThreads(50);
        
        /*如果不能判断需要遍历几层，start后面的参数，可以设置成一个比较大的值*/
        crawler.start(50);
    }

    
}
