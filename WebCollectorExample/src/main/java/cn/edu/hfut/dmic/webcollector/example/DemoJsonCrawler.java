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

package cn.edu.hfut.dmic.webcollector.example;

import cn.edu.hfut.dmic.webcollector.crawler.DeepCrawler;
import cn.edu.hfut.dmic.webcollector.model.Links;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.net.HttpRequesterImpl;
import java.net.HttpURLConnection;
import java.net.ProtocolException;

import org.json.JSONObject;



/**爬取JSON的例子
 * 例如我们爬取http://www.brieftools.info/proxy/test/test1.json
 * 和http://www.brieftools.info/proxy/test/test2.json
 * 
 * 很多JSON爬取必须要设置Cookie、User-Agent
 * 有时候还需要使用POST方法
 * 
 * @author hu
 */
public class DemoJsonCrawler extends DeepCrawler{
    
   

    public DemoJsonCrawler(String crawlPath) {
        super(crawlPath);
        HttpRequesterImpl myRequester=new HttpRequesterImpl(){
            /*Override这个方法，可以用来修改http请求(HttpURLConnection)*/
            @Override
            public void configConnection(HttpURLConnection con) {
                
                try {
                    con.setRequestMethod("POST");
                } catch (ProtocolException ex) {
                   ex.printStackTrace();
                }
                
                /*添加http头*/
                con.addRequestProperty("xxx", "xxxxxxx");
            }
            
        };
        myRequester.setCookie("你的cookie");
        this.setHttpRequester(myRequester);
        
    }

    @Override
    public Links visitAndGetNextLinks(Page page) {
        String jsonStr=page.getHtml();
        JSONObject json=new JSONObject(jsonStr);
        String ip=json.getString("ip");
        int port=json.getInt("port");
        System.out.println("原JSON:"+jsonStr.trim()+"\n"
                +"JSON解析信息 ip="+ip+"  port="+port);
        return null;
    }
    
    public static void main(String[] args) throws Exception{
        
        DemoJsonCrawler crawler=new DemoJsonCrawler("/home/hu/data/wb");
        crawler.addSeed("http://www.brieftools.info/proxy/test/test1.json");
        crawler.addSeed("http://www.brieftools.info/proxy/test/test2.json");
        
        crawler.start(1);
    }
    
    

    
}
