/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.net;


import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollector.util.Config;
import cn.edu.hfut.dmic.webcollector.util.ConnectionConfig;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;


/**
 *
 * @author hu
 */
public class HttpRequest implements Request{

    public URL url=null;
    public Proxy proxy=null;
    public ConnectionConfig config=null;

    @Override
    public URL getURL() {
        return url;
    }

    @Override
    public void setURL(URL url) {
        this.url=url;
    }

    @Override
    public Response getResponse(CrawlDatum datum) throws Exception {  
        HttpResponse response=new HttpResponse(url);
        HttpURLConnection con;
        
        if(proxy==null){
            con=(HttpURLConnection) url.openConnection();
        }else{
            con=(HttpURLConnection) url.openConnection(proxy);
        }

        con.setDoInput(true);
        con.setDoOutput(true);
        
        if(config!=null){
            config.config(con);
        }
        
        
        InputStream is;
  
        response.setCode(con.getResponseCode());
        
        
        is=con.getInputStream();
            

        byte[] buf = new byte[2048];
        int read;
        int sum=0;
        int maxsize=Config.maxsize;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        while ((read = is.read(buf)) != -1) {
            if(maxsize>0){
            sum=sum+read;
                if(sum>maxsize){
                    read=maxsize-(sum-read);
                    bos.write(buf, 0, read);                    
                    break;
                }
            }
            bos.write(buf, 0, read);
        }

        is.close();       
        
        response.content=bos.toByteArray();
        response.headers= con.getHeaderFields();
        return response;
    }
    

    public static void main(String[] args) throws Exception{
        Request request=RequestFactory.createRequest("http://www.xinhuanet.com");
        CrawlDatum datum=new CrawlDatum();
        Response response=request.getResponse(datum);
        System.out.println("status="+datum.getStatus());
        System.out.println("code="+response.getCode());
        System.out.println(response.getHeaders());
    }

    @Override
    public void setProxy(Proxy proxy) {
        this.proxy=proxy;
    }

    @Override
    public Proxy getProxy() {
        return proxy;
    }

    @Override
    public void setConnectionConfig(ConnectionConfig config) {
        this.config=config;
    }
    
    
}
