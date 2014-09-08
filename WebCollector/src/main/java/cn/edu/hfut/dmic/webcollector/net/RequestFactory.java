/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.net;

import cn.edu.hfut.dmic.webcollector.util.ConnectionConfig;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;

/**
 *
 * @author hu
 */
public class RequestFactory {
    
    /*
    public static Class<? extends Request> requestClass=HttpRequest.class;
    
    public static void setRequestClass(Class<? extends  Request> requestClass){
        RequestFactory.requestClass=requestClass;
    }
    
    public static Class<? extends Request> requestClass(){
        return requestClass;
    }
    */
    
    public static Request createRequest(String url) throws Exception{
        return RequestFactory.createRequest(url,null,null);
                
    }
    
    public static Request createRequest(String url,Proxy proxy,ConnectionConfig config) throws Exception{
        Request request=new HttpRequest();
        URL _URL=new URL(url);
        request.setURL(_URL);
        request.setProxy(proxy);
        request.setConnectionConfig(config);
        return request;
                
    }
    
}
