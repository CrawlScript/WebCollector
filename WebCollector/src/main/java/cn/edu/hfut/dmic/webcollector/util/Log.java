/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.util;

import cn.edu.hfut.dmic.webcollector.handler.Handler;
import cn.edu.hfut.dmic.webcollector.handler.Message;

/**
 *
 * @author hu
 */
public class Log {
    public static Handler handler=null;
    public static void Info(String type,String info){
        if(handler==null)
            System.out.println(type+":"+info);
        else{
            Message msg=new Message();
            msg.obj=new String[]{type,info};
            handler.sendMessage(msg);
        }
            
        
        
    }
}
