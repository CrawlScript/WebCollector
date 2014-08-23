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
public class HandlerUtils {
    public static void sendMessage(Handler handler,Message msg){
        sendMessage(handler, msg,false);
    }
    public static void sendMessage(Handler handler,Message msg,boolean checknull){
        if(checknull){
            if(handler==null){
                return;
            }
        }
        handler.sendMessage(msg);
    }
}
