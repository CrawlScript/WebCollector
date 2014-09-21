/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.handler;

/**
 * 用于传递和处理消息类
 * @author hu
 */
public class Handler {
    
    /**
     * 发送一条消息
     * @param msg 待发送消息
     */
    public void sendMessage(Message msg){     
            handleMessage(msg);
    }
    
    /**
     * 处理消息，用户可以通过Override这个方法，来自定义处理消息的方法
     * @param msg
     */
    public void handleMessage(Message msg){
        
    }
    
}
