/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.handler;

/**
 * 消息
 * @author hu
 */
public class Message {

    /**
     * 消息附带的数据
     */
    public Object obj;

    /**
     * 消息的种类
     */
    public int what;
    
    /**
     * 构造一个空的消息
     */
    public Message(){
        
    }

    /**
     * 构造一个消息
     * @param what 消息的种类
     * @param obj 消息附带的数据
     */
    public Message(int what,Object obj) {
        this.what = what;
        this.obj = obj;
        
    }
    
    
    
    
    
    
}
