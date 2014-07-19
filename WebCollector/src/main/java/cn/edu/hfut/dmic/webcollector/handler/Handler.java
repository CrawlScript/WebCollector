/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.handler;

/**
 *
 * @author hu
 */
public class Handler {
    Integer lock=1;
    public void sendMessage(Message msg){
      //  synchronized(lock){
            handleMessage(msg);
       // }
    }
    
    public void handleMessage(Message msg){
        
    }
    
}
