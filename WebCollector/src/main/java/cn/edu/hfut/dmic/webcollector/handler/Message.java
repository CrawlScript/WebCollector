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
public class Message {
    public Object obj;
    public int what;
    
    public Message(){
        
    }

    public Message(int what,Object obj) {
        this.what = what;
        this.obj = obj;
        
    }
    
    
    
    
    
    
}
