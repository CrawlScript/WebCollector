/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.util;

/**
 *
 * @author hu
 */
public abstract class Retry<T> {
    public abstract Object run() throws Exception;
    
    public T getResult(int count){
        while(count>0){
            try{
                return (T)run();
            }catch(Exception e){
                System.out.println("retry:");
                e.printStackTrace();
                count--;
            }
            
        }
        return null;
    }
    public static void main(String[] args){
        
        
    }
}
