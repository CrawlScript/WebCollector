/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.filter;

import java.util.HashSet;
import java.util.UUID;
import cn.edu.hfut.dmic.webcollector.model.Page;

/**
 *
 * @author hu
 */
public class UniqueFilter extends Filter{
    public HashSet<String> hashset=new HashSet<String>();
    
    public void addUrl(String url){
         hashset.add(url);
    }

    @Override
    public boolean  shouldFilter(Object object) {
      
        String url=(String) object;
        if(hashset.contains(url)){
            return true;
        }
        else{
           
            return false;
        }
     
        
    }
    
    public static void main(String[] args){
        HashSet<String> test=new HashSet<String>();
        for(int i=0;i<1000000000;i++){
            System.out.println(i);
            test.add("https://www.facebook.com/"+UUID.randomUUID()+UUID.randomUUID());
        }
    }
    
}
