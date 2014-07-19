/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.filter;

import java.util.ArrayList;
import java.util.regex.Pattern;

/**
 *
 * @author hu
 */
public class RegexFilter extends Filter{

    
    public RegexFilter(){
    }
    
    public ArrayList<String> positive=new ArrayList<String>();
    public ArrayList<String> negative=new ArrayList<String>();
    
    public void addRule(String rule){
        if(rule.length()==0){
            return;
        }
        char pn=rule.charAt(0);
        String realrule=rule.substring(1);
        if(pn=='+'){
            addPositive(realrule);
        }else if(pn=='-'){
            addNegative(realrule);
        }else{
            addPositive(rule);
        }
    }
    
    public void addPositive(String positiveregex){
        positive.add(positiveregex);        
    }
    public void addNegative(String negativeregex){
        negative.add(negativeregex);
    }
    
    @Override
    public boolean shouldFilter(Object object) {
        
        
        String url=(String) object;
        for(String nregex:negative){
            if(Pattern.matches(nregex, url)){
                return true;
            }
        }
        
        
        int count=0;
        for(String pregex:positive){
            if(Pattern.matches(pregex, url)){
                count++;
            }
        }
        if(count==0)
            return true;
        else
            return false;
    }
    
    
}
