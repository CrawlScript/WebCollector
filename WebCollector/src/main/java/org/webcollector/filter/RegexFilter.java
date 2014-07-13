/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.webcollector.filter;

import java.util.regex.Pattern;

/**
 *
 * @author hu
 */
public class RegexFilter extends Filter{

    public String regex;
    public RegexFilter(String regex){
        this.regex=regex;
    }
    
    @Override
    public boolean shouldFilter(Object object) {
        String url=(String) object;
        return !(Pattern.matches(regex, url));
    }
    
    
}
