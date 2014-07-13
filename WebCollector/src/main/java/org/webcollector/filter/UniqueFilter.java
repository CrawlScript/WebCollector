/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.webcollector.filter;

import java.util.HashSet;
import org.webcollector.model.Page;

/**
 *
 * @author hu
 */
public class UniqueFilter extends Filter{
    public HashSet hashset=new HashSet();

    @Override
    public boolean shouldFilter(Object object) {
        String url=(String) object;
        if(hashset.contains(url)){
            return true;
        }
        else{
            hashset.add(url);
            return false;
        }
        
    }
    
}
