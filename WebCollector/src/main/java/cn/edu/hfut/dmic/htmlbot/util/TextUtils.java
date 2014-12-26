/*
 * Copyright (C) 2014 hu
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

package cn.edu.hfut.dmic.htmlbot.util;

import org.jsoup.nodes.TextNode;

/**
 *
 * @author hu
 */
public class TextUtils {
    public static int countText(String text){
        return text.trim().length();
    }
    
    public static boolean isEmptyNode(TextNode node){
        int count=countText(node.text());
         return count==0;
    }
           
    
    public static char[] puncs=new char[]{',','.',';','\'','\"',
    ',','。',';','‘','’','“'
    };
    
    public static int countPunc(String text){
        text=text.trim();
        int sum=0;
        for(int i=0;i<text.length();i++){
            char c=text.charAt(i);
            for(char punc:puncs){
                if(punc==c){
                    sum++;
                    break;
                }
            }
        }
        return sum;
    }
}
