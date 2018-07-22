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
package cn.edu.hfut.dmic.webcollector.util;


import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 *
 * @author hu
 */
public class RegexRule {
    
    public RegexRule(){
        
    }
    public RegexRule(String regex){
        addRule(regex);
    }
    public RegexRule(String... regexes){
        for(String regex:regexes){
            addRule(regex);
        }
    }
    public RegexRule(List<String> regexList){
        for (String regex : regexList) {
            addRule(regex);
        }
    }
    
    public boolean isEmpty(){
        return positive.isEmpty();
    }

    private ArrayList<String> positive = new ArrayList<String>();
    private ArrayList<String> negative = new ArrayList<String>();

  
    
    /**
     * 添加一个正则规则 正则规则有两种，正正则和反正则 
     * URL符合正则规则需要满足下面条件： 1.至少能匹配一条正正则 2.不能和任何反正则匹配
     * 正正则示例：+a.*c是一条正正则，正则的内容为a.*c，起始加号表示正正则
     * 反正则示例：-a.*c时一条反正则，正则的内容为a.*c，起始减号表示反正则
     * 如果一个规则的起始字符不为加号且不为减号，则该正则为正正则，正则的内容为自身
     * 例如a.*c是一条正正则，正则的内容为a.*c
     * @param rule 正则规则
     * @return 自身
     */
    public RegexRule addRule(String rule) {
        if (rule.length() == 0) {
            return this;
        }
        char pn = rule.charAt(0);
        String realrule = rule.substring(1);
        if (pn == '+') {
            addPositive(realrule);
        } else if (pn == '-') {
            addNegative(realrule);
        } else {
            addPositive(rule);
        }
        return this;
    }

   
    
    /**
     * 添加一个正正则规则
     * @param positiveregex
     * @return 自身
     */
    public RegexRule addPositive(String positiveregex) {
        positive.add(positiveregex);
        return this;
    }

  
    /**
     * 添加一个反正则规则
     * @param negativeregex
     * @return 自身
     */
    public RegexRule addNegative(String negativeregex) {
        negative.add(negativeregex);
        return this;
    }

   
    /**
     * 判断输入字符串是否符合正则规则
     * @param str 输入的字符串
     * @return 输入字符串是否符合正则规则
     */
    public boolean satisfy(String str) {

        int state = 0;
        for (String nregex : negative) {
            if (Pattern.matches(nregex, str)) {
                return false;
            }
        }

        int count = 0;
        for (String pregex : positive) {
            if (Pattern.matches(pregex, str)) {
                count++;
            }
        }
        if (count == 0) {
            return false;
        } else {
            return true;
        }

    }
}
