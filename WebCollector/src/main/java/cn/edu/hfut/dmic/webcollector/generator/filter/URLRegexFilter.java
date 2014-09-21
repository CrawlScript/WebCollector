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
package cn.edu.hfut.dmic.webcollector.generator.filter;

import cn.edu.hfut.dmic.webcollector.generator.Generator;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatum;
import java.util.ArrayList;
import java.util.regex.Pattern;

/**
 * 正则规律过滤器
 * @author hu
 */
public class URLRegexFilter extends Filter {

    private ArrayList<String> positive = new ArrayList<String>();
    private ArrayList<String> negative = new ArrayList<String>();

    /**
     * 根据正则规则列表，生成正则规则过滤器
     * @param generator 嵌套的任务生成器
     * @param rules 正则规则列表
     */
    public URLRegexFilter(Generator generator, ArrayList<String> rules) {
        super(generator);
        for (String rule : rules) {
            addRule(rule);
        }
    }

    /**
     * 添加一个正则规则
     * 正则规则有两种，正正则和反正则
     * URL符合正则规则需要满足下面条件：
     *   1.至少能匹配一条正正则
     *   2.不能和任何反正则匹配
     * @param rule 正则规律，需要按照正反正则的规范输入，在普通正则前加入一个正(负)号来表示正反正则（正正则可不加符号，或者加"+",反正则必须加上'-')
     */
    public void addRule(String rule) {
        if (rule.length() == 0) {
            return;
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
    }

    /**
     * 添加一个正正则规则
     * @param positiveregex
     */
    public void addPositive(String positiveregex) {
        positive.add(positiveregex);
    }

    /**
     * 添加一个反正则规则
     * @param negativeregex
     */
    public void addNegative(String negativeregex) {
        negative.add(negativeregex);
    }

    
    /**
     * 获取下一个符合正则规则的爬取任务
     * URL符合正则规则需要满足下面条件：
     *   1.至少能匹配一条正正则
     *   2.不能和任何反正则匹配
     * @return 下一个符合正则规则的爬取任务，如果没有符合规则的任务，返回null
     */
    @Override
    public CrawlDatum next() {
        while (true) {
            CrawlDatum crawldatum = generator.next();
            if (crawldatum == null) {
                return null;
            }
            String url = crawldatum.getUrl();
            int state=0;
            for (String nregex : negative) {
                if (Pattern.matches(nregex, url)) {
                   state=1;
                   break;
                }
            }
            if(state==1){
                continue;
            }
            int count = 0;
            for (String pregex : positive) {
                if (Pattern.matches(pregex, url)) {
                    count++;
                }
            }
            if (count == 0) {
                continue;
            } else {
                return crawldatum;
            }
        }
    }

}
