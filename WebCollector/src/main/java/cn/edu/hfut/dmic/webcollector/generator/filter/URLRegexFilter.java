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
import cn.edu.hfut.dmic.webcollector.util.RegexRule;
import java.util.ArrayList;
import java.util.regex.Pattern;

/**
 * 正则规律过滤器
 * @author hu
 */
public class URLRegexFilter extends Filter {
    
    private RegexRule regexRule=null;

   
    /**
     * 根据正则规则列表，生成正则规则过滤器
     * @param generator 嵌套的任务生成器
     * @param rules 正则规则列表
     */
    public URLRegexFilter(Generator generator, RegexRule regexRule) {
        super(generator);
        this.regexRule=regexRule;
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
            if(regexRule.satisfy(url)){
                return crawldatum;
            }else{
                continue;
            }
            
        }
    }

}
