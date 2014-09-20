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
 *
 * @author hu
 */
public class URLRegexFilter extends Filter {

    public ArrayList<String> positive = new ArrayList<String>();
    public ArrayList<String> negative = new ArrayList<String>();

    public URLRegexFilter(Generator generator, ArrayList<String> rules) {
        super(generator);
        for (String rule : rules) {
            addRule(rule);
        }
    }

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

    public void addPositive(String positiveregex) {
        positive.add(positiveregex);
    }

    public void addNegative(String negativeregex) {
        negative.add(negativeregex);
    }

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
