/*
 * Copyright (C) 2015 hu
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
package cn.edu.hfut.dmic.webcollector.extract;

import cn.edu.hfut.dmic.webcollector.model.Page;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 *
 * @author hu
 */
public class RegexExtractorFactory implements ExtractorFactory {

    public HashMap<String, Class<? extends Extractor>> extractorClassMap
            = new HashMap<String, Class<? extends Extractor>>();

    @Override
    public Extractors createExtractor(Page page) throws Exception {
        String url = page.getUrl();
        Extractors extractors=new Extractors();
        for (Entry<String, Class<? extends Extractor>> entry : extractorClassMap.entrySet()) {
            String regex = entry.getKey();
            if (Pattern.matches(regex, url)) {
                Class extractorClass = entry.getValue();
                Constructor<? extends Extractor> cons = extractorClass.getDeclaredConstructor(Page.class);
                Extractor extractor=cons.newInstance(page);
                extractors.add(extractor);
            }
        }
        return extractors;
    }
    
    public void addExtractor(String urlRegex,Class<? extends Extractor> extractorClass){
        extractorClassMap.put(urlRegex, extractorClass);
    }
    
    public static class MyExtractor extends Extractor{

        public MyExtractor(Page page) {
            super(page);
            System.out.println("my extractor");
        }

        @Override
        public boolean shouldExecute() {
            return true;
        }

        @Override
        public void extract() throws Exception {
            addNextLinksByRegex(".*");
        }

        @Override
        public void output() throws Exception {
        }

    
        
    }
    
    
}
