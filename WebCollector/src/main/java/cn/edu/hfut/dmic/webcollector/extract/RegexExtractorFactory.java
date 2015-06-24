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
import java.util.ArrayList;
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

    
    
    protected ArrayList<ExtractorInfo> extractorInfoList=new ArrayList<ExtractorInfo>();
    
    public static class ExtractorInfo{
        public String regex;
        public Class<? extends Extractor> extractorClass;
        public ExtractorParams params;

        public ExtractorInfo(String regex, Class<? extends Extractor> extractorClass, ExtractorParams params) {
            this.regex = regex;
            this.extractorClass = extractorClass;
            this.params = params;
        }
    }

    @Override
    public Extractors createExtractor(Page page) throws Exception {
        String url = page.getUrl();
        Extractors extractors = new Extractors();
        for(ExtractorInfo extractorInfo:extractorInfoList){
            String regex=extractorInfo.regex;
             if (Pattern.matches(regex, url)) {
                Class extractorClass = extractorInfo.extractorClass;
                Constructor<? extends Extractor> cons = extractorClass.getDeclaredConstructor(Page.class, ExtractorParams.class);
                ExtractorParams params = extractorInfo.params;
                Extractor extractor = cons.newInstance(page, params);
                extractors.add(extractor);
            }
        }
        
        return extractors;
    }

    public void addExtractor(String urlRegex, Class<? extends Extractor> extractorClass) {
        addExtractor(urlRegex, extractorClass, null);
    }

    public void addExtractor(String urlRegex, Class<? extends Extractor> extractorClass, ExtractorParams params) {
        ExtractorInfo extractorInfo=new ExtractorInfo(urlRegex, extractorClass, params);
        extractorInfoList.add(extractorInfo);
    }

    public ArrayList<ExtractorInfo> getExtractorInfoList() {
        return extractorInfoList;
    }

    public void setExtractorInfoList(ArrayList<ExtractorInfo> extractorInfoList) {
        this.extractorInfoList = extractorInfoList;
    }

    

}
