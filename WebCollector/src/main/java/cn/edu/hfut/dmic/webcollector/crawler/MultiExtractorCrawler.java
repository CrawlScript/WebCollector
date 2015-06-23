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

package cn.edu.hfut.dmic.webcollector.crawler;

import cn.edu.hfut.dmic.webcollector.crawler.BreadthCrawler;
import cn.edu.hfut.dmic.webcollector.extract.Extractor;
import cn.edu.hfut.dmic.webcollector.extract.Extractors;
import cn.edu.hfut.dmic.webcollector.extract.RegexExtractorFactory;
import cn.edu.hfut.dmic.webcollector.model.Links;
import cn.edu.hfut.dmic.webcollector.model.Page;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author hu
 */
public class MultiExtractorCrawler extends BreadthCrawler{
    
    protected RegexExtractorFactory regexExtractorFactory=new RegexExtractorFactory();

    public MultiExtractorCrawler(String crawlPath, boolean autoParse) {
        super(crawlPath, autoParse);
    }

    @Override
    public void visit(Page page, Links nextLinks) {
        try {
            Extractors extractors=regexExtractorFactory.createExtractor(page);
            for(Extractor extractor:extractors){
                try{
                    extractor.execute(nextLinks);
                }catch(Exception ex){
                    LOG.info("Exception",ex);
                }
            }
        } catch (Exception ex) {
            LOG.info("Exception",ex);
        }
    }

    public void addExtractor(String urlRegex,Class<? extends Extractor> extractorClass){
        regexExtractorFactory.addExtractor(urlRegex, extractorClass);
    }
    
    public RegexExtractorFactory getRegexExtractorFactory() {
        return regexExtractorFactory;
    }

    public void setRegexExtractorFactory(RegexExtractorFactory regexExtractorFactory) {
        this.regexExtractorFactory = regexExtractorFactory;
    }
    
    
    
    
    public static void main(String[] args) {
        
    }
    
    
    
    
    
}
