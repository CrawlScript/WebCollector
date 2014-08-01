/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.parser;

import cn.edu.hfut.dmic.webcollector.model.Link;
import java.util.ArrayList;

/**
 *
 * @author hu
 */
public class ParseResult {
    public String title;
    public ArrayList<Link> links;
    public ParseResult(String title,ArrayList<Link> links){
        this.title=title;
        this.links=links;
    }
}
