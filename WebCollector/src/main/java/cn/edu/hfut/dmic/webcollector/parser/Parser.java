/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector.parser;

import cn.edu.hfut.dmic.webcollector.model.Page;

/**
 * 网页解析器接口，用户如果需要自定义网页解析器，必须实现这个接口
 * @author hu
 */
public interface Parser {

    /**
     * 对指定页面进行解析，返回解析结果
     * @param page 待解析页面
     * @return 解析结果
     * @throws Exception
     */
    public ParseResult getParse(Page page) throws Exception;
}
