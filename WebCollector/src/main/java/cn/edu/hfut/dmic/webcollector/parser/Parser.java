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
