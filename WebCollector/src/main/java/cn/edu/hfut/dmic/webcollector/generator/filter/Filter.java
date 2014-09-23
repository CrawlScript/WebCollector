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

/**
 * 爬取任务过滤器，是爬取任务生成器的一种，嵌套在一个已有的爬取任务生成器外部，从
 * 已有的爬取任务生成器中获取符合规则的任务
 * @author hu
 */
public abstract class Filter implements Generator{
    Generator generator;

    /**
     * 构造一个过滤器(也是爬取任务生成器),从一个已有的爬取任务生成器中获取下一个符合规则的任务
     * @param generator 已有的爬取任务生成器
     */
    public Filter(Generator generator){
        this.generator=generator;
    }
}
