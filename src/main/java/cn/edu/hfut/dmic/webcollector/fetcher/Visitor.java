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
package cn.edu.hfut.dmic.webcollector.fetcher;

import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.util.ReflectionUtils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * @author hu
 */
public interface Visitor {


    /**
     * @param page 当前访问页面的信息
     * @param next 可以手工将希望后续采集的任务加到next中（会参与自动去重）
     */
    void visit(Page page, CrawlDatums next);


    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface MatchType {
        String[] types();
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface MatchNullType {
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface MatchUrl {
        String urlRegex();
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface MatchUrlRegexRule {
        String[] urlRegexRule();
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface BeforeVisit {
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface AfterParse {
    }


    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface MatchCode {
        int[] codes();
    }



    public static class InvalidAnnotatedVisitorMethodException extends Exception {

        static String buildMessage(Visitor visitor, Method method) {

            String fullMethodName = ReflectionUtils.getFullMethodName(method);

            String validMethodFormat = String.format("public void %s(%s param0, %s param1){...}",
                    method.getName(),
                    Page.class.getName(),
                    CrawlDatums.class.getName()
            );
            StringBuilder sb = new StringBuilder("\n\tThe definition of ")
                    .append(fullMethodName)
                    .append(" is invalid,\n")
                    .append("\texpect    \"").append(validMethodFormat).append("\",")
                    .append("\n\tbut found \"")
                    .append(ReflectionUtils.getMethodDeclaration(method)).append("{...}")
                    .append("\"");
            return sb.toString();

        }

        public InvalidAnnotatedVisitorMethodException(String message) {
            super(message);
        }

        public InvalidAnnotatedVisitorMethodException(Visitor visitor, Method method) {
            super(buildMessage(visitor, method));

        }

    }
}
