package cn.edu.hfut.dmic.webcollector.fetcher;

import cn.edu.hfut.dmic.webcollector.conf.DefaultConfigured;
import cn.edu.hfut.dmic.webcollector.model.CrawlDatums;
import cn.edu.hfut.dmic.webcollector.model.Links;
import cn.edu.hfut.dmic.webcollector.model.Page;
import cn.edu.hfut.dmic.webcollector.util.ReflectionUtils;
import cn.edu.hfut.dmic.webcollector.util.RegexRule;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class VisitorMethodDispatcher extends DefaultConfigured{

    public static final Logger LOG = LoggerFactory.getLogger(VisitorMethodDispatcher.class);

    protected boolean autoParse = true;
    protected RegexRule regexRule;


    protected Visitor visitor;


    protected HashMap<String, Method> typeMethodMap;
    protected HashMap<String, Method> urlRegexMethodMap;
    protected HashMap<RegexRule, Method> urlRegexRuleMethodMap;
    protected HashMap<Integer, Method> codeMethodMap;

    public void checkMethod(Method method) throws Exception {
        Class[] paramTypes = method.getParameterTypes();
        if(paramTypes.length != 2){
            throw new Visitor.InvalidAnnotatedVisitorMethodException(visitor, method);
        }


        if(!paramTypes[0].equals(Page.class)){
            throw new Visitor.InvalidAnnotatedVisitorMethodException(visitor, method);
        }

        if(!paramTypes[1].equals(CrawlDatums.class)){
            throw new Visitor.InvalidAnnotatedVisitorMethodException(visitor, method);
        }
    }

    Method visitMethod;
    Method beforeVisitMethod;
    Method afterParseMethod;

    public VisitorMethodDispatcher(Visitor visitor, boolean autoParse, RegexRule regexRule) throws Exception {
        this.visitor = visitor;
        this.autoParse = autoParse;
        this.regexRule = regexRule;
        typeMethodMap = new HashMap<String, Method>();
        urlRegexMethodMap = new HashMap<String, Method>();
        urlRegexRuleMethodMap = new HashMap<RegexRule, Method>();
        codeMethodMap = new HashMap<Integer, Method>();

        visitMethod = visitor.getClass().getMethod("visit", Page.class, CrawlDatums.class);

        Method[] methods = visitor.getClass().getDeclaredMethods();


        for(Method method: methods){

            Visitor.BeforeVisit beforeVisit = method.getAnnotation(Visitor.BeforeVisit.class);
            if(beforeVisit != null){
                checkMethod(method);
                beforeVisitMethod = method;
            }

            Visitor.AfterParse afterParse = method.getAnnotation(Visitor.AfterParse.class);
            if(afterParse != null){
                checkMethod(method);
                afterParseMethod = method;
            }


            Visitor.MatchType matchType = method.getAnnotation(Visitor.MatchType.class);
            if(matchType != null){
                checkMethod(method);
                for(String type: matchType.types()){
                    typeMethodMap.put(type, method);
                }
            }

            Visitor.MatchNullType matchNullType = method.getAnnotation(Visitor.MatchNullType.class);
            if(matchNullType != null){
                checkMethod(method);
                typeMethodMap.put(null, method);
            }

            Visitor.MatchUrl matchUrl = method.getAnnotation(Visitor.MatchUrl.class);
            if(matchUrl != null){
                checkMethod(method);
                urlRegexMethodMap.put(matchUrl.urlRegex(), method);
            }

            Visitor.MatchUrlRegexRule matchUrlRegexRule= method.getAnnotation(Visitor.MatchUrlRegexRule.class);
            if(matchUrlRegexRule != null){
                checkMethod(method);
                RegexRule urlRegexRule = new RegexRule(matchUrlRegexRule.urlRegexRule());
                urlRegexRuleMethodMap.put(urlRegexRule, method);
            }

            Visitor.MatchCode matchCode = method.getAnnotation(Visitor.MatchCode.class);
            if(matchCode != null){
                checkMethod(method);
                for(int code: matchCode.codes()){
                    codeMethodMap.put(code, method);
                }
            }

            Annotation[] annotations = {
                    matchType,
                    matchNullType,
                    matchUrl,
                    matchUrlRegexRule,
                    beforeVisit,
                    afterParse
            };

//            boolean useType = matchType!=null || matchNullType!=null;
//            boolean useRegex = matchUrl!=null || matchUrlRegexRule != null;
//
//            if(useType && useRegex){
//                StringBuilder sb = new StringBuilder("\n\tType and Regex Annotations cannot be used together for  \"")
//                        .append(ReflectionUtils.getFullMethodName(method))
//                        .append("\"");
//                throw new Visitor.InvalidAnnotatedVisitorMethodException(
//                        sb.toString()
//                );
//            }


            for(Annotation annotation: annotations){
                if(annotation != null){
                    if(method.equals(visitMethod)){
                        StringBuilder sb = new StringBuilder("\n\tdefault visit method \"")
                                .append(ReflectionUtils.getFullMethodName(method))
                                .append("\" cannot be annotated with \"")
                                .append(annotation.annotationType().getName())
                                .append("\"");
                        throw new Visitor.InvalidAnnotatedVisitorMethodException(sb.toString());
                    }
                }
            }

        }


    }


//    protected void invokeVisit(HashSet<Method> invokedMethods, Method method, Visitor visitor, Page page, CrawlDatums next) throws InvocationTargetException, IllegalAccessException {
//        if(invokedMethods.contains(method)){
//            return;
//        }
//        method.invoke(visitor, page, next);
//        invokedMethods.add(method);
//    }


    public Method getMethodByCode(Page page){
        Method method = codeMethodMap.get(page.code());
        return method;
    }

    public Method getMethodByType(Page page){
        String type = page.crawlDatum().type();
        return typeMethodMap.get(type);
    }

    public Method getMethodByUrlRegex(Page page){
        for(Map.Entry<String, Method> entry: urlRegexMethodMap.entrySet()){
            String urlRegex = entry.getKey();
            if(page.matchUrl(urlRegex)){
                return entry.getValue();
            }
        }
        return null;
    }
    public Method getMethodByUrlRegexRule(Page page){
        for(Map.Entry<RegexRule, Method> entry: urlRegexRuleMethodMap.entrySet()){
            RegexRule regexRule = entry.getKey();
            if(page.matchUrlRegexRule(regexRule)){
                return entry.getValue();
            }
        }
        return null;
    }

    public void dispatch(Page page, CrawlDatums next) throws InvocationTargetException, IllegalAccessException {
        HashSet<Method> invokedMethods = new HashSet<Method>();

        if(beforeVisitMethod != null){
            beforeVisitMethod.invoke(visitor, page, next);
        }

        Method method;
        method = getMethodByCode(page);
        if(method == null){
            method = getMethodByType(page);
        }
        if(method == null){
            method = getMethodByUrlRegex(page);
        }
        if(method == null){
            method = getMethodByUrlRegexRule(page);
        }
        if(method == null){
            method = visitMethod;
        }
        method.invoke(visitor, page, next);
//        visitor.visit(page, next);

        if (autoParse && !regexRule.isEmpty()) {
            parseLink(page, next);
        }

        if(afterParseMethod != null){
            afterParseMethod.invoke(visitor, page, next);
        }
    }

    public boolean isAutoParse() {
        return autoParse;
    }

    public void setAutoParse(boolean autoParse) {
        this.autoParse = autoParse;
    }

    protected void parseLink(Page page, CrawlDatums next) {
        String conteType = page.contentType();
        if (conteType != null && conteType.contains("text/html")) {
            Document doc = page.doc();
            if (doc != null) {
                Links links = new Links().addByRegex(doc, regexRule, getConf().getAutoDetectImg());
                next.add(links);
            }
        }

    }



    //    public static void main(String[] args) throws Exception {
//        Visitor visitor = new Visitor() {
//
//            @MatchNullType
//            @MatchType(types = {"list"})
//            public void visitListType(Page page, CrawlDatums next) {
//
//            }
//
//            @MatchNullType
//            @Override
//            public void visit(Page page, CrawlDatums next) {
//
//            }
//        };
//
//        VisitorMethodDispatcher visitorMethodDispatcher = new VisitorMethodDispatcher(visitor);
//
//    }
//
//

}
