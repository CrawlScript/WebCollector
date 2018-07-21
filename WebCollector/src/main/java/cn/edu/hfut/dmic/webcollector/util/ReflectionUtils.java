package cn.edu.hfut.dmic.webcollector.util;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class ReflectionUtils {
    public static String getMethodDeclaration(Method method){
        StringBuilder sb = new StringBuilder();
        String accessibility = getMethodAccessibility(method);
        sb.append(accessibility.equals("default")?"": accessibility+" ")
                .append("void")
                .append(" ")
                .append(method.getName())
                .append("(");
        Class[] paramTypes = method.getParameterTypes();
        for(int paramIndex=0;paramIndex<paramTypes.length;paramIndex++){
            Class paramType = paramTypes[paramIndex];
            sb.append(paramType.getName()).append(" ").append("param").append(paramIndex);
            if(paramIndex != paramTypes.length - 1){
                sb.append(", ");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    public static String getMethodAccessibility(Method method){
        int modifiers = method.getModifiers();
        if(Modifier.isPublic(modifiers)){
            return "public";
        }
        if(Modifier.isProtected(modifiers)){
            return "protected";
        }
        if(Modifier.isPrivate(modifiers)){
            return "private";
        }
        return "default";
    }

    public static String getFullMethodName(Method method){
        return method.getDeclaringClass().getName()+"."+method.getName();
    }
}
