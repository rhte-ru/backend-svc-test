package com.redhat.dsevosty.common.model;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import io.vertx.core.json.JsonObject;

public interface AbstractDataObject extends Serializable {

    public UUID getId();
    public JsonObject toJson();

    default String toStringAbstract() {
        StringBuffer sb = new StringBuffer();
        Class<?> clazz = getClass();
        Method[] methods = clazz.getDeclaredMethods();
        List<Method> methodList = Arrays.asList(methods).stream()
                .filter(method -> Modifier.isPublic(method.getModifiers())).collect(Collectors.toList());
        sb.append("[ ").append(clazz.getName()).append(" -> { ");
        for (Method method : methodList) {
            if (method.getParameterCount() != 0) {
                continue;
            }
            String name = method.getName();
            String fieldName;
            if (name.startsWith("get")) {
                fieldName = name.substring(3);
            } else {
                if (name.startsWith("is")) {
                    fieldName = name.substring(2);
                } else {
                    continue;
                }
            }
            sb.append(fieldName.substring(0, 1).toLowerCase()).append(fieldName.substring(1)).append(": ");
            Object value;
            try {
                value = method.invoke(this);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                value = "Excaption during invoking method '" + name + "': " + e.getMessage();
            }
            if (value instanceof String || value instanceof StringBuffer) {
                sb.append('"').append(value).append('"');
            } else {
                sb.append(value);
            }

            sb.append(", ");
        }
        if (methodList.size() > 0) {
            sb.deleteCharAt(sb.length() - 2);
        }

        return sb.append("} ]").toString();
    }
}
