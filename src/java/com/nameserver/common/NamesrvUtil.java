package com.nameserver.common;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Properties;

import com.nameserver.annotation.ImportantField;
import org.slf4j.Logger;

public class NamesrvUtil {

    public static final String NAMESPACE_ORDER_TOPIC_CONFIG = "ORDER_TOPIC_CONFIG";
    public static final String NAMESPACE_PROJECT_CONFIG = "PROJECT_CONFIG";

    public static final long MASTER_ID = 0L;

    public static void printObjectProperties(final Logger log, final Object object) {
        printObjectProperties(log, object, false);
    }


    public static void printObjectProperties(final Logger log, final Object object,
                                             final boolean onlyImportantField) {
        Field[] fields = object.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (!Modifier.isStatic(field.getModifiers())) {
                String name = field.getName();
                if (!name.startsWith("this")) {
                    Object value = null;
                    try {
                        field.setAccessible(true);
                        value = field.get(object);
                        if (null == value) {
                            value = "";
                        }
                    } catch (IllegalArgumentException e) {
                        System.out.println(e);
                    } catch (IllegalAccessException e) {
                        System.out.println(e);
                    }

                    if (onlyImportantField) {
                        Annotation annotation = field.getAnnotation(ImportantField.class);
                        if (null == annotation) {
                            continue;
                        }
                    }

                    if (log != null) {
                        log.info(name + "=" + value);
                    } else {
                        System.out.println(name + "=" + value);
                    }
                }
            }
        }
    }

    public static void properties2Object(final Properties p, final Object object) {
        Method[] methods = object.getClass().getMethods();
        for (Method method : methods) {
            String mn = method.getName();
            if (mn.startsWith("set")) {
                try {
                    String tmp = mn.substring(4);
                    String first = mn.substring(3, 4);

                    String key = first.toLowerCase() + tmp;
                    String property = p.getProperty(key);
                    if (property != null) {
                        Class<?>[] pt = method.getParameterTypes();
                        if (pt != null && pt.length > 0) {
                            String cn = pt[0].getSimpleName();
                            Object arg = null;
                            if (cn.equals("int")) {
                                arg = Integer.parseInt(property);
                            } else if (cn.equals("long")) {
                                arg = Long.parseLong(property);
                            } else if (cn.equals("double")) {
                                arg = Double.parseDouble(property);
                            } else if (cn.equals("boolean")) {
                                arg = Boolean.parseBoolean(property);
                            } else if (cn.equals("String")) {
                                arg = property;
                            } else {
                                continue;
                            }
                            method.invoke(object, new Object[]{arg});
                        }
                    }
                } catch (Throwable e) {
                }
            }
        }
    }
}
