package com.github.dxee.joo.kafka;

import java.lang.reflect.Type;

/**
 * Get the typename
 *
 * @author bing.fan
 * 2018-07-10 22:46
 */
public class TypeNames {

    public static String of(Type t) {
        return t.getTypeName();
    }

    public static String of(Object object) {
        return object.getClass().getTypeName();
    }
}
