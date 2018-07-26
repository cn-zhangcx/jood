package com.github.dxee.woow.kafka;

import java.lang.reflect.Type;

/**
 *
 *
 * @author bing.fan
 * 2018-07-10 22:46
 */
public class TypeName {

    public static String of(Type t) {
        return t.getTypeName();
    }

    public static String of(Object object) {
        return object.getClass().getTypeName();
    }
}
