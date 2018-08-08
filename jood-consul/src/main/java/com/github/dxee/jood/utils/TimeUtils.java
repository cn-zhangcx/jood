package com.github.dxee.jood.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Time utils
 *
 * @author bing.fan
 * 2018-08-02 14:51
 */
public class TimeUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeUtils.class);

    public static void sleep(long sleepInMillis) {
        try {
            Thread.sleep(sleepInMillis);
        } catch (Exception e) {
            LOGGER.error("sleep error", e);
        }
    }
}
