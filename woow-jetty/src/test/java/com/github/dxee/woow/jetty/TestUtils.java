package com.github.dxee.woow.jetty;

import java.net.*;
import java.util.Random;

/**
 * TestUtils
 *
 * @author bing.fan
 * 2018-07-12 12:03
 */
public class TestUtils {

    public static int random(int min, int max) {
        Random random = new Random();
        int randomInt = random.nextInt(max) % (max - min + 1) + min;
        return randomInt;
    }

    public static int freePort() {
        ServerSocket tmp;
        for (int i = 65536; i > 5001; i--) {
            try {
                tmp = new ServerSocket(i);
                tmp.close();
                return i;
            } catch (Exception e) {
                // Do nothing
            }
        }
        return random(5001, 65536);
    }
}
