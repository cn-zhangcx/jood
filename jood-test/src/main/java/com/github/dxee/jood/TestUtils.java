package com.github.dxee.jood;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * TestUtils
 *
 * @author bing.fan
 * 2018-07-11 23:48
 */
public class TestUtils {

    public static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot find available port: "
                    + e.getMessage(), e);
        }
    }

    public static int freePort(int... excludePort) {
        int port = freePort();

        boolean flag = false;
        for (int i = 0; i < excludePort.length; i++) {
            if (port == excludePort[i]) {
                flag = true;
            }
        }

        if (!flag) {
            return port;
        }

        return freePort(excludePort);
    }

}
