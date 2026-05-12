package com.uiys.skillforflink.level_1;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import static org.junit.jupiter.api.Assertions.*;

class SocketWordCountTest {

    public static void main(String[] args) {
        System.out.println("开始发送!");
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(9999)) {
                Socket socket = serverSocket.accept();
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                // 模拟发送几行数据给 Flink
                out.println("spark flink kafka");
                out.println("spark sqoop flink");
                out.println("nihc hadoop flink");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
        System.out.println("发送完成!");
    }


}