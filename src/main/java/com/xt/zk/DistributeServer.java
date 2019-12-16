package com.xt.zk;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @author xt
 * @create 2019/12/16 12:14
 * @Desc
 */
public class DistributeServer {

    private String connectString = "hadoop101:2181,hadoop102:2181,hadoop103:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;

    // 创建到zk的客户端连接
    private void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    // 注册
    private void regist(String hostname) throws Exception {
        String path = zkClient.create("/servers/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + " is online " + path);
    }

    // 业务功能
    private void business(String hostname) throws Exception {
        System.out.println(hostname + " is working...");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {
        DistributeServer server = new DistributeServer();

        // 1. 连接 zookeeper 集群
        server.getConnect();

        // 2. 注册节点
        server.regist(args[0]);

        // 3. 业务逻辑处理
        server.business(args[0]);
    }

}
