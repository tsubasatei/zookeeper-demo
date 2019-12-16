package com.xt.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xt
 * @create 2019/12/16 12:55
 * @Desc
 */
public class DistributeClient {

    private String connectString = "hadoop101:2181,hadoop102:2181,hadoop103:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;
    private String parentNode = "/servers";

    // 创建到zk的客户端连接
    private void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    getChildren();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    // 获取服务器列表信息
    public void getChildren() throws KeeperException, InterruptedException {

        // 1. 获取服务器子节点信息，并且对父节点进行监听
        List<String> children = zkClient.getChildren(parentNode, true);

        // 2. 存储服务器信息列表
        List<String> servers = new ArrayList<>();

        // 3. 遍历所有节点，获取节点中的主机名称信息
        for (String child : children) {
            byte[] data = zkClient.getData(parentNode + "/" + child, false, null);
            servers.add(new String(data));
        }

        // 4. 打印服务器列表信息
        System.out.println(servers);
    }

    // 业务功能
    private void business() throws InterruptedException {
        System.out.println("client is working...");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DistributeClient client = new DistributeClient();

        // 1. 获取 zk 连接
        client.getConnect();

        // 2. 获取 servers 的子节点信息，从中获取服务器信息列表
        client.getChildren();

        // 3. 业务进程启动
        client.business();
    }


}
