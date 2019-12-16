package com.xt.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Zookeeper 客户端
 */
public class TestZK {

    // 地址不要有空格
    private static String connectingString = "hadoop101:2181,hadoop102:2181,hadoop103:2181";
    private static int sessionTimeout = 2000;
    private ZooKeeper zkClient = null;

    // 创建 ZooKeeper 客户端
    @Before
    public void init () throws Exception {

        zkClient = new ZooKeeper(connectingString, sessionTimeout, watchedEvent -> {

            /*try {
                System.out.println("-----start-------");
                List<String> children = zkClient.getChildren("/", true);
                for (String child : children) {
                    System.out.println(child);
                }
                System.out.println("-------end-------");
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/

            // 收到事件通知后的回调函数（用户的业务逻辑）
//            System.out.println(watchedEvent.getType() + " -- " + watchedEvent.getPath());
        });
    }

    // 创建节点
    @Test
    public void createNode () throws KeeperException, InterruptedException {
        /**
         * 参数1：要创建的节点的路径；
         * 参数2：节点数据 ；
         * 参数3：节点权限 ；
         * 参数4：节点的类型
         */
        String path = zkClient.create("/sanae", "captain".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("path = " + path);
    }

    // 获取子节点并监控节点的变化
    @Test
    public void getDataAndWatch () throws Exception {
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }

        Thread.sleep(Integer.MAX_VALUE);
    }

    // 判断 znode 是否存在
    @Test
    public void exists () throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/sanae", false);
        System.out.println(stat == null ? "not exits" : "exits");
    }
}
