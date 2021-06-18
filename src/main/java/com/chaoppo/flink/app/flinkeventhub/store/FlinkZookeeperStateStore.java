package com.chaoppo.flink.app.flinkeventhub.store;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.storm.eventhubs.spout.IStateStore;
import org.apache.storm.eventhubs.spout.ZookeeperStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FlinkZookeeperStateStore implements IStateStore {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperStateStore.class);
    private final String zookeeperConnectionString;
    private final CuratorFramework curatorFramework;

    public FlinkZookeeperStateStore(String zookeeperConnectionString) {
        this(zookeeperConnectionString, 3, 100);
    }

    public FlinkZookeeperStateStore(String connectionString, int retries, int retryInterval) {
        if (connectionString == null) {
            this.zookeeperConnectionString = "localhost:2181";
        } else {
            this.zookeeperConnectionString = connectionString;
        }

        RetryPolicy retryPolicy = new RetryNTimes(retries, retryInterval);
        this.curatorFramework = CuratorFrameworkFactory.newClient(this.zookeeperConnectionString, retryPolicy);
    }

    public void open() {
        this.curatorFramework.start();
    }

    public void close() {
        this.curatorFramework.close();
    }

    public void saveData(String statePath, String data) {
        data = data == null ? "" : data;
        byte[] bytes = data.getBytes();

        try {
            if (this.curatorFramework.checkExists().forPath(statePath) == null) {
                this.curatorFramework.create().creatingParentsIfNeeded().forPath(statePath, bytes);
            } else {
                this.curatorFramework.setData().forPath(statePath, bytes);
            }

            logger.info(String.format("data was saved. path: %s, data: %s.", statePath, data));
        } catch (Exception var5) {
            throw new RuntimeException(var5);
        }
    }

    public List<String> getChildrenBuilder(String path) {
        List<String> list = null;
        try {
            if (this.curatorFramework.checkExists().forPath(path) != null) {
                list = this.curatorFramework.getChildren().forPath(path);
            }
            logger.info(String.format("list of children paths : {}.", list));
        } catch (Exception var5) {
            throw new RuntimeException(var5);
        }
        return list;
    }

    public String readData(String statePath) {
        try {
            if (this.curatorFramework.checkExists().forPath(statePath) == null) {
                return null;
            } else {
                byte[] bytes = (byte[]) this.curatorFramework.getData().forPath(statePath);
                String data = new String(bytes);
                logger.info(String.format("data was retrieved. path: %s, data: %s.", statePath, data));
                return data;
            }
        } catch (Exception var4) {
            throw new RuntimeException(var4);
        }
    }

    //    private static final String SEPARATOR = "/";
    //    private transient ZookeeperStateStore zookeeperStateStore = null;
    //    private transient EventPosition eventPosition;
    //    public FlinkZookeeperStateStore(String zookeeperConnectionString) {
    //        this(zookeeperConnectionString, 3, 100);
    //    }
    //
    //    public FlinkZookeeperStateStore(EventPosition eventPosition) {
    //        this.eventPosition = eventPosition;
    //    }
    //
    //    public FlinkZookeeperStateStore(String connectionString, int retries, int retryInterval) {
    //        zookeeperStateStore = new ZookeeperStateStore(connectionString, retries, retryInterval);
    //    }
    //
    //    public ZookeeperStateStore getZookeeperStateStore() {
    //        return zookeeperStateStore;
    //    }
    //
    //    public EventPosition getEventPosition() {
    //        return eventPosition;
    //    }
    //

}