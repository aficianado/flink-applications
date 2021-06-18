package com.chaoppo.flink.app.flinkeventhub.store;

import com.chaoppo.flink.app.flinkeventhub.util.HBaseUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.eventhubs.spout.IStateStore;

import java.io.IOException;

public class HbaseStateStore implements IStateStore {

    public static final byte[] d = "d".getBytes();
    /**
     *
     */
    private static final long serialVersionUID = -2244822122340006845L;
    //    private static final Logger logger = LoggerFactory.getLogger(HbaseStateStore.class);
    private final byte[] rowKey;
    private final String tableName;
    private org.apache.hadoop.hbase.client.Table table;

    public HbaseStateStore(String topologyName, String tableName) {
        this.rowKey = Bytes.toBytes(String.format("storm:%s:control", topologyName));
        this.tableName = tableName;
    }

    public static void main(String[] args) {
        HbaseStateStore hbase = new HbaseStateStore("IngestTopology", "CONTROL");
        hbase.open();
        String offset = hbase.readData("/eventhubspout/VtIngest/inbound01/vteventhub02/partitions/8");
        System.out.println(offset);
    }

    @Override
    public void open() {
        try {
            HBaseUtil.init();
            table = HBaseUtil.getTable(tableName);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public String readData(String statePath) {
        try {
            int partitionLocation = statePath.indexOf("partitions/") + 11;
            String partition = statePath.substring(partitionLocation, statePath.length());
            if (partition.length() == 1) {
                partition = String.format("0%s", partition);
            }
            String column = String.format("p:%s", partition);

            Get get = new Get(rowKey);
            Result r = table.get(get);
            byte[] offset = r.getValue(d, Bytes.toBytes(String.format("%s:xOptOffset", column)));

            if (offset != null) {
                String data = Bytes.toString(offset);
                System.out.println(String.format("data was retrieved. path: %s, data: %s.", statePath, data));

                return data;
            } else {
                // do we want to throw an exception if no offset was found?
                return null;
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            table.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void saveData(String statePath, String data) {
        data = data == null ? "" : data;
        try {
            int partitionLocation = statePath.indexOf("partitions/") + 11;
            String partition = statePath.substring(partitionLocation, statePath.length());
            if (partition.length() == 1) {
                partition = String.format("0%s", partition);
            }
            String column = String.format("p:%s", partition);

            Put put = new Put(rowKey);
            put.addColumn(d, Bytes.toBytes(String.format("%s:id", column)), Bytes.toBytes(partition));
            put.addColumn(d, Bytes.toBytes(String.format("%s:xOptOffset1", column)), Bytes.toBytes(data));

            table.put(put);

            System.out.println(String.format("data was saved. path: %s, data: %s.", statePath, data));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}