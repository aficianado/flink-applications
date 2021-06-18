package com.chaoppo.flink.app.flinkeventhub.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public class HBaseUtil {

    // column family 'a'rchive
    public static final byte[] CF_A = Bytes.toBytes("a");
    // column family 'd'ata
    public static final byte[] CF_D = Bytes.toBytes("d");
    static Logger log = LoggerFactory.getLogger(HBaseUtil.class);
    // private static final String pid =
    static String principal = System.getProperty("kerberosPrincipal",
            "hbase/ddtmeutelc3sl01.azure-dev-eastus.us164.corpintra.net@KDCDEVEAST.AZURE-DEV-EASTUS.US164.CORPINTRA.NET");
    static String keytabLocation = System.getProperty("kerberosKeytab", "/etc/security/keytabs/hbase.service.keytab");
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss.SSS");
    static boolean initialized = false;
    // this will become deprecated in HBASE .98
    private static Admin admin;
    private static Connection connection = null;
    private static Configuration config = null;

    public static Connection getConnection() {
        return connection;
    }

    public static Admin getAdmin() {
        return admin;
    }

    public static String getArchiveKey(Object o, long ts, int instanceId) {
        Date now = new Date(ts);
        return String.format("%s:%d:%s", o.getClass().getSimpleName(), instanceId, sdf.format(now));
    }

    /*
     * static public String getArchiveKey(long inTs, String name) { String
     * archiveKey = null; archiveKey = String.format("%d:%s", Long.MAX_VALUE -
     * inTs, name); return archiveKey; }
     */

    public static org.apache.hadoop.hbase.client.Table getTable(String table) throws IOException {
        return connection.getTable(TableName.valueOf(table));
    }

    public static String[] listTables() throws IOException {
        TableName[] tables = admin.listTableNames();
        String[] names = new String[tables.length];
        for (int i = 0; i < tables.length; ++i) {
            TableName name = tables[i];
            names[i] = name.getNameAsString();
        }
        return names;
    }

    // FIXME - refactor to take a properties file if offered
    public static void init() throws Exception {
        if (!initialized) {
            log.info("HBaseUtil.init - config");
            // -
            // http://stackoverflow.com/questions/29482803/whats-the-best-way-to-get-htable-handle-of-hbase
            Configuration config = HBaseConfiguration.create();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            Writer writer = new OutputStreamWriter(bos);
            Configuration.dumpConfiguration(config, writer);
            writer.flush();
            bos.flush();
            log.debug(new String(bos.toByteArray()));

            //            String hostname = SystemInfo.getFQDNHostName();
            // String json = "";
            boolean isKerborized = false;

            // config.set("hbase.zookeeper.quorum",
            // "node1.us164.corpintra.net");
            // config.set("hbase.zookeeper.quorum", "devnode01.cloudapp.net");
            config.set("hbase.zookeeper.quorum", "zookeeper");
            config.set("hbase.zookeeper.property.clientPort", "2181");

            // config.set("hbase.master", "node8.us164.corpintra.net");
            // config.set("hbase.master", "devnode01.cloudapp.net");
            config.set("hbase.master", "hbase");
            config.set("hbase.master.port", "16000");

            // necessary hack !
            // http://2scompliment.wordpress.com/2013/12/11/running-hbase-java-applications-on-hortonworks-hadoop-sandbox-2-x-with-yarn/
            config.set("zookeeper.znode.parent", "/hbase-unsecure");
            init(config);
            initialized = true;
        }
    }

    @SuppressWarnings("deprecation")
    public static synchronized void init(Configuration initConfig) throws IOException {
        config = initConfig;
        //connection = HConnectionManager.createConnection(config);
        connection = ConnectionFactory.createConnection(config);
        admin = connection.getAdmin();

        log.info("HBaseUtil initialized connection and admin");
    }

    public static Configuration getConfig() {
        return config;
    }

    public static void main(String[] args) {

    }

    public static Put add(Put put, String columnName, Object value) {
        if (value == null) {
            return put;
        }
        return add(put, columnName, value.toString());
    }

    @SuppressWarnings("deprecation")
    public static Put add(Put put, String columnName, String value) {
        if (value == null) {
            return put;
        }
        //put.addImmutable(CF_D, Bytes.toBytes(columnName), Bytes.toBytes(value.trim()));
        put.addColumn(CF_D, Bytes.toBytes(columnName), Bytes.toBytes(value.trim()));
        return put;
    }

    /*
     * static public Put put(String rowKey, String columnName, byte[] data){
     *
     * Put put = new Put(rowKey.getBytes());
     *
     * if (data == null){ return put; }
     *
     * put.add(CF_D, Bytes.toBytes(columnName), data); return put; }
     */

    public static Map<String, Object> addObject(Map<String, Object> record, String columnName, Object value) {
        if (value == null) {
            return record;
        }

        record.put(columnName, value.toString().trim());
        return record;
    }

    public static Map<String, String> add(Map<String, String> record, String columnName, Object value) {
        if (value == null) {
            return record;
        }

        record.put(columnName, value.toString().trim());

        return record;
    }

    public static Integer getInteger(Map<String, String> record, String colName) {
        if (record.containsKey(colName)) {
            return Integer.parseInt((String) record.get(colName));
        } else {
            return null;
        }
    }

    public static Double getDouble(Map<String, String> record, String colName) {
        if (record.containsKey(colName)) {
            return Double.parseDouble((String) record.get(colName));
        } else {
            return null;
        }
    }

    public static byte[] getByteArray(Map<String, String> record, String colName) {
        if (record.containsKey(colName)) {
            return null; // FIXME !!! (byte[])record.get(colName);
        } else {
            return null;
        }
    }

    public static Map<String, String> copy(Map<String, String> copyTo, Map<String, String> copyFrom, String column) {
        return add(copyTo, column, get(copyFrom, column));
    }

    public static String get(Map<String, String> record, String colName) {
        if (record.containsKey(colName)) {
            return (String) record.get(colName);
        } else {
            return null;
        }
    }

    public static BigDecimal getOrCreate(Map<String, String> record, String colName, BigDecimal value) {
        String ret = get(record, colName);
        if (ret == null) {
            record.put(colName, value.toString());
            return value;
        } else {
            return new BigDecimal(ret);
        }
    }

    public static BigDecimal getOrCreate(HashMap<String, Object> record, String col, BigDecimal value) {
        if (record.containsKey(col)) {
            return (BigDecimal) record.get(col);
        } else {
            record.put(col, value);
            return value;
        }

    }

    public static Object add(HashMap<String, Object> sensor, String col, Object value) {
        if (value == null || col == null) {
            return null;
        }
        return sensor.put(col, value);
    }

    public static Object add(HashMap<String, Object> sensor, String col, BigDecimal value) {
        if (value == null || col == null) {
            return null;
        }
        return sensor.put(col, value);
    }

    public static List<Map<String, String>> fromJsonStringToListOfRecords(String json) {
        JsonParser parser = new JsonParser();
        List<Map<String, String>> records = new ArrayList<Map<String, String>>();

        JsonArray jArray = parser.parse(json).getAsJsonArray();
        for (JsonElement child : jArray) {
            HashMap<String, String> obj = new HashMap<>();
            for (Entry<String, JsonElement> valueEntry : ((JsonObject) child).entrySet()) {
                obj.put(valueEntry.getKey(), valueEntry.getValue().getAsString());
            }
            records.add(obj);
        }
        return records;
    }

    public static List<Map<String, String>> fromJsonArrayToListOfRecords(JsonArray json) {

        List<Map<String, String>> records = new ArrayList<Map<String, String>>();

        for (JsonElement child : json) {
            HashMap<String, String> obj = new HashMap<String, String>();
            for (Entry<String, JsonElement> valueEntry : ((JsonObject) child).entrySet()) {
                obj.put(valueEntry.getKey(), valueEntry.getValue().getAsString());
            }
            records.add(obj);
        }
        return records;
    }

}