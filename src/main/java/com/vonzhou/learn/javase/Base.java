package com.vonzhou.learn.javase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class Base {
    public final static String TABLE_NAME = "test";
    public final static String COL_FAMILY = "cf";

    public static Table getTable() throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, "ubuntu");
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        config.set(HConstants.HBASE_DIR, "hdfs://ubuntu:8020/hbase");
        HBaseAdmin.checkHBaseAvailable(config);

        Connection connection = ConnectionFactory.createConnection(config);

        TableName tableName = TableName.valueOf(TABLE_NAME);
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        tableDescriptor.addFamily(new HColumnDescriptor(COL_FAMILY));
        Admin admin = connection.getAdmin();
        createOrOverride(admin, tableDescriptor);
        System.out.println("表创建成功");

        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        return table;
    }

    public static void createOrOverride(Admin admin, HTableDescriptor table) throws Exception {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }

        admin.createTable(table);
    }

    public static byte[] printOngPage(ResultScanner rs) {
        byte[] lastRowKey = null;
        for (Result r : rs) {
            byte[] rk = r.getRow();
            String row = Bytes.toString(rk);
            String name = Bytes.toString(r.getValue(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name")));
            System.out.println(row + ": " + name);
            lastRowKey = rk;
        }

        return lastRowKey;
    }
}
