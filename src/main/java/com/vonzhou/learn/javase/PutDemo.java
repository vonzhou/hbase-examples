package com.vonzhou.learn.javase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

public class PutDemo {
    private final static String TABLE_NAME = "test";
    private final static String COL_FAMILY = "cf";

    public static void main(String[] args) throws Exception {
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
        Put put = new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name"), Bytes.toBytes("vonzhou"));
        table.put(put);


        // 语法糖：级联操作
        Put put2 = new Put(Bytes.toBytes("row2"));

        put2.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name"), Bytes.toBytes("vonzhou"))
                .addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("age"), Bytes.toBytes("12"))
                .addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("class"), Bytes.toBytes("A"));
        table.put(put2);

        // check and put
        put = new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name"), Bytes.toBytes("vonzhouXXXX"));
        boolean res = table.checkAndPut(Bytes.toBytes("row1"),
                Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name"), Bytes.toBytes("vonzhou"),
                put);
        System.out.println(String.format("checkAndPut %s", res ? "成功" : "失败"));


        // 假设我们之前读取的row1 = vonzhou, 此时已经被改为了 vonzhouXXXX，再次修改会失败
        put = new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name"), Bytes.toBytes("vonzhouYYY"));
        res = table.checkAndPut(Bytes.toBytes("row1"),
                Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name"), Bytes.toBytes("vonzhou"),
                put);
        System.out.println(String.format("checkAndPut %s", res ? "成功" : "失败"));


        // 检查条件，然后put
        put = new Put(Bytes.toBytes("row2"));
        put.addColumn(Bytes.toBytes(COL_FAMILY),
                Bytes.toBytes("name"),
                Bytes.toBytes("eastwind"));
        res = table.checkAndPut(Bytes.toBytes("row2"),
                Bytes.toBytes(COL_FAMILY),
                Bytes.toBytes("age"),
                CompareFilter.CompareOp.LESS,
                Bytes.toBytes("1"), // 我们传入的 “1” 如果比之前的 age 小的话，就执行put
                put);
        System.out.println(String.format("checkAndPut %s", res ? "成功" : "失败"));


        connection.close();
    }

    public static void createOrOverride(Admin admin, HTableDescriptor table) throws Exception {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }

        admin.createTable(table);
    }

}
