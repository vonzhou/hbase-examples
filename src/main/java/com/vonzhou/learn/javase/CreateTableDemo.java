package com.vonzhou.learn.javase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;

public class CreateTableDemo {
    private final static String TABLE_NAME = "test";
    private final static String COL_FAMILY = "cf";

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
//        config.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
//        config.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));
        config.set(HConstants.ZOOKEEPER_QUORUM, "ubuntu");
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        config.set(HConstants.HBASE_DIR, "hdfs://ubuntu:8020/hbase");
        HBaseAdmin.checkHBaseAvailable(config);
        Connection connection = ConnectionFactory.createConnection(config);
        TableName tableName = TableName.valueOf(TABLE_NAME);
        HTableDescriptor table = new HTableDescriptor(tableName);
        table.addFamily(new HColumnDescriptor(COL_FAMILY));
        Admin admin = connection.getAdmin();


        // 创建表
        createOrOverride(admin, table);
        System.out.println("表创建成功");
        System.in.read();

        // 设置列族属性
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(COL_FAMILY);
        columnDescriptor.setCompactionCompressionType(Compression.Algorithm.GZ);
        columnDescriptor.setMaxVersions(HConstants.ALL_VERSIONS);
        table.modifyFamily(columnDescriptor);
        System.out.println("列族属性修改成功");
        System.in.read();

        // 添加新的列族
        HColumnDescriptor newColFamily = new HColumnDescriptor("newcf");
        newColFamily.setCompactionCompressionType(Compression.Algorithm.GZ);
        newColFamily.setMaxVersions(HConstants.ALL_VERSIONS);
        admin.addColumn(tableName, newColFamily);
        System.out.println("新增列族成功");
        System.in.read();


        // 删除一个列族，删除前要先disable表
        admin.disableTable(tableName);
        admin.deleteColumn(tableName, "newcf".getBytes("UTF-8"));
        System.out.println("删除列族成功");
        System.in.read();

        // 删除表
        admin.deleteTable(tableName);
        System.out.println("表删除成功");

        admin.close();
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
