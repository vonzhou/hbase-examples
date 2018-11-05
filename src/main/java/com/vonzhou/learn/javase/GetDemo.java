package com.vonzhou.learn.javase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class GetDemo {
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
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name"), Bytes.toBytes("von"))
                .addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("age"), Bytes.toBytes("11"));
        table.put(put);


        Get get = new Get(Bytes.toBytes("row1"));
        Result res = table.get(get);
        System.out.println(res.size());
        System.out.println(res.isEmpty());
        System.out.println(Bytes.toString(res.value()));
        System.out.println(Bytes.toString(res.getValue(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name"))));


        List<Cell> cellList = res.getColumnCells(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name"));
        System.out.println(Bytes.toString(CellUtil.cloneValue(cellList.get(0))));


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
