package com.vonzhou.learn.javase;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class PageFilterDemo extends Base {

    public static void main(String[] args) throws Exception {
        Table table = getTable();

        List<Put> puts = Lists.newArrayList();
        Put put = new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
        puts.add(put);

        put = new Put(Bytes.toBytes("row2"));
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name"), Bytes.toBytes("zhanghua"));
        puts.add(put);


        put = new Put(Bytes.toBytes("row3"));
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
        puts.add(put);

        put = new Put(Bytes.toBytes("row4"));
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name"), Bytes.toBytes("wangermazi"));
        puts.add(put);

        put = new Put(Bytes.toBytes("row5"));
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name"), Bytes.toBytes("zhang"));
        puts.add(put);


        table.put(puts);
        System.out.println("批量PUT成功！");

        Filter filter = new PageFilter(2L);
        Scan scan = new Scan();
        scan.setFilter(filter);

        ResultScanner rs = table.getScanner(scan);
        // 第一页
        byte[] lastRowKey = printOngPage(rs);
        rs.close();

        System.out.println("================== page 2==================");
        // 为什么拼接一个0字节？？
        byte[] startRowKey = Bytes.add(lastRowKey, new byte[1]);
        scan.setStartRow(startRowKey);
        rs = table.getScanner(scan);
        printOngPage(rs);


        rs.close();
    }

}
