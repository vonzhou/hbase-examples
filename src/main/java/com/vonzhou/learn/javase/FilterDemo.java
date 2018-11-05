package com.vonzhou.learn.javase;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class FilterDemo extends Base {

    public static void main(String[] args) throws Exception {
        Table table = getTable();

        List<Put> puts = Lists.newArrayList();
        Put put = new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("teacher"), Bytes.toBytes("vonzhou"));
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("age"), Bytes.toBytes(18L));
        puts.add(put);

        put = new Put(Bytes.toBytes("row2"));
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name"), Bytes.toBytes("zhanghua"));
        puts.add(put);


        put = new Put(Bytes.toBytes("row3"));
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("teacher"), Bytes.toBytes("Dr.zhang"));
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("age"), Bytes.toBytes(18L));
        puts.add(put);

        put = new Put(Bytes.toBytes("row4"));
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name"), Bytes.toBytes("wangermazi"));
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("teacher"), Bytes.toBytes("daye"));
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("age"), Bytes.toBytes(28L));
        puts.add(put);

        put = new Put(Bytes.toBytes("row5"));
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name"), Bytes.toBytes("zhang"));
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("teacher"), Bytes.toBytes("vonzhou"));
        put.addColumn(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("age"), Bytes.toBytes(18L));
        puts.add(put);


        table.put(puts);
        System.out.println("批量PUT成功！");


        // ValueFilter 会比较所有的列
        Scan scan = new Scan();
        Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("zhang"));
        scan.setFilter(filter);
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            String row = Bytes.toString(r.getRow());
            String name = Bytes.toString(r.getValue(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name")));
            System.out.println(row + ": " + name);
        }

        System.out.println("=====================================");
        // 指定具体的列过滤
        filter = new SingleColumnValueFilter(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("teacher"),
                CompareFilter.CompareOp.EQUAL, new SubstringComparator("vonzhou"));
        scan.setFilter(filter);
        rs = table.getScanner(scan);
        for (Result r : rs) {
            String row = Bytes.toString(r.getRow());
            String name = Bytes.toString(r.getValue(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("teacher")));
            System.out.println(row + ": " + name);
        }
        System.out.println("===========================================");

        // 等值运算符
        filter = new SingleColumnValueFilter(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("zhang")));
        scan.setFilter(filter);
        rs = table.getScanner(scan);
        for (Result r : rs) {
            String row = Bytes.toString(r.getRow());
            String name = Bytes.toString(r.getValue(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name")));
            System.out.println(row + ": " + name);
        }
        System.out.println("=============================================");

        // >运算符，字典序
        filter = new SingleColumnValueFilter(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name"),
                CompareFilter.CompareOp.GREATER, new BinaryComparator(Bytes.toBytes("zhang")));
        scan.setFilter(filter);
        rs = table.getScanner(scan);
        for (Result r : rs) {
            String row = Bytes.toString(r.getRow());
            String name = Bytes.toString(r.getValue(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name")));
            System.out.println(row + ": " + name);
        }

        /**
         *  数字比较
         row1: zhangsan
         row2: zhanghua
         row2: zhanghua
         row4: wangermazi
         * 为何row1 会出现？
         * row2 有2个列不存在，所以出现了2次？
          */

        filter = new SingleColumnValueFilter(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("age"),
                CompareFilter.CompareOp.GREATER, new BinaryComparator(Bytes.toBytes(20L)));
        scan.setFilter(filter);
        rs = table.getScanner(scan);
        for (Result r : rs) {
            String row = Bytes.toString(r.getRow());
            String name = Bytes.toString(r.getValue(Bytes.toBytes(COL_FAMILY), Bytes.toBytes("name")));
            System.out.println(row + ": " + name);
        }

        rs.close();

    }

}
