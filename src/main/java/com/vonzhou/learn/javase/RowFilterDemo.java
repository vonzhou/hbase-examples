package com.vonzhou.learn.javase;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.List;

public class RowFilterDemo extends Base {

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

        // 普通行过滤器
        Filter rowFilter = new RowFilter(CompareFilter.CompareOp.GREATER,
                new BinaryComparator(Bytes.toBytes("row4")));

        Scan scan = new Scan();
        scan.setFilter(rowFilter);

        ResultScanner rs = table.getScanner(scan);
        // 第一页
        byte[] lastRowKey = printOngPage(rs);
        rs.close();


        System.out.println("==============MultiRowRangeFilter===========");
        // 多行范围过滤器
        List<MultiRowRangeFilter.RowRange> rowRanges = Arrays.asList(
                new MultiRowRangeFilter.RowRange("row1", true, "row3", false),
                new MultiRowRangeFilter.RowRange("row5", true, "row7", true)
        );
        scan.setFilter(new MultiRowRangeFilter(rowRanges));
        rs = table.getScanner(scan);
        printOngPage(rs);
        rs.close();

        System.out.println("====================PrefixFilter==================");
        // 行前缀过滤器
        PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes("row"));
        scan.setFilter(prefixFilter);
        rs = table.getScanner(scan);
        printOngPage(rs);
        rs.close();


    }


}
