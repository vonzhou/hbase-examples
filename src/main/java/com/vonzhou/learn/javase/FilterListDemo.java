package com.vonzhou.learn.javase;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class FilterListDemo extends Base {

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

        List<Filter> filters = Lists.newArrayList();
        Filter nameFilter = new SingleColumnValueFilter(Bytes.toBytes(COL_FAMILY),
                Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("zhang"));

        Filter pageFilter = new PageFilter(2L);

        filters.add(nameFilter);
        filters.add(pageFilter);


        FilterList filterList = new FilterList(filters);


        Scan scan = new Scan();
        scan.setFilter(filterList);

        ResultScanner rs = table.getScanner(scan);
        // 第一页
        byte[] lastRowKey = printOngPage(rs);
        rs.close();
    }


}
