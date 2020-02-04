package com.bliu;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MyHBaseClient {
    Connection conn;
    Admin admin;


    public static void main(String[] args) {
        Logger.getRootLogger().setLevel(Level.DEBUG);

        MyHBaseClient myHBaseClient = new MyHBaseClient();
        myHBaseClient.init_connection();

        myHBaseClient.listTable();

        myHBaseClient.addTable("MyTable", new String[]{"MyColumnFamily"});
        myHBaseClient.listTable();

        myHBaseClient.addDataRow("MyTable", "1", "MyColumnFamily", new String[]{"column1"}, new String[]{"value1"});
        Result result = myHBaseClient.getRow("MyTable", "MyColumnFamily", "1", new String[]{"column1"});
        myHBaseClient.deleteDataRows("MyTable", new String[]{"1"});

        myHBaseClient.dropTable("MyTable");
        myHBaseClient.listTable();

        myHBaseClient.destroy();

    }


    private void init_connection() {
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();

        // Zookeeper quorum
        conf.set("hbase.zookeeper.quorum", "18.177.150.178");
        conf.set("hbase.zookeeper.property.clientPort","2181");

        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hbase.security.authentication", "kerberos");

        conf.set("hbase.cluster.distributed", "true");

        // check this setting on HBase side
        conf.set("hbase.rpc.protection", "authentication");

        // what principal the master/region. servers use.
        conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@FIELD.HORTONWORKS.COM");
        conf.set("hbase.regionserver.keytab.file", "src/hbase.service.keytab");

        // this is needed even if you connect over rpc/zookeeper
        conf.set("hbase.master.kerberos.principal", "hbase/_HOST@FIELD.HORTONWORKS.COM");
        conf.set("hbase.master.keytab.file", "src/hbase.service.keytab");

        System.setProperty("java.security.krb5.conf","src/krb5.conf");
        // Enable/disable krb5 debugging
        System.setProperty("sun.security.krb5.debug", "false");

        String principal = System.getProperty("kerberosPrincipal","hbase/hdp1.field.hortonworks.com@FIELD.HORTONWORKS.COM");
        String keytabLocation = System.getProperty("kerberosKeytab","src/hbase.service.keytab");

        try {
            // kinit with principal and keytab
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(principal, keytabLocation);

            this.conn = ConnectionFactory.createConnection(conf);
            this.admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void destroy() {
        try {
            this.admin.close();
            this.conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void listTable() {
        TableName[] tableNames;
        try {
            tableNames = this.admin.listTableNames();
            for (TableName tableName: tableNames) {
                System.out.println(tableName.getNameAsString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void addTable(String table, String[] columnFamilies){
        try {
            if (!this.admin.isTableAvailable(TableName.valueOf(table))) {
                TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(table));
                for (String familyName : columnFamilies) {
                    tableDescriptor.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(familyName)).build());
                }
                admin.createTable(tableDescriptor.build());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void addDataRow(String tableName, String rowKey, String family, String[] columns, String[] values) {
        try {
            Table table = this.conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.setDurability(Durability.SKIP_WAL);
            ColumnFamilyDescriptor[] columnFamilyDescriptors = table.getDescriptor().getColumnFamilies();
            for (ColumnFamilyDescriptor columnFamilyDescriptor : columnFamilyDescriptors) {
                String familyName = columnFamilyDescriptor.getNameAsString();
                if (familyName.equals(family)) {
                    for (int i = 0; i < columns.length; i++) {
                        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
                    }
                }
            }
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Result getRow(String tableName, String familyName, String rowKey, String[] columns) {
        try {
            Table table = this.conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            //指定列
            if (null != columns && columns.length > 0) {
                for (String column : columns) {
                    get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column));
                }
            }
            return table.get(get);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private void deleteDataRows(String tableName, String[] rowKeys) {
        try {
            Table table = this.conn.getTable(TableName.valueOf(tableName));
            List<Delete> deleteList = new ArrayList<>(rowKeys.length);
            Delete delete;
            for (String rowKey : rowKeys) {
                delete = new Delete(Bytes.toBytes(rowKey));
                deleteList.add(delete);
            }
            table.delete(deleteList);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void dropTable(String table) {
        try {
            if (!this.admin.isTableAvailable(TableName.valueOf(table))) {
                admin.disableTable(TableName.valueOf(table));
                admin.deleteTable(TableName.valueOf(table));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
