/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hortonworks.hwc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author Utilizador
 */
public interface HiveWarehouseSession {

    //Execute Hive SELECT query and return DataFrame
    Dataset<Row> executeQuery(String sql);
//Execute Hive update statement

    boolean executeUpdate(String sql);
//Execute Hive catalog-browsing operation and return DataFrame

    Dataset<Row> execute(String sql);
//Reference a Hive table as a DataFrame

    Dataset<Row> table(String sql);

    //Return the SparkSession attached to this HiveWarehouseSession
    SparkSession session();
//Set the current database for unqualified Hive table references

    void setDatabase(String name);

    /**
     * Helpers: wrapper functions over execute or executeUpdate
     */
//Helper for show databases
    Dataset<Row> showDatabases();
//Helper for show tables

    Dataset<Row> showTables();
//Helper for describeTable

    Dataset<Row> describeTable(String table);
//Helper for create database

    void createDatabase(String database, boolean ifNotExists);
//Helper for create table stored as ORC

    CreateTableBuilder createTable(String tableName);
//Helper for drop database

    void dropDatabase(String database, boolean ifExists, boolean cascade);
//Helper for drop table

    void dropTable(String table, boolean ifExists, boolean purge);

}
