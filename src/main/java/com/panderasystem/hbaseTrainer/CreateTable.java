package com.panderasystem.hbaseTrainer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class CreateTable {
	public CreateTable(){
		
	}
	
	public void newTable(String tableName, String[] columnFamilies) throws IOException{
		Configuration config = HBaseConfiguration.create();
		
		HBaseAdmin admin = new HBaseAdmin(config);
		if(admin.tableExists(tableName)){
			System.out.println(tableName + "Already exists");
		}else{
		
			HTableDescriptor tblDesc = new HTableDescriptor(TableName.valueOf(tableName));
			
			for(int i = 0; i < columnFamilies.length; i ++){
				tblDesc.addFamily(new HColumnDescriptor(columnFamilies[i]));
			}
			
			admin.createTable(tblDesc);
			System.out.println("Created table: "+ tableName + " - ok.");
		}
	}
}
