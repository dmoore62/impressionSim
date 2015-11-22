package com.panderasystem.hbaseTrainer;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class TablePopulator {
	private final int NUMBER_OF_DMAS = 211;
	private final int NUMBER_OF_ZIPS = 40000;
	private final int NUMBER_OF_DAYS_IN_WEEK = 7;
	private final int NUMBER_OF_WEEKS_IN_YEAR = 52;
	private final int CAMPAIGN_LENGTH_IN_DAYS = 90;
	
	private Random rand;
	private String tableName;
	private int campaignId;
	private String agencyName;
	private String branch;
	
	public TablePopulator(String tableName){
		this.rand = new Random();
		this.tableName = tableName;
		this.campaignId = 1;
		this.agencyName = "Acme";
		this.branch = "Demo Branch";
	}
	
	public void simulateImpressionData() throws IOException{
		Configuration config = HBaseConfiguration.create();
		int randomDataPoint;
		int randomImpressionDataPoint;
		
		HTable table = new HTable(config, this.tableName);
		
		for(int dayCounter = 1; dayCounter <= CAMPAIGN_LENGTH_IN_DAYS; dayCounter ++){
			for(int zipCounter = 1; zipCounter <= NUMBER_OF_ZIPS; zipCounter ++){
				int dma = this.getDmaFromZip(zipCounter);
				int dayOfWeekId = this.getDayOfWeek(dayCounter);
				int weekId = this.getWeekOfYear(dayCounter);
				randomImpressionDataPoint = this.getRandomNumberOverRange(2, 3, this.rand);
				randomDataPoint = this.getRandomNumberOverRange(100, 1000, this.rand);
				String rowKey = this.buildRowKey(this.campaignId, dayCounter, zipCounter, dma);
				System.out.println("ROWKEY: "+ rowKey);
				System.out.println("IMPRESSIONS: "+ randomImpressionDataPoint);
				System.out.println("Data point: "+ randomDataPoint);
				
				try{
					Put put = new Put(Bytes.toBytes(rowKey));
					put.add(Bytes.toBytes("GeoDim"), Bytes.toBytes("zip"), Bytes.toBytes(zipCounter));
					put.add(Bytes.toBytes("GeoDim"), Bytes.toBytes("dma"), Bytes.toBytes(dma));
					put.add(Bytes.toBytes("CampaignDim"), Bytes.toBytes("campaign"), Bytes.toBytes(this.campaignId));
					put.add(Bytes.toBytes("CampaignDim"), Bytes.toBytes("agency"), Bytes.toBytes(this.agencyName));
					put.add(Bytes.toBytes("CampaignDim"), Bytes.toBytes("branch"), Bytes.toBytes(this.branch));
					put.add(Bytes.toBytes("DateDim"), Bytes.toBytes("dateid"), Bytes.toBytes(dayCounter));
					put.add(Bytes.toBytes("DateDim"), Bytes.toBytes("dayid"), Bytes.toBytes(dayOfWeekId));
					put.add(Bytes.toBytes("DateDim"), Bytes.toBytes("weekid"), Bytes.toBytes(weekId));
					put.add(Bytes.toBytes("AttrDim"), Bytes.toBytes("impressionCount"), Bytes.toBytes(randomImpressionDataPoint));
					put.add(Bytes.toBytes("AttrDim"), Bytes.toBytes("impressionMetric"), Bytes.toBytes(randomDataPoint));
					
					table.put(put);
					System.out.println("Insert "+ rowKey + "ok.");
				}catch(Exception e){
					System.out.println("Insert error with "+ rowKey);
					e.printStackTrace();
				}
			}
		}
	}
	
	private int getDmaFromZip(int zip){
		return zip / NUMBER_OF_DMAS;
	}
	
	private int getDayOfWeek(int day){
		return day % NUMBER_OF_DAYS_IN_WEEK;
	}
	
	private int getWeekOfYear(int day){
		return day / NUMBER_OF_DAYS_IN_WEEK;
	}
	
	private int getRandomNumberOverRange(int min, int max, Random rand){
		int random = rand.nextInt((max-min) + 1) + min;
		return random;
	}
	
	private String buildRowKey(int campaignId, int dayId, int zip, int dma){
		return String.valueOf(campaignId) + "-" +  String.valueOf(dayId) + "-" + String.valueOf(zip) + "-" +String.valueOf(dma);
	}
}
