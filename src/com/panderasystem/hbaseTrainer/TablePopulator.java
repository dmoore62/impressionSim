package com.panderasystem.hbaseTrainer;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class TablePopulator {
	private final int NUMBER_OF_DMAS = 211;
	private final int NUMBER_OF_ZIPS = 200;
	private final int NUMBER_OF_DAYS_IN_WEEK = 7;
	private final int NUMBER_OF_WEEKS_IN_YEAR = 52;
	private final int CAMPAIGN_LENGTH_IN_DAYS = 90;
	private final long MILLIS_IN_DAY = 86400;
	
	private Random rand;
	private String tableName;
	private int campaignId;
	private String agencyName;
	private String branch;
	private long startEpochTime;
	
	public TablePopulator(String tableName){
		this.rand = new Random();
		this.tableName = tableName;
		this.campaignId = 1;
		this.agencyName = "Acme";
		this.branch = "Demo Branch";
		this.startEpochTime = 1448841600;
	}
	
	public void simulateImpressionData() throws IOException{
		Configuration config = HBaseConfiguration.create();
		long randomDataPoint;
		long randomImpressionDataPoint;
		
		HTable table = new HTable(config, this.tableName);
		
		for(int dayCounter = 1; dayCounter <= CAMPAIGN_LENGTH_IN_DAYS; dayCounter ++){
			System.out.println("<<<<<<<<<<< Running Day - "+ dayCounter);
			long dailyMetricSum = 0;
			long dailyImpressionSum = 0;
			long timestamp = this.startEpochTime + (dayCounter * MILLIS_IN_DAY);
			int dayOfWeekId = this.getDayOfWeek(dayCounter);
			int weekId = this.getWeekOfYear(dayCounter);
			
			for(int dmaCounter = 1; dmaCounter <= NUMBER_OF_DMAS; dmaCounter ++){
				long dmaMetricSum = 0;
				long dmaImpressionSum = 0;
				
				for(int zipCounter = 1; zipCounter <= NUMBER_OF_ZIPS; zipCounter ++){
					int dma = dmaCounter;
						
					randomImpressionDataPoint = this.getRandomNumberOverRange(2, 3, this.rand);
					dmaImpressionSum += randomImpressionDataPoint;
					randomDataPoint = this.getRandomNumberOverRange(100, 1000, this.rand);
					dmaMetricSum += randomDataPoint;
					
					String rowKey = this.buildRowKey(this.campaignId, zipCounter, dma);
					System.out.println("ROWKEY: "+ rowKey);
					System.out.println("IMPRESSIONS: "+ randomImpressionDataPoint);
					System.out.println("Data point: "+ randomDataPoint);
					
					try{					
						table.put(this.createZipRowVersion(rowKey, timestamp, (dma * NUMBER_OF_DMAS) + zipCounter, dma, dayCounter, dayOfWeekId, weekId, randomImpressionDataPoint, randomDataPoint));
						System.out.println("Insert "+ rowKey + "ok.");
					}catch(Exception e){
						System.out.println("Insert error with "+ rowKey);
						e.printStackTrace();
					}
				}
				
				String parentRowKey = this.buildDMARowKey(this.campaignId, dmaCounter);
				try{
					table.put(this.createDMARowVersion(parentRowKey, timestamp, dmaCounter, dayCounter, dayOfWeekId, weekId, dmaImpressionSum, dmaMetricSum));
					System.out.println("Insert "+ parentRowKey + "ok.");
				}catch(Exception e){
					System.out.println("Insert error with "+ parentRowKey);
					e.printStackTrace();
				}
				
				dailyImpressionSum += dmaImpressionSum;
				dailyMetricSum += dmaMetricSum;
			}
			
			String rootRowKey = Integer.toString(this.campaignId);
			try{
				table.put(this.createRootRowVersion(rootRowKey, timestamp, dayCounter, dayOfWeekId, weekId, dailyImpressionSum, dailyMetricSum));
				System.out.println("Insert "+ rootRowKey + "ok.");
			}catch(Exception e){
				System.out.println("Insert error with "+ rootRowKey);
				e.printStackTrace();
			}
		}
	}
	
	private Put createZipRowVersion(String rowKey, 
			long timestamp, 
			int zipCounter, 
			int dma, 
			int dayCounter, 
			int dayOfWeekId,
			int weekId,
			long randomImpressionDataPoint,
			long randomDataPoint) throws Exception{
		Put put = new Put(Bytes.toBytes(rowKey), timestamp);
		put.add(Bytes.toBytes("GeoDim"), Bytes.toBytes("zip"), Bytes.toBytes(zipCounter));
		put.add(Bytes.toBytes("GeoDim"), Bytes.toBytes("dma"), Bytes.toBytes(dma));
		put.add(Bytes.toBytes("CampaignDim"), Bytes.toBytes("campaign"), Bytes.toBytes(this.campaignId));
		put.add(Bytes.toBytes("CampaignDim"), Bytes.toBytes("agency"), Bytes.toBytes(this.agencyName));
		put.add(Bytes.toBytes("CampaignDim"), Bytes.toBytes("branch"), Bytes.toBytes(this.branch));
		put.add(Bytes.toBytes("DateDim"), Bytes.toBytes("dateid"), Bytes.toBytes(dayCounter));
		put.add(Bytes.toBytes("DateDim"), Bytes.toBytes("dayid"), Bytes.toBytes(dayOfWeekId));
		put.add(Bytes.toBytes("DateDim"), Bytes.toBytes("weekid"), Bytes.toBytes(weekId));
		put.add(Bytes.toBytes("AttrDim"), Bytes.toBytes("impressionCount"), Bytes.toBytes(new Long(randomImpressionDataPoint)));
		put.add(Bytes.toBytes("AttrDim"), Bytes.toBytes("impressionMetric"), Bytes.toBytes(new Long(randomDataPoint)));
		
		return put;
	}
	
	private Put createDMARowVersion(String rowKey, 
			long timestamp, 
			int dma, 
			int dayCounter, 
			int dayOfWeekId,
			int weekId,
			long randomImpressionDataPoint,
			long randomDataPoint){
		Put put = new Put(Bytes.toBytes(rowKey), timestamp);
		put.add(Bytes.toBytes("GeoDim"), Bytes.toBytes("dma"), Bytes.toBytes(dma));
		put.add(Bytes.toBytes("CampaignDim"), Bytes.toBytes("campaign"), Bytes.toBytes(this.campaignId));
		put.add(Bytes.toBytes("CampaignDim"), Bytes.toBytes("agency"), Bytes.toBytes(this.agencyName));
		put.add(Bytes.toBytes("CampaignDim"), Bytes.toBytes("branch"), Bytes.toBytes(this.branch));
		put.add(Bytes.toBytes("DateDim"), Bytes.toBytes("dateid"), Bytes.toBytes(dayCounter));
		put.add(Bytes.toBytes("DateDim"), Bytes.toBytes("dayid"), Bytes.toBytes(dayOfWeekId));
		put.add(Bytes.toBytes("DateDim"), Bytes.toBytes("weekid"), Bytes.toBytes(weekId));
		put.add(Bytes.toBytes("AttrDim"), Bytes.toBytes("impressionCount"), Bytes.toBytes(new Long(randomImpressionDataPoint)));
		put.add(Bytes.toBytes("AttrDim"), Bytes.toBytes("impressionMetric"), Bytes.toBytes(new Long(randomDataPoint)));
		
		return put;
	}
	
	private Put createRootRowVersion(String rowKey, 
			long timestamp,  
			int dayCounter, 
			int dayOfWeekId,
			int weekId,
			long randomImpressionDataPoint,
			long randomDataPoint){
		Put put = new Put(Bytes.toBytes(rowKey), timestamp);
		//put.add(Bytes.toBytes("GeoDim"), Bytes.toBytes("zip"), Bytes.toBytes(zipCounter));
		//put.add(Bytes.toBytes("GeoDim"), Bytes.toBytes("dma"), Bytes.toBytes(dma));
		put.add(Bytes.toBytes("CampaignDim"), Bytes.toBytes("campaign"), Bytes.toBytes(this.campaignId));
		put.add(Bytes.toBytes("CampaignDim"), Bytes.toBytes("agency"), Bytes.toBytes(this.agencyName));
		put.add(Bytes.toBytes("CampaignDim"), Bytes.toBytes("branch"), Bytes.toBytes(this.branch));
		put.add(Bytes.toBytes("DateDim"), Bytes.toBytes("dateid"), Bytes.toBytes(dayCounter));
		put.add(Bytes.toBytes("DateDim"), Bytes.toBytes("dayid"), Bytes.toBytes(dayOfWeekId));
		put.add(Bytes.toBytes("DateDim"), Bytes.toBytes("weekid"), Bytes.toBytes(weekId));
		put.add(Bytes.toBytes("AttrDim"), Bytes.toBytes("impressionCount"), Bytes.toBytes(new Long(randomImpressionDataPoint)));
		put.add(Bytes.toBytes("AttrDim"), Bytes.toBytes("impressionMetric"), Bytes.toBytes(new Long(randomDataPoint)));
		
		return put;
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
	
	private long getRandomNumberOverRange(int min, int max, Random rand){
		long random = rand.nextInt((max-min) + 1) + min;
		return random;
	}
	
	private String buildDMARowKey(int campaignId, int dma){
		return String.valueOf(campaignId)  + "-" +String.valueOf(dma);
	}
	
	private String buildRowKey(int campaignId, int zip, int dma){
		return String.valueOf(campaignId)  + "-" +String.valueOf(dma) + "-" + String.valueOf((dma * NUMBER_OF_DMAS) + zip);
	}
}
