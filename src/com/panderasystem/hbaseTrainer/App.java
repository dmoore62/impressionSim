package com.panderasystem.hbaseTrainer;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        CreateTable ct = new CreateTable();
        String tableName = "impressionsAgg";
        String[] columnFamilies = {"GeoDim", "CampaignDim", "DateDim", "AttrDim"};
        try{
        	ct.newTable(tableName, columnFamilies);
        }catch(Exception e){
        	System.out.println(e.getMessage() + e.getCause());
        }
        
        TablePopulator tp = new TablePopulator(tableName);
        try{
        	tp.simulateImpressionData();
        }catch(Exception e){
        	e.printStackTrace();
        }
    }
}
