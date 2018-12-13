/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context; 
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import faasinspector.register;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Scanner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * uwt.lambda_test::handleRequest
 * @author wlloyd
 */
public class Service3 implements RequestHandler<Request, Response>
{
    static String CONTAINER_ID = "/tmp/container-id";
    static Charset CHARSET = Charset.forName("US-ASCII");
    
     // Initialize the Log4j logger.
    static LambdaLogger logger;
    
    static String sqlDatabaseFileName = "salespipeline.db";
    
    public static boolean setCurrentDirectory(String directory_name)
    {
        boolean result = false;  // Boolean indicating whether directory was set
        File    directory;       // Desired current working directory

        directory = new File(directory_name).getAbsoluteFile();
        if (directory.exists() || directory.mkdirs())
        {
            result = (System.setProperty("user.dir", directory.getAbsolutePath()) != null);
        }

        return result;
    }
        
     public static void createFile(String directoryName, String filename,S3Object object){
       try{
           InputStream reader = new BufferedInputStream(object.getObjectContent());
            File file = new File(directoryName, filename);      
            OutputStream writer = new BufferedOutputStream(new FileOutputStream(file));

            int read = -1;

            while ( ( read = reader.read() ) != -1 ) {
                writer.write(read);
            }

            writer.flush();
            writer.close();
            reader.close();
          
        logger.log("File after writing has size = " + file.length());
        logger.log("File path = " + file.getAbsolutePath());
	}catch(Exception ex){
            logger.log(ex.toString());
        }
    }
    
    private  void DownloadSQLiteDatabase(String bucketName, String sourceKey, String fileName, String directoryName)
    {
        
        setCurrentDirectory("/tmp");
            
        logger.log("Download the sqlite database");
        
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();         
        
        //get object file using source bucket and srcKey name
        
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, sourceKey));
        //get content of the file        
        logger.log("Now create the file after downloading..");
       createFile(directoryName, "salespipeline.db", s3Object);
       logger.log("Created the file successfully");
    }
    
    private boolean checkIfFileExists(String directory, String fileWithDatabase){
        File f = new File(directory, fileWithDatabase);
        if(f.exists()){
            logger.log("File size = " + f.length());
        }
        return f.exists();
    }
    
    private boolean deleteDatabase(String directoryName, String fileWithDatabase){
        File f = new File(directoryName, fileWithDatabase);
        return f.delete();
    }
    
     public static int getCountOfRecords(String sqliteDatabaseFileName){
	int count = 0;
	try{
	    Connection con = DriverManager.getConnection("jdbc:sqlite:" + sqliteDatabaseFileName);
	    PreparedStatement ps1 = con.prepareStatement("select count(*) as count from sales;");
	    ResultSet rs1 = ps1.executeQuery();
	    count = rs1.getInt("count");	
	}
	catch(Exception e){
		e.printStackTrace();
                logger.log(e.toString());
	}
	return count;
    }
    
    public static String[] Columns = {
        "region",
        "country",
        "itemtype",
        "saleschannel",
        "orderpriority",
        "orderdate",
        "orderid",
        "shipdate",
        "unitssold",
        "unitprice",
        "unitcost",
        "totalrevenue",
        "totalcost",
        "totalprofit"            
    };
         
    public String CreateAggregateQuery(String originalQuery, LinkedList<AggregateInfo> aggregateInfos) throws Exception{
        
        if(originalQuery.compareTo("") == 0){
            throw new Exception("Query is empty!");
        }
        originalQuery += " ";
        
        if(aggregateInfos.size() > 0){
            originalQuery += ",";
        }
        for(int i = 0; i < aggregateInfos.size(); i++){
            AggregateInfo info = aggregateInfos.get(i);
            String columnName = info.getColumnname();
            logger.log("column = " + columnName);
            columnName = columnName.toLowerCase();
            columnName = columnName.replace(" ", "");
            columnName = Transformer.transformColumn(logger, columnName);
            String type =  info.getType();
            logger.log("type = " + type);
            originalQuery += type + "(" + columnName + ")";
            if(i != aggregateInfos.size() - 1)
            {
                originalQuery += ",";
            }
            
        }
//        originalQuery += " GROUP BY ";
        return originalQuery;
    }
     
    public String getColumnName(String columnName){
        columnName = columnName.toLowerCase();
        columnName = columnName.replace(" ", "");
        return columnName;
    }
    
    
    public LinkedList< Map<String, Object>> HandleQuery(Response response, 
                            LinkedList<AggregateInfo> aggregateInfos,
                            LinkedList<FilterInfo> filterInfos, String[] columns, String[] groupByColumns) throws Exception
    {       
        logger.log("Handle aggregate and filter query");     
        String query = "SELECT ";
        String[] arr;
        if(columns!=null && columns.length != 0){
            arr = columns;
        }
        else{
            arr = Columns;
        }
        
        for(int i = 0; i < arr.length; i++){
            query += arr[i];
            if(i!= arr.length - 1){
                query += ",";
            }
        }
        query += " ";
        logger.log("Query  = " + query);
        boolean aggregate = false;
        if(aggregateInfos!= null && !aggregateInfos.isEmpty()){
            query = CreateAggregateQuery(query, aggregateInfos);                        
            aggregate = true;
        }
        
        query += " from sales ";
        logger.log("Query  = " + query);
        
        if(aggregate)
        {
            query += "GROUP BY ";
            for(int i = 0; i < groupByColumns.length; i++){
                logger.log("Adding column = " + groupByColumns[i]);
                String columnName = groupByColumns[i];
                query  = query +   getColumnName(columnName); 
                if(i!= groupByColumns.length - 1){
                    query  += ",";
                }

            }            
        }
        
        logger.log("query we are executing = " + query);
        if(filterInfos!= null && !filterInfos.isEmpty()){
            
            if(aggregate){
                query += " HAVING ";                
            }
            else
            {
                query += " WHERE ";
            }
            
            logger.log("there is filter info");
            for(int i = 0; i < filterInfos.size(); i++){
                String columnName = getColumnName(filterInfos.get(i).getColumnname());
                String columnValue = Transformer.transformValue(logger, columnName, filterInfos.get(i).getColumnvalue());               
                query +=  columnName + "='" +  columnValue + "'";
                if(i!= filterInfos.size() - 1){
                    query += " AND ";
                }
            }
        }
        
        logger.log("query we are executing = " + query);
        response.setQuery(query);
        return ExecuteQuery(query);
    }
    
    public LinkedList< Map<String, Object>> ExecuteQuery(String query){
        int count = 0;
        
        LinkedList< Map<String, Object>> results = new LinkedList< Map<String, Object>>();
	try{
	    Connection con = DriverManager.getConnection("jdbc:sqlite:" + sqlDatabaseFileName);
	    PreparedStatement ps1 = con.prepareStatement(query);
	    ResultSet ps = ps1.executeQuery();
            
            logger.log("Execute the results, query = " + query);
            
            ResultSetMetaData metadata = ps.getMetaData();
            int columnCount = metadata.getColumnCount();
            logger.log("Column count = " + columnCount);
            
            LinkedList<Map<String, Object>> jsonObjects = new LinkedList<Map<String, Object>>();
            
            while (ps.next())
            {
                Map<String, Object> jsonObject = new  HashMap<String, Object>();
                
                for(int i = 1; i <= columnCount; i++){
                    jsonObject.put(metadata.getColumnName(i), ps.getObject(i)); 
                    logger.log("column = " + metadata.getColumnName(i));
                }
                results.add(jsonObject);
                
                logger.log("Found record");

/*                
                            
                           ps.get
                SalesRecord record = new SalesRecord();
                record.Region = ps.getString(Columns[0]);
                record.Country = ps.getString(Columns[1]);
                record.ItemType = ps.getString(Columns[2]);
                record.SalesChannel = ps.getString(Columns[3]);
                record.OrderPriority = ps.getString(Columns[4]);
                record.OrderDate = ps.getString(Columns[5]);
                record.OrderId = ps.getInt(Columns[6]);
                record.ShipDate = ps.getString(Columns[7]);
                record.UnitsSold = ps.getInt(Columns[8]);                   
                record.UnitPrice = ps.getDouble(Columns[9]);
                record.UnitCost = ps.getDouble(Columns[10]);
                record.TotalRevenue = ps.getDouble(Columns[11]);
                record.TotalCost = ps.getDouble(Columns[12]);
                record.TotalProfit  = ps.getDouble(Columns[13]);	
                results.add(record); */
            }
            ps.close();
            con.close();
	}
	catch(Exception e){
		e.printStackTrace();
                logger.log(e.toString());
	}
        return results;
    }
      
    public boolean isFilterTypeQuery(String queryType){
        return queryType.toLowerCase().equals("filter");
    }
    
    public LinkedList< Map<String, Object>> GetResults(LinkedList<AggregateInfo> aggregateInfos) throws Exception
    {
        String query = "SELECT * FROM sales";
        query = CreateAggregateQuery(query, aggregateInfos);
        return ExecuteQuery(query);
    }
    
    // Lambda Function Handler
    public Response handleRequest(Request request, Context context) {
        // Create logger
         logger = context.getLogger();

         logger.log("log = " + request.toString());
        // Register function
        register reg = new register(logger);

        Response r = reg.StampContainer();
                      
        String directoryName = "/tmp";
        logger.log("Called the aws lamnbda");
        String bucketName = request.getBucketname();//"test.bucket.562.rah1";
        String databaseFileName = "";
        if(request.getDatabasefilename()== null || "".equals(request.getDatabasefilename())){
             databaseFileName = sqlDatabaseFileName;            
        }
        else{
            databaseFileName = request.getDatabasefilename();      
        }
       
        if(!checkIfFileExists(directoryName, databaseFileName))
        {
            logger.log("Creating the database file since it does not exist");
            DownloadSQLiteDatabase(bucketName, databaseFileName, databaseFileName, directoryName);
            logger.log("Downloaded the entire datavase");
        }
        
        try{
            LinkedList<FilterInfo> filterInfos = request.getFilterinfo();
            if(filterInfos == null){
                logger.log("Filter info nbot passed");
            }
            else
            {
                logger.log("Count = " + filterInfos.size());
            }
            String[] groupByColumns = request.getGroupbycolumns();
            LinkedList<Map<String, Object>> result = HandleQuery(r, request.getAggregateInfo(), request.getFilterinfo(),
                            request.getColumns(), groupByColumns);
            r.setSalesRecords(result);
            r.setCount(result.size());  
            r.setMessage("Success");
            
        }catch(Exception ex)
        {
            logger.log(ex.toString());
            logger.log("m,essage = " + ex.getMessage());
            logger.log("Stack trace = " + ex.getStackTrace());
            r.setMessage("ERROR: " + ex.getMessage());
        }
        return r; 
    }
    
    
    public void QueryHandler(LinkedList<AggregateInfo> aggregateInfo){
        
    }
    
    
    // int main enables testing function from cmd line
    public static void main (String[] args)
    {
        Context c = new Context() {
            @Override
            public String getAwsRequestId() {
                return "";
            }

            @Override
            public String getLogGroupName() {
                return "";
            }

            @Override
            public String getLogStreamName() {
                return "";
            }

            @Override
            public String getFunctionName() {
                return "";
            }

            @Override
            public String getFunctionVersion() {
                return "";
            }

            @Override
            public String getInvokedFunctionArn() {
                return "";
            }

            @Override
            public CognitoIdentity getIdentity() {
                return null;
            }

            @Override
            public ClientContext getClientContext() {
                return null;
            }

            @Override
            public int getRemainingTimeInMillis() {
                return 0;
            }

            @Override
            public int getMemoryLimitInMB() {
                return 0;
            }

            @Override
            public LambdaLogger getLogger() {
                return new LambdaLogger() {
                    @Override
                    public void log(String string) {
                        System.out.println("LOG:" + string);
                    }
                };
            }
        };
        
        // Create an instance of the class
        Service3 lt = new Service3();
        
        
        // Grab the name from the cmdline from arg 0
        String name = (args.length > 0 ? args[0] : "");
        // Create a request object
        Request req = new Request(null, null, null, null, null, null);
        
        
        // Run the function
        Response resp = lt.handleRequest(req, c);
        try
        {
            Thread.sleep(100000);
        }
        catch (InterruptedException ie)
        {
            System.out.print(ie.toString());
        }
        // Print out function result
        System.out.println("function result:" + resp.toString());
    }
}
