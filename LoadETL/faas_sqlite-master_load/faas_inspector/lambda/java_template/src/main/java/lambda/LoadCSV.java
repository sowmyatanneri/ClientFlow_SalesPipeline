/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template f-ile, choose Tools | Templates
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
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.PutObjectRequest;
import java.lang.management.ManagementFactory;
import faasinspector.register;
import java.text.DecimalFormat;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Scanner;
/**
 * uwt.lambda_test::handleRequest
 * @author Sowmya Tanneri
 */
public class LoadCSV implements RequestHandler<Request, Response>
{
    static String CONTAINER_ID = "/tmp/container-id";
    static Charset CHARSET = Charset.forName("US-ASCII");


    // Lambda Function Handler
    public Response handleRequest(Request request, Context context) {
        //Getting the start time of the service on server
	long start = System.currentTimeMillis();
	//Initilizing the start,finish,elapsed time to calculate throughput on sqllite db
	long startTime = 0L;
	long finishTime = 0L;
	long elapsedTime = 0L;
        //Getting the start CPU time on service
	long cputime0 = ManagementFactory.getThreadMXBean().getThreadCpuTime(java.lang.Thread.currentThread().getId());
        // Create logger
        LambdaLogger logger = context.getLogger();
	   // Register function
        register reg = new register(logger);

        //stamp container with uuid
        Response r = reg.StampContainer();
        // *********************************************************************
        // Implement Lambda Function Here
        // *********************************************************************
        //Fecting the bucket name and CSV file name
        String bucketname = request.getBucketname();
        String filename = request.getFilename();
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        //fetch the file from S3
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname,filename));
        //get content of the file
        InputStream objectData = s3Object.getObjectContent();
        setCurrentDirectory("/tmp");
	int id =  request.getseqNum()>0? request.getseqNum():request.getthreadId();
	 try
        {

            // Connection string an in-memory SQLite DB
	    Connection con = DriverManager.getConnection("jdbc:sqlite:salespipeline_"+id+".db");
            logger.log("trying to create able 'sales' if it does not exists");
	    PreparedStatement ps = con.prepareStatement("SELECT   name   FROM   sqlite_master   WHERE type='table' AND name='sales'");
	    ResultSet rs = ps.executeQuery();
	    if (!rs.next())
	    {
		    // 'sales' does not exist, and should be created
		    logger.log("trying to create table 'sales'");
		    ps = con.prepareStatement("CREATE TABLE IF NOT EXISTS sales(region text,country text,itemtype text,saleschannel text,orderpriority text,orderdate text,orderid integer,shipdate text,unitssold integer,unitprice real,unitcost real,totalrevenue real,totalcost real,totalprofit real,orderprocessingtime text,grossmargin real);");
		    ps.execute();
	    }
	    rs.close();
	    //getting the start time to load data to sqlite db
	    startTime = System.currentTimeMillis();
	    //reading from csv and loading to database
       	    loadData(con,objectData);
	    //getting the finish time and calculating the time elapsed on sqlite db
            finishTime = System.currentTimeMillis();
	    elapsedTime = finishTime - startTime;
	    //closing connections
            con.close();

            // uploading salespipeline.db file to S3
	    //creating putObjectRequest
            PutObjectRequest req = new PutObjectRequest(bucketname, "salespipeline_"+id+".db", new File("/tmp/salespipeline_"+id+".db"));
            //setting the metadata for the file to be uploaded to S3
	    ObjectMetadata meta = new ObjectMetadata();
	    meta.setContentType("binary/octet-stream");
	    meta.addUserMetadata("x-amz-meta-title", "Uploading DB file");
	    req.setMetadata(meta);
            //Putting the file on S3
	    s3Client.putObject(new PutObjectRequest(bucketname, "salespipeline_"+id+".db", new File("/tmp/salespipeline_"+id+".db")));
        }
        catch(Exception e)
        {
            logger.log("ERROR:" + e.toString());
            e.printStackTrace();
        }

        //getiing the finish time on server and calculating the time elapsed on servetr
        long finish = System.currentTimeMillis();
	long timeElapsed = finish - start;

        //getiing the finish cpu time on server and calculating the time elapsed on servetr
	long cputime1 = ManagementFactory.getThreadMXBean(). getThreadCpuTime(java.lang.Thread.currentThread().getId());
	Long cputimedelta = (cputime1-cputime0)/1000000;

	//getting the number of rows inserted
	 int rows = Integer.parseInt(getRowsInserted(id));

	 // Set return result in Response class, class is marshalled into JSON
        r.setValue("loaded data from Bucket:"+bucketname+" filename:"+filename+" to Database\n"+" Elapsede time on Server:"+timeElapsed+" ms"+"\n cpuTime:"+cputimedelta+" seconds\n"+"Data Throughput in loading "+rows+"rows to Sqlite is "+elapsedTime+" ms per row");
        return r;
    }
    public static String getRowsInserted(int id){
	String rows = "";
	setCurrentDirectory("/tmp");
	 try
        {
            // Connection string an in-memory SQLite DB
	    Connection con = DriverManager.getConnection("jdbc:sqlite:salespipeline_"+id+".db");
	    String query = "SELECT   count(*) as rowsInserted   FROM  sales";
	    Statement stmt = con.createStatement();
	    ResultSet rs = stmt.executeQuery(query);
	    while(rs.next()){
		rows = rs.getString("rowsInserted");
	    }

	}catch(Exception e){
	    e.printStackTrace();
	}
	return rows;
    }
    public static void loadData(Connection con,InputStream objectData){
	try{

	    Scanner scanner = new Scanner(objectData);
	    //setting auto commit mode to false
	    con.setAutoCommit(false);
	    //fetching the field names for db
	    if(scanner.hasNext()) scanner.nextLine();
	    PreparedStatement ps = con.prepareStatement("insert into sales(region,country,itemtype,saleschannel,orderpriority,orderdate,orderid,shipdate,unitssold,unitprice,unitcost,totalrevenue,totalcost,totalprofit,orderprocessingtime,grossmargin) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");
	    while(scanner.hasNext()){
            	String downloadedText=scanner.nextLine();
		//fetching data from csv and loading to sales table
	    	String[] fields = new String[16];
		fields = downloadedText.split(",");
	    	    ps.setString(1,fields[0]);
		    ps.setString(2,fields[1]);
		    ps.setString(3,fields[2]);
		    ps.setString(4,fields[3]);
		    ps.setString(5,fields[4]);
		    ps.setString(6,fields[5]);
		    ps.setInt(7,Integer.parseInt(fields[6]));
		    ps.setString(8,fields[7]);
		    ps.setInt(9,Integer.parseInt(fields[8]));
		    ps.setDouble(10,Double.parseDouble(fields[9]));
		    ps.setDouble(11,Double.parseDouble(fields[10]));
		    ps.setDouble(12,Double.parseDouble(fields[11]));
		    ps.setDouble(13,Double.parseDouble(fields[12]));
		    ps.setDouble(14,Double.parseDouble(fields[13]));
		    ps.setString(15,fields.length > 14?fields[14]:"");
		    ps.setDouble(16,fields.length > 14?Double.parseDouble(fields[15]):0.0);
		    ps.addBatch();
            }
	    ps.executeBatch();
	    //closing connections
	    con.commit();
            ps.close();
	   scanner.close();
	}
	catch(Exception e)
        {
            e.printStackTrace();
        }
    }
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
        LoadCSV lt = new LoadCSV();

        // Create a request object
        Request req = new Request();

        // Grab the name from the cmdline from arg 0
        String fileName = (args.length > 0 ? args[0] : "");
        String bucketName = (args.length > 1 ? args[1] : "");
	    int seqNum = Integer.parseInt((args.length > 2 ? args[2] : ""));
	    int threadId = Integer.parseInt((args.length > 3 ? args[3] : ""));

        // Load the bucketname and filename into the request object
        req.setBucketname(bucketName);
        req.setFilename(fileName);
	req.setseqNum(seqNum);
	req.setthreadId(threadId);

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
