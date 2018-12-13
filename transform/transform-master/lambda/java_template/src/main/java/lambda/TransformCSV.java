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
import com.amazonaws.services.s3.model.ObjectMetadata;
import faasinspector.register;
import java.nio.charset.Charset;
import java.io.*;
import java.util.Random;
import java.nio.charset.StandardCharsets;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.lang.Object;
import java.lang.*;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;


/**
 * uwt.lambda_test::handleRequest
 * @author pdixith
 */
public class TransformCSV implements RequestHandler<Request, Response>
{
    static String CONTAINER_ID = "/tmp/container-id";
    static Charset CHARSET = Charset.forName("US-ASCII");


    // Lambda Function Handler
    public Response handleRequest(Request request, Context context) {
        // Create logger
        LambdaLogger logger = context.getLogger();

        // Register function
        register reg = new register(logger);
	long start = System.currentTimeMillis();
	long count=0;
	//variables to calculate Compute Time
	long startTime = 0L;
	long finishTime = 0L;
	long computeTime = 0L;
        //stamp container with uuid
        Response r = reg.StampContainer();

	setCurrentDirectory("/tmp");

        // *********************************************************************
        // Implement Lambda Function Here
        // *********************************************************************
            String bucketname=request.getBucketname();
	    String filename =request.getFilename();

	    String directoryName = "/tmp";

try{
	AmazonS3 s3Client = new AmazonS3Client();
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
	BufferedReader br = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
        CSVReader csvReader = new CSVReader(br);


	//scanning data line by line
	    String[] nextRecord;

	    HashSet<String> h = new HashSet<String>();
	    ArrayList region = new ArrayList();
            ArrayList country = new ArrayList();
            ArrayList itemType = new ArrayList();
            ArrayList salesChannel = new ArrayList();
            ArrayList orderPriority = new ArrayList();
            ArrayList orderDate = new ArrayList();
            ArrayList orderID = new ArrayList();
            ArrayList shipDate = new ArrayList();
            ArrayList unitsSold = new ArrayList();
            ArrayList unitPrice = new ArrayList();
            ArrayList unitCost = new ArrayList();
            ArrayList totalRevenue = new ArrayList();
            ArrayList totalCost = new ArrayList();
            ArrayList totalProfit = new ArrayList();
            ArrayList orderProcessingTime = new ArrayList();
            ArrayList grossMargin = new ArrayList();



	 startTime = System.currentTimeMillis();
	 int flag=1;
            while ((nextRecord = csvReader.readNext()) != null) {
                 if(h.contains(nextRecord[6])){
			logger.log("Dup");
		}
		else{
		count++;
		h.add(nextRecord[6]);
                region.add(nextRecord[0]);
                country.add(nextRecord[1]);
                itemType.add(nextRecord[2]);
                salesChannel.add(nextRecord[3]);

                 if(flag == 1){
                      orderPriority.add("Order Priority");
                 }
                 else {
                    if ("L".equals(nextRecord[4]))
                    orderPriority.add("Low");
                    else if ("M".equals(nextRecord[4]))
                    orderPriority.add("Medium");
                    else if ("H".equals(nextRecord[4]))
                    orderPriority.add("High");
                    else if ("C".equals(nextRecord[4]))
                    orderPriority.add("Critical");
                  }
                 if(flag == 1){
                      orderDate.add("Order Date");
                 }
                 else {
                    orderDate.add(nextRecord[5]);
                 }
                orderID.add(nextRecord[6]);
                 if(flag == 1){
                      shipDate.add("Ship Date");
                 }
                 else {
                    shipDate.add(nextRecord[7]);
                 }
                unitsSold.add(nextRecord[8]);
                unitPrice.add(nextRecord[9]);
                unitCost.add(nextRecord[10]);
                totalRevenue.add(nextRecord[11]);
                totalCost.add(nextRecord[12]);
                totalProfit.add(nextRecord[13]);
                if(flag == 1){
                    orderProcessingTime.add("Order Processing Time");
                    grossMargin.add("Gross Margin");
                    flag=0;
                }
                else{
                     SimpleDateFormat myFormat = new SimpleDateFormat("MM/dd/yyyy");
                     String oDate = nextRecord[5];
                     String sDate = nextRecord[7];

                     try {
                            Date order = myFormat.parse(oDate);
                            Date ship = myFormat.parse(sDate);
                            long difference = ((ship.getTime()) - (order.getTime()));
                             float daysBetween = (difference / (1000*60*60*24));
                            orderProcessingTime.add(daysBetween);
                        }
                            catch (Exception e) {
                            throw e;
                            }

                     float gmargin = (float) (Math.round(Float.valueOf(nextRecord[13])/Float.valueOf(nextRecord[11])* 100.0) / 100.0);
                     grossMargin.add(gmargin);
                }

       }
        }
	finishTime = System.currentTimeMillis();
	 computeTime = finishTime - startTime;
	csvReader.close();
	br.close();
	s3Object.close();

 try {
	    FileWriter fw = new FileWriter("/tmp/newsales.csv");
            Writer writer = new BufferedWriter(fw);
            CSVWriter csvWriter = new CSVWriter(writer,CSVWriter.DEFAULT_SEPARATOR,CSVWriter.NO_QUOTE_CHARACTER,CSVWriter.DEFAULT_ESCAPE_CHARACTER,CSVWriter.DEFAULT_LINE_END);


            for (int i = 0; i < region.size(); i++) {

               csvWriter.writeNext(new String[] {((region.get(i)).toString()),((country.get(i)).toString()),((itemType.get(i)).toString()),((salesChannel.get(i)).toString()),((orderPriority.get(i)).toString()),((orderDate.get(i)).toString()),((orderID.get(i)).toString()),((shipDate.get(i)).toString()),((unitsSold.get(i)).toString()),((unitPrice.get(i)).toString()),((unitCost.get(i)).toString()),((totalRevenue.get(i)).toString()),((totalCost.get(i)).toString()),((totalProfit.get(i)).toString()),((orderProcessingTime.get(i)).toString()),((grossMargin.get(i)).toString())});
            }

        }
catch (Exception ex) {
logger.log(ex.getMessage());
}

    }
  catch (Exception ex) {
logger.log(ex.getMessage());

}

        String clientRegion = "us-east-1";
        int id =  request.getseqNum()>0? request.getseqNum():request.getthreadId();
        String fileObjKeyName = "newsales_"+id+".csv";
        String fileName = "/tmp/newsales.csv";


  try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();


           // Upload a file as a new object with ContentType and title specified.
            PutObjectRequest rq = new PutObjectRequest(bucketname,fileObjKeyName,new File ("/tmp/newsales.csv"));
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("text/plain");
           // metadata.addUserMetadata("x-amz-meta-title", "someTitle");

            s3Client.putObject(rq);
        }
          catch(AmazonServiceException e) {
            //The call was transmitted successfully, but Amazon S3 couldn't process
             //it, so it returned an error response.
          e.printStackTrace();
        }
        catch(SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
          e.printStackTrace();
       }
catch (Exception ex) {
logger.log(ex.getMessage());

}
 long finish = System.currentTimeMillis();
 long timeElapsed = finish - start;

  r.setValue("loaded data from Bucket:"+bucketname+", filename:"+filename+", turnaround time:"+(timeElapsed)+" seconds , total rows: "+(count-1)+" ,Compute time: "+(computeTime));

return r;
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

	public static void createFile(String filename,FileInputStream input){
       try{
	    byte[] buffer = new byte[input.available()];
            input.read(buffer);
	    File file = new File(filename);
	    OutputStream out = new FileOutputStream(file);
            out.write(buffer);
            out.flush();
	    out.close();
	}
	catch(Exception e){
	}

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
        TransformCSV lt = new TransformCSV();

        // Create a request object
        Request req = new Request();

        // Grab the name from the cmdline from arg 0
        String name = (args.length > 0 ? args[0] : "");

        // Load the name into the request object
        //req.setName(name);

        // Report name to stdout
        //System.out.println("cmd-line param name=" + req.getName());

        // Run the function
        Response resp = lt.handleRequest(req, c);

        // Print out function result
        System.out.println("function result:" + resp.toString());
    }
}

