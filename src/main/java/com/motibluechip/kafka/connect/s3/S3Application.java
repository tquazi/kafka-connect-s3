package com.motibluechip.kafka.connect.s3;



import java.io.IOException;
import java.nio.charset.StandardCharsets;



import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;


public class S3Application {

    private static final AWSCredentials credentials;
    private static String bucketName;

    static {
        //put your accesskey and secretkey here
        credentials = new BasicAWSCredentials(
          "",
          ""
        );
    }

    public static void main(String[] args) throws IOException {
        //set-up the client
        AmazonS3 s3client = AmazonS3ClientBuilder
          .standard()
          .withCredentials(new AWSStaticCredentialsProvider(credentials))
          .withRegion(Regions.US_EAST_1)
          .build();

        AWSS3Service awsService = new AWSS3Service(s3client);

        bucketName = "";

        //creating a bucket
//        if(awsService.doesBucketExist(bucketName)) {
//            System.out.println("Bucket name is not available."
//              + " Try again with a different Bucket name.");
//            return;
//        }
        //awsService.createBucket(bucketName);

        //list all the buckets


        //deleting bucket
       // awsService.deleteBucket("baeldung-bucket-test2");

        //uploading object


        //listing objects
        ObjectListing objectListing = awsService.listObjects(bucketName);
        for(S3ObjectSummary os : objectListing.getObjectSummaries()) {
            String k = os.getKey();
            if (k != null && k.startsWith("data") && k.length() > 5){
            	String filename = k.substring(5);
            	System.out.println(os.getKey() + " " + filename);
            	S3Object s3object = awsService.getObject(bucketName, k);
                S3ObjectInputStream inputStream = s3object.getObjectContent();

                String theString = IOUtils.toString(inputStream);
                System.out.println(theString);
            	awsService.copyObject(bucketName, os.getKey(), bucketName, "processed/"+filename);
            	//awsService.deleteObject(bucketName, os.getKey());
            }

        }

        //downloading an object
     //   S3Object s3object = awsService.getObject(bucketName, "Document/hello.txt");
     //   S3ObjectInputStream inputStream = s3object.getObjectContent();
        //FileUtils.copyInputStreamToFile(inputStream, new File("/Users/user/Desktop/hello.txt"));

        //copying an object
//        awsService.copyObject(
//          "baeldung-bucket",
//          "picture/pic.png",
//          "baeldung-bucket2",
//          "Document/picture.png"
//        );

        //deleting an object
   //     awsService.deleteObject(bucketName, "Document/hello.txt");

        //deleting multiple objects

    }
}