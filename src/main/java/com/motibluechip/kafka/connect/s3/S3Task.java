package com.motibluechip.kafka.connect.s3;



/**
 *
 * @author Tabrez Quazi
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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


public class S3Task extends SourceTask {
    private final static Logger log = LoggerFactory.getLogger(S3Task.class);
    private final static String LAST_UPDATE = "last_update";
    private final static String FILE_UPDATE = "file_update";


    private TimerTask task;
    private static Schema schema = null;
    private String schemaName;
    private String topic;
    private String check_dir_ms;

    private String bucket_name;
    private String access_key;
    private String secret_key;
    private String region;
    private AWSS3Service awsService = null;


    Long offset = null;
    private Set<File> retries = new HashSet<>();


    @Override
    public String version() {
        return new S3Connector().version();
    }

    /**
     * Start the Task. Handles configuration parsing and one-time setup of the Task.
     *
     * @param props initial configuration
     */
    @Override
    public void start(Map<String, String> props) {
        schemaName = props.get(S3Connector.SCHEMA_NAME);
        if (schemaName == null)
            throw new ConnectException("config schema.name null");
        topic = props.get(S3Connector.TOPIC);
        if (topic == null)
            throw new ConnectException("config topic null");

        bucket_name = props.get(S3Connector.S3_BUCKET_NAME);
        if (bucket_name == null)
            throw new ConnectException("config bucket_name null");

        access_key = props.get(S3Connector.S3_ACCESS_KEY);
        if (access_key == null)
            throw new ConnectException("config access_key null");

        secret_key = props.get(S3Connector.S3_SECRET_KEY);
        if (secret_key == null)
            throw new ConnectException("config secret_key null");

        region = props.get(S3Connector.S3_REGION);
        if (region == null)
            throw new ConnectException("config region null");



        AWSCredentials credentials = new BasicAWSCredentials(access_key, secret_key);
        AmazonS3 s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(region)
                .build();

        awsService = new AWSS3Service(s3client);

        check_dir_ms = props.get(S3Connector.CHCK_DIR_MS);

        loadOffsets();


        schema = SchemaBuilder
                .struct()
                .name(schemaName)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("path", Schema.OPTIONAL_STRING_SCHEMA)
                .field("content", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }


    /**
     * Poll this S3Task for new records.
     *
     * @return a list of source records
     * @throws InterruptedException
     */
    @Override
    public List<SourceRecord> poll() throws InterruptException {

        List<SourceRecord> records = new ArrayList<>();


        ObjectListing objectListing = awsService.listObjects(bucket_name);
        for(S3ObjectSummary os : objectListing.getObjectSummaries()) {
            String k = os.getKey();
            if (k != null && k.startsWith("data") && k.length() > 5){
            	String filename = k.substring(5);
            	System.out.println(os.getKey() + " " + filename);
            	S3Object s3object = awsService.getObject(bucket_name, k);
                S3ObjectInputStream inputStream = s3object.getObjectContent();
                String theString = "";
                try{
                	theString = IOUtils.toString(inputStream);
                	System.out.println(theString);
                    records.addAll(createUpdateRecord(filename,k,theString));
                	awsService.copyObject(bucket_name, os.getKey(), bucket_name, "processed/"+filename);
                	awsService.deleteObject(bucket_name, os.getKey());
                }catch(Exception e){
                	e.printStackTrace();
                }


            	//awsService.deleteObject(bucketName, os.getKey());
            }

        }





        return records;
    }

    private SourceRecord createPendingRecord(File file) {
        // creates the structured message
        Struct messageStruct = new Struct(schema);
        messageStruct.put("name", file.getName());
        messageStruct.put("path", file.getPath());
		messageStruct.put("content", getContents(file.getPath()));
        return new SourceRecord(Collections.singletonMap(file.toString(), "state"), Collections.singletonMap("pending", "yes"), topic, messageStruct.schema(), messageStruct);
    }

    /**
     * Create a new SourceRecord from a File
     *
     * @return a source records
     */
    private List<SourceRecord> createUpdateRecord(String filename, String filepath, String content) {
        List<SourceRecord> recs = new ArrayList<>();
        // creates the structured message
		try {
			 Struct messageStruct = new Struct(schema);
			messageStruct.put("name", filename);
	        messageStruct.put("path", filepath);
			messageStruct.put("content", content);
	        recs.add(new SourceRecord(offsetKey(filename), Collections.singletonMap("committed", "yes"), topic, messageStruct.schema(), messageStruct));
		//recs.add(new SourceRecord(offsetKey(), offsetValue(System.currentTimeMillis()), topic, messageStruct.schema(), messageStruct));
		}catch (Exception e) {
			e.printStackTrace();
        }
        return recs;
    }

    private Map<String, String> offsetKey() {
        return Collections.singletonMap(FILE_UPDATE, "/data/");
    }

    private Map<String, String> offsetKey(String filename) {
        return Collections.singletonMap(FILE_UPDATE, filename);
    }

    private Map<String, Object> offsetValue(FileTime time) {
        offset = time.toMillis();
        return Collections.singletonMap(LAST_UPDATE, offset);
    }

    /**
     * Loads the current saved offsets.
     */
    private void loadOffsets() {
        Map<String, Object> off = context.offsetStorageReader().offset(offsetKey());
        if (off != null)
            offset = (Long) off.get(LAST_UPDATE);
    }

    /**
     * Signal this SourceTask to stop.
     */
    @Override
    public void stop() {
        task.cancel();
    }


	private String getContents(String fileName){
		String content = "";
		try {
			BufferedReader reader = new BufferedReader(new FileReader(fileName));
			StringBuilder stringBuilder = new StringBuilder();
			char[] buffer = new char[10];
			while (reader.read(buffer) != -1) {
				stringBuilder.append(new String(buffer));
				buffer = new char[10];
			}
			reader.close();

			content = stringBuilder.toString();
		}catch (Exception e) {
			e.printStackTrace();
        }
		return content;
	}
}
