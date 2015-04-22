/*
 * Seldon -- open source prediction engine
 * =======================================
 *
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 * ********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************************************
 */

package io.seldon.resources.external;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 *
 *  Makes S3 files available as streams
 *
 * @author firemanphil
 *         Date: 06/10/2014
 *         Time: 14:30
 */
@Component
public class S3FileStreamer  {

    private static Logger logger = Logger.getLogger(S3FileStreamer.class.getName());

    private final AWSCredentials creds;

    @Autowired
    public S3FileStreamer(@Value("${aws.key:}") String awsKey, @Value("${aws.secret:}") String awsSecret) {
        if (awsKey == null || awsKey.equals("") || awsSecret == null || awsSecret.equals("")) {
            creds = null;
        } else {
            this.creds = new BasicAWSCredentials(awsKey, awsSecret);
        }
    }


    public InputStream getResourceStream(String reference) throws IOException {
        logger.info("Reading file from s3://"+reference);
        AmazonS3Client client;
        if(creds != null) {
            client = new AmazonS3Client(creds);
        } else {
            client = new AmazonS3Client();
        }
        String[] bucketAndFile = reference.split("/", 2);
        if(bucketAndFile.length!=2){
            return null;
        }
        S3Object object = client.getObject(new GetObjectRequest(bucketAndFile[0], bucketAndFile[1]));
        if(reference.endsWith(".gz")){
            return new S3ObjectInputStreamWrapper(new GZIPInputStream(object.getObjectContent()),client);

        } else {
            return new S3ObjectInputStreamWrapper(object.getObjectContent(), client);
        }
    }

    // to fix a bug in the S3 lib where if the client gets GCed before the data is read, everything breaks
    public class S3ObjectInputStreamWrapper extends FilterInputStream {

        @SuppressWarnings("unused")
        private AmazonS3Client client;

        // don't call this
        protected S3ObjectInputStreamWrapper(InputStream inputStream) {
            super(inputStream);
        }

        public S3ObjectInputStreamWrapper(InputStream inputStream, AmazonS3Client client) {
            super(inputStream);
            this.client = client;
        }

        // S3ObjectInputStream also implements abort() and getHttpRequest(). Override and delegate if needed.
    }
}
