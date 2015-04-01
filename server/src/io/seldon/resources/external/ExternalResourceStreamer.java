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

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 * Returns external resources as a stream.
 *
 * @author firemanphil
 *         Date: 06/10/2014
 *         Time: 14:28
 */
@Component
public class ExternalResourceStreamer {

    private static Logger logger = Logger.getLogger(ExternalResourceStreamer.class.getName());
    private final S3FileStreamer s3Streamer;
    private final LocalFileStreamer localStreamer;

    @Autowired
    public ExternalResourceStreamer(S3FileStreamer s3Streamer, LocalFileStreamer localStreamer) {
        this.s3Streamer = s3Streamer;
        this.localStreamer = localStreamer;
    }

    public InputStream getResourceStream(String reference) throws IOException {
        if(reference.startsWith("s3n://")){
            return s3Streamer.getResourceStream(reference.replace("s3n://",""));
        } else if(reference.startsWith("local:/") || reference.startsWith("/")){
            return localStreamer.getResourceStream(reference.replace("local:/",""));
        } else {
            logger.warn("Couldn't decode the format in message: "+reference);
            throw new IOException("Unknown external resource format: " + reference);
        }
    }
}
