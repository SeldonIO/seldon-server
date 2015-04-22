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

import java.io.*;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

/**
 * @author firemanphil
 *         Date: 22/12/14
 *         Time: 21:36
 */
@Component
public class LocalFileStreamer {

    private static Logger logger = Logger.getLogger(LocalFileStreamer.class.getName());
    public InputStream getResourceStream(String reference) throws IOException {
        logger.info("Reading file from local://"+reference);

        File f = new File(reference);

        if(reference.endsWith(".gz")) {
            return new BufferedInputStream(new GZIPInputStream(new FileInputStream(f)));
        } else {
            return new BufferedInputStream(new FileInputStream(f));
        }

        //return LocalFileStreamer.class.getResourceAsStream(reference);
    }
}