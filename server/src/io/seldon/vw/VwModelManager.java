/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.vw;

import io.seldon.mf.PerClientExternalLocationListener;
import io.seldon.resources.external.ExternalResourceStreamer;
import io.seldon.resources.external.NewResourceNotifier;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class VwModelManager implements PerClientExternalLocationListener {


    private static Logger logger = Logger.getLogger(VwModelManager.class.getName());
    private final ConcurrentMap<String, VwModel> clientStores = new ConcurrentHashMap<>();
    private NewResourceNotifier notifier;
    private final ExternalResourceStreamer featuresFileHandler;
    private static final String MF_NEW_LOC_PATTERN = "vw";

    private final Executor executor = Executors.newFixedThreadPool(5);

    @Autowired
    public VwModelManager(ExternalResourceStreamer featuresFileHandler,
                             NewResourceNotifier notifier){
        this.featuresFileHandler = featuresFileHandler;
        this.notifier = notifier;

    }

    @PostConstruct
    public void init(){
        notifier.addListener(MF_NEW_LOC_PATTERN, this);
    }

    public void reloadFeatures(final String location, final String client){
        executor.execute(new Runnable() {
            @Override
            public void run() {
                logger.info("Reloading VW model for client: "+ client);

                try {
                    BufferedReader modelReader = new BufferedReader(new InputStreamReader(
                            featuresFileHandler.getResourceStream(location + "/model")
                    ));
                    VwModel model = loadModel(modelReader);
                    
                    clientStores.put(client, model);
                    
                    modelReader.close();

                } catch (FileNotFoundException e) {
                    logger.error("Couldn't reload modelfor client "+ client+" at "+location, e);
                } catch (IOException e) {
                    logger.error("Couldn't reload model for client "+ client+" at "+location, e);
                }
            }
        });

    }

    public VwModel getModel(String client){
        return clientStores.get(client);
    }


    private VwModel loadModel(BufferedReader reader) throws IOException {
        Map<Integer,Float> weights = new HashMap<Integer,Float>();
        int oaa = 1;
        int bits = 18;
        String line;
        boolean insideFeatures = false;
        while((line = reader.readLine()) !=null)
        {
            if (!insideFeatures)
            {
            	if (line.startsWith("bits:"))
            	{
            		bits = Integer.parseInt(line.split(":")[1]);
            	}
            	else if (line.startsWith("options:"))
            	{
            		String[] parts = line.split(":");
            		if (parts.length > 1)
            		{
            			String[] options = parts[1].split("\\s+");
            			for(int i=0;i<options.length;i++)
            			{
            				if ("--oaa".equals(options[i]))
            				{
            					oaa = Integer.parseInt(options[i+1]);
            				}
            			}
            		}
            	}
            	else if (line.startsWith(":0"))
            	{
            		insideFeatures = true;
            	}
            }
            else
            {
            	String[] featureAndWeight = line.split(":");
            	int feature = Integer.parseInt(featureAndWeight[0]);
            	float weight = Float.parseFloat(featureAndWeight[1]);
            	weights.put(feature, weight);
            }
        }
        return new VwModel(bits, oaa, weights);
    }

    @Override
    public void newClientLocation(String client, String location,String nodePattern) {
        reloadFeatures(location,client);
    }

    @Override
    public void clientLocationDeleted(String client,String nodePattern) {
        clientStores.remove(client);
    }

    public static class VwModel 
    {
    	public final int bits;
    	public final int oaa;
    	public final Map<Integer,Float> weights;
    	public final VwFeatureHash hasher;
    	
		public VwModel(int bits, int oaa, Map<Integer, Float> weights) {
			super();
			this.bits = bits;
			this.oaa = oaa;
			this.weights = weights;
			this.hasher = new VwFeatureHash(bits, oaa);
		}

		@Override
		public String toString() {
			return "VwModel [bits=" + bits + ", oaa=" + oaa + ", weights="
					+ weights + "]";
		}
    	
    	
    }
    
    public static void main(String[] args) throws IOException
    {
    	BufferedReader br = new BufferedReader(new FileReader("/home/clive/work/seldon/external_prediction_server/vw/iris/model.txt"));
    	VwModelManager m = new VwModelManager(null, null);
    	VwModel vwModel = m.loadModel(br);
    	System.out.println(vwModel);
    }
}
