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

package io.seldon.api.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import com.netflix.curator.utils.EnsurePath;
import io.seldon.api.resource.service.CacheExpireService;
import io.seldon.facebook.SocialFriendsBlender;
import io.seldon.facebook.SocialFriendsScoreDecayFunction;
import io.seldon.facebook.SocialRecommendationStrategy;
import io.seldon.facebook.user.FacebookFriendConnectionGraphBuilder;
import io.seldon.facebook.user.FacebookUsersAlgorithm;
import io.seldon.facebook.user.algorithm.BaseSocialRecommendationStrategyStore;
import io.seldon.facebook.user.algorithm.experiment.MultiVariateTest;
import io.seldon.facebook.user.algorithm.experiment.MultiVariateTestStore;
import io.seldon.facebook.user.algorithm.experiment.MultiVariateTestVariation;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

/**
 * @author philipince
 *         Date: 13/10/2013
  *
 *         dylanlentini
 *         Date: 17/01/2014
 */
@Component
public class ZkMgmUpdater implements ApplicationContextAware, PathChildrenCacheListener {

    private static final String ALG_MULTIVARIATE_TEST_PATH = "/mgmalgtest";
    private static final String ALG_PATH = "/mgmalg";
    private static final String FRONTEND_MULTIVARIATE_TEST_PATH = "/mgmfronttest";
    private static final String ON_OFF_PATH = "/mgmonoff";
    private static final String CLIENTS_MGM = "/clients/mgm";
    private static final String MGM_SETTINGS = "/mgmsettings";
    private static final String MGM_CACHE_EXPIRE_SECS = "/mgmcacheexpiresecs";
    private static final String MGM_FRIENDS_PER_QUERY = "/v";


    private static Logger logger = Logger.getLogger(ZkMgmUpdater.class.getName());
    @Resource
    private BaseSocialRecommendationStrategyStore strategyStore;
    @Resource
    private MultiVariateTestStore mvTestStore;
    @Autowired
    private CacheExpireService cacheExpireService;


    private Set<PathChildrenCache> caches = new HashSet<PathChildrenCache>();

    private Map<String, MultiVariateTest<SocialRecommendationStrategy>> suspendedTests = new HashMap<String, MultiVariateTest<SocialRecommendationStrategy>>();
    private Map<String, Boolean> testingOnOrOff = new HashMap<String, Boolean>();

    private ApplicationContext applicationContext;


    public void initialise(ZkCuratorHandler curator){
        logger.info("Starting zookeeper mgm multivariate testing server");
        listenToNewClientsPath(curator);
        setupAndListenToExistingClients(curator);
        listenToMgmSettings(curator);
    }

    public void listenToMgmSettings(ZkCuratorHandler curator) {
        logger.info("Zookeeper setting up listening to path["+MGM_SETTINGS+"]");
        try
        {
            CuratorFramework curatorFramework = curator.getCurator();
            PathChildrenCache cache = new PathChildrenCache(curatorFramework, MGM_SETTINGS, true);
            caches.add(cache);
            EnsurePath ensureMvTestPath = new EnsurePath(MGM_SETTINGS);
            ensureMvTestPath.ensure(curatorFramework.getZookeeperClient());
            cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            cache.getListenable().addListener(this);
            logger.info("Watching path " + MGM_SETTINGS);
            for(ChildData data : cache.getCurrentData())
            {
                childEvent(curatorFramework, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_UPDATED, data));
            }
        }
        catch (Exception e)
        {
            logger.warn("Couldn't start listener for path " + CLIENTS_MGM, e);
        }
    }

    public void listenToNewClientsPath(ZkCuratorHandler curator)
    {
        logger.info("Zookeeper setting up /clients/mgm path to listen to new clients");
        try
        {
            CuratorFramework client = curator.getCurator();
            PathChildrenCache cache = new PathChildrenCache(client, CLIENTS_MGM, true);
            caches.add(cache);
            EnsurePath ensureMvTestPath = new EnsurePath(CLIENTS_MGM);
            ensureMvTestPath.ensure(client.getZookeeperClient());
            cache.start(PathChildrenCache.StartMode.NORMAL);
            cache.getListenable().addListener(this);
            logger.info("Watching path " + CLIENTS_MGM);
        }
        catch (Exception e)
        {
            logger.warn("Couldn't start listener for path " + CLIENTS_MGM, e);
        }
    }


    public void setupAndListenToExistingClients(ZkCuratorHandler curator)
    {
        logger.info("Setting listeners for mgm clients");
        try
        {
            CuratorFramework curatorFramework = curator.getCurator();
            for (String clientName : curatorFramework.getChildren().forPath(CLIENTS_MGM))
            {
                PathChildrenCache cache = new PathChildrenCache(curatorFramework, CLIENTS_MGM + '/' + clientName, true);
                caches.add(cache);
                String[] types = {ALG_MULTIVARIATE_TEST_PATH, ALG_PATH, FRONTEND_MULTIVARIATE_TEST_PATH, ON_OFF_PATH};
                for (String type : types)
                {
                    EnsurePath ensureMvTestPath = new EnsurePath(CLIENTS_MGM + '/' + clientName + type);
                    ensureMvTestPath.ensure(curatorFramework.getZookeeperClient());
                }
                cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
                cache.getListenable().addListener(this);
                logger.info("Watching path " + CLIENTS_MGM + '/' + clientName);
                logger.info("Updating strategy store " + CLIENTS_MGM + '/' + clientName);

                for(ChildData data : cache.getCurrentData())
                {
                    childEvent(curatorFramework, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_UPDATED, data));
                }
            }
        }
        catch (Exception e)
        {
            logger.warn("Couldn't start listeners for path " + CLIENTS_MGM, e);
        }

    }



    @Override
    public void childEvent(CuratorFramework framework, PathChildrenCacheEvent event) throws Exception {

        if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED))
        {
            String path = event.getData().getPath();

            logger.info("CHILD_ADDED for " + path);
            if ((StringUtils.countMatches(path, "/") == 2) && path.contains(MGM_SETTINGS))
            {
                updateMgmSettings(framework, event);
            }
            else
            {
                newClientEvent(framework, event);
            }
        } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
            String path = event.getData().getPath();

            logger.info("CHILD_UPDATED for " + path);
                if ((StringUtils.countMatches(path, "/") == 2) && path.contains(MGM_SETTINGS))
                {
                    updateMgmSettings(framework, event);
                }
                else
                {
                    updateExistingClientEvent(event);
                }
        } else {
            if (event != null) {
                logger.warn("Child event received for type " + event.getType() + " with data " + event.getData());
            }
        }


    }


    private void newClientEvent(CuratorFramework framework, PathChildrenCacheEvent event)
    {
        String path = event.getData().getPath();
        if ((StringUtils.countMatches(path, "/") == 3)  && path.contains(CLIENTS_MGM))
        {
            logger.info("Creating a new client for path " + path);
            try
            {
                PathChildrenCache cache = new PathChildrenCache(framework, path, true);
                caches.add(cache);
                String[] types = {ALG_MULTIVARIATE_TEST_PATH, ALG_PATH, FRONTEND_MULTIVARIATE_TEST_PATH, ON_OFF_PATH};
                for (String type : types)
                {
                    EnsurePath ensureMvTestPath = new EnsurePath(path + type);
                    ensureMvTestPath.ensure(framework.getZookeeperClient());
                }
                cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
                cache.getListenable().addListener(this);
                logger.info("Watching path " + path);
            }
            catch (Exception e)
            {
                logger.warn("Couldn't start listener for path "+ path, e);
            }
        }
    }


    private void updateExistingClientEvent(PathChildrenCacheEvent event) throws Exception
    {
        byte[] json = event.getData().getData();
        String path = event.getData().getPath();

        if(json.length > 0)
        {
            ObjectMapper mapper = new ObjectMapper();
            String client = path.split("/")[3];

            if(path.contains(ALG_MULTIVARIATE_TEST_PATH))
            {
                setupStrategyMultivariateTest(json,path,mapper,client);
            }
            else if(path.contains(ALG_PATH))
            {
                clientStrategyUpdate(json,path,mapper,client);
            }
            else if(path.contains(FRONTEND_MULTIVARIATE_TEST_PATH))
            {
                setupFrontendMultivariateTest(json,path,mapper);
            }
            else if(path.contains(ON_OFF_PATH))
            {
                startStopMultivariateTest(json,path,mapper,client);
            }
        }
    }


    private void clientStrategyUpdate(byte[] json, String path, ObjectMapper mapper, String client) throws Exception
    {
        logger.info("Received "+new String(json)+ " of length " +json.length + " from path " + path);

        ZkMgmAlgMessage algs = mapper.readValue(json, ZkMgmAlgMessage.class);

        //backwards compatible
        if (algs.filters != null)
        {
            if (!algs.filters.isEmpty())
            {
                algs.excFilters = algs.filters;
            }
        }

        SocialRecommendationStrategy.StrategyAim aim = algs.aim==null?
                null :
                SocialRecommendationStrategy.StrategyAim.valueOf(algs.aim);
        SocialRecommendationStrategy strategy = SocialRecommendationStrategy.build(
                algsFromString(algs.onlineAlgs, FacebookUsersAlgorithm.class),
                algsFromString(algs.offlineAlgs, FacebookUsersAlgorithm.class),
                algsFromString(algs.incFilters, FacebookUsersAlgorithm.class),
                algsFromString(algs.excFilters, FacebookUsersAlgorithm.class),
                blenderFromString(algs.blender),
                decayFunctionsFromString(algs.decayFunctions, SocialFriendsScoreDecayFunction.class),
                aim,
                new String (json)
        );
        strategyStore.getClientToAlgorithmStore().put(client, strategy);
    }


    private void startStopMultivariateTest(byte[] json, String path, ObjectMapper mapper, String client) throws Exception
    {
        logger.info("Received "+new String(json)+ " of length " +json.length + " from path " + path);

        Boolean testing = mapper.readValue(json, Boolean.class);
        testingOnOrOff.put(client, testing);
        if(testing){
            if(!mvTestStore.testRunning(client)){
                MultiVariateTest<SocialRecommendationStrategy> suspended = suspendedTests.get(client);
                if(suspended!=null){
                    mvTestStore.addTest(client, suspended);
                    strategyStore.getClientToTest().put(client, suspended);
                    logger.info("Starting multi variate test for client " + client);
                } else {
                    logger.warn("Could not turn on multi variate test as none have been specified for client " + client);
                }
            }  else {
                logger.warn("Ignoring instruction to start MV test for client " + client + " as one is already running.");
            }

        } else {
            if(mvTestStore.testRunning(client)){
                mvTestStore.removeTest(client);
                suspendedTests.put(client, strategyStore.getClientToTest().remove(client));
                logger.info("Ending multi variate test for client " + client);

            } else {
                logger.warn("Could not turn off multi variate test for client " + client + " as none are running.");
            }
        }
    }


    private void setupStrategyMultivariateTest(byte[] json, String path, ObjectMapper mapper, String client) throws Exception
    {
        logger.info("Received "+new String(json)+ " of length " +json.length + " from path " + path);

        ZkMgmAlgMvTestMessage test = mapper.readValue(json, ZkMgmAlgMvTestMessage.class);
        final MultiVariateTest<SocialRecommendationStrategy> multiVariateTest = toTest(client, test, json);
        if(testingOnOrOff.get(client)!=null && testingOnOrOff.get(client)){
            logger.info("Adding new multivariate test for " + client);
            removeAnyRunningTests(client);
            strategyStore.getClientToTest().put(client, multiVariateTest);
            mvTestStore.addTest(client,multiVariateTest);
        } else {
            logger.info("Suspending new multivariate test for " + client + " until multi variate testing is turned on");
            suspendedTests.put(client, multiVariateTest);
        }
    }


    private void setupFrontendMultivariateTest(byte[] json, String path, ObjectMapper mapper) throws Exception
    {
        logger.info("Received "+new String(json)+ " of length " +json.length + " from path " + path);
        ZkMgmFrontendTestMessage test = mapper.readValue(json, ZkMgmFrontendTestMessage.class);
    }







    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }


    public MultiVariateTest<SocialRecommendationStrategy> toTest(String client, ZkMgmAlgMvTestMessage test, byte[] json) throws Exception {
        Map<MultiVariateTestVariation<SocialRecommendationStrategy>, BigDecimal> realRateToVariation = new HashMap<MultiVariateTestVariation<SocialRecommendationStrategy>, BigDecimal>();
        for(ZkMgmTestVariation entry : test.variations)
        {
            SocialRecommendationStrategy.StrategyAim aim = entry.aim==null? null : SocialRecommendationStrategy.StrategyAim.valueOf(entry.aim);
            SocialRecommendationStrategy strategy = SocialRecommendationStrategy.build(
                algsFromString(entry.onlineAlgs, FacebookUsersAlgorithm.class),
                algsFromString(entry.offlineAlgs, FacebookUsersAlgorithm.class),
                algsFromString(entry.incFilters, FacebookUsersAlgorithm.class),
                algsFromString(entry.excFilters, FacebookUsersAlgorithm.class),
                blenderFromString(entry.blender),
                decayFunctionsFromString(entry.decayFunctions, SocialFriendsScoreDecayFunction.class),
                aim,
                new String (json)
            );

            MultiVariateTestVariation<SocialRecommendationStrategy> variation = new MultiVariateTestVariation<SocialRecommendationStrategy>(
                    entry.label, strategy);
            realRateToVariation.put( variation, entry.size);
        }
        MultiVariateTest<SocialRecommendationStrategy> outputTest = new MultiVariateTest<SocialRecommendationStrategy>(client, realRateToVariation);
        return outputTest;
    }

    private SocialFriendsBlender blenderFromString(String blender) throws Exception {
        if(blender==null) return null;
        SocialFriendsBlender blenderBean = (SocialFriendsBlender) applicationContext.getBean(blender);
        if(blenderBean==null){
            throw new Exception("Couldn't deserialize blender with name "+ blender);
        }
        return blenderBean;
    }

    private <T> List<T> algsFromString(List<String> algs, Class<T> type) throws Exception {
        if(algs==null) return null;
        List<T> realAlgs = new ArrayList<T>();
        for(String alg : algs){
            T readAlg = (T) applicationContext.getBean(alg);
            if(readAlg==null){
                throw new Exception("Couldn't deserialize algorithm with name "+ alg);
            }
            realAlgs.add(readAlg);
        }
        return realAlgs;
    }


    private <T> List<T> decayFunctionsFromString(List<String> decayFunctions, Class<T> type) throws Exception {
        if(decayFunctions==null) return null;
        List<T> realDecayFunctions = new ArrayList<T>();
        for(String decayFunction : decayFunctions){
            T readDecayFunction = (T) applicationContext.getBean(decayFunction);
            if(readDecayFunction==null){
                throw new Exception("Couldn't deserialize decay function with name "+ decayFunction);
            }
            realDecayFunctions.add(readDecayFunction);
        }
        return realDecayFunctions;
    }



    private void removeAnyRunningTests(String client) {
        // destroy tests
    }

    public void shutdown(){
        logger.info("Shutting down...");
        for(PathChildrenCache cache : caches){
            try {
                cache.close();
            } catch (IOException e) {
                // who cares
            }
        }
    }
    
    public void updateMgmSettings(CuratorFramework framework, PathChildrenCacheEvent event) throws Exception {
        final String path = event.getData().getPath();
        if (path.equals(MGM_SETTINGS+MGM_CACHE_EXPIRE_SECS)) {
            if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED) ||  event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                final byte[] json = event.getData().getData();
                logger.info("Received MGM Settings ["+MGM_SETTINGS+MGM_CACHE_EXPIRE_SECS+"] with json["+new String(json)+"] of length["+json.length+"]");
                //ZkMgmAlgMessage algs = mapper.readValue(json, ZkMgmAlgMessage.class);
                ObjectMapper mapper = new ObjectMapper();
                ZkMgmCacheExpireSecsMessage cacheExpireSecsMessage = mapper.readValue(json, ZkMgmCacheExpireSecsMessage.class);
                final int cacheExpireSecs = cacheExpireSecsMessage.cacheExpireSecs;
                //logger.info("Changing MGM Settings ["+MGM_SETTINGS+MGM_CACHE_EXPIRE_SECS+"] with cacheExpireSecs["+cacheExpireSecs+"]");
                cacheExpireService.setCacheExpireSecs(cacheExpireSecs);
            }
        } else if (path.equals(MGM_SETTINGS + MGM_FRIENDS_PER_QUERY)){
            if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED) ||  event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                final byte[] json = event.getData().getData();
                logger.info("Received MGM Settings ["+MGM_SETTINGS+MGM_FRIENDS_PER_QUERY+"] with json["+new String(json)+"] of length["+json.length+"]");
                //ZkMgmAlgMessage algs = mapper.readValue(json, ZkMgmAlgMessage.class);
                ObjectMapper mapper = new ObjectMapper();
                ZkMgmNoOfFriendsToQueryMessage noOfFriendsMessage = mapper.readValue(json, ZkMgmNoOfFriendsToQueryMessage.class);
                final int noOfFriends = noOfFriendsMessage.noOfFriends;
                FacebookFriendConnectionGraphBuilder.noOfFriendsPerQuery=noOfFriends;
            }
        }
    }

    public static class ZkMgmTestVariation {
        public String label;
        public BigDecimal size;
        public String aim;
        public List<String> incFilters;
        public List<String> excFilters;
        public String blender;
        public List<String> onlineAlgs;
        public List<String> offlineAlgs;
        public List<String> filters;
        public List<String> decayFunctions;
        public Integer uniqueCode;
    }
    public static class ZkMgmAlgMvTestMessage {
        public Set<ZkMgmTestVariation> variations;
    }

    public static class ZkMgmAlgMessage  {
        public String aim;
        public List<String> incFilters;
        public List<String> excFilters;
        public String blender;
        public List<String> offlineAlgs;
        public List<String> onlineAlgs;
        public List<String> filters;
        public List<String> decayFunctions;
        public Integer uniqueCode;
    }

    public static class ZkMgmFrontendTestMessage {
        public Map<BigDecimal, ZkMgmFrontendTestVariation> rateToVariation;
    }

    public static class ZkMgmFrontendTestVariation {
        public String label;
        public String clientVariables;
    }

    public static class ZkMgmCacheExpireSecsMessage {
        public Integer cacheExpireSecs;
    }

    public static class ZkMgmNoOfFriendsToQueryMessage {
        public Integer noOfFriends;
    }

}
