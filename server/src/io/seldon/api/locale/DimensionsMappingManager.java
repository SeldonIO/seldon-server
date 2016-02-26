package io.seldon.api.locale;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.seldon.api.state.ClientConfigHandler;
import io.seldon.api.state.ClientConfigUpdateListener;

@Component
public class DimensionsMappingManager implements ClientConfigUpdateListener {

    private static Logger logger = Logger.getLogger(DimensionsMappingManager.class.getName());

    public static class DimensionsMappingConfig {
        public Map<String, Object> mappings_by_locale;

        @Override
        public String toString() {
            return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        }
    }

    private final String DIMENSIONS_MAPPING_KEY = "dimensions_mapping";
    private ObjectMapper objMapper = new ObjectMapper();
    private Map<String, DimensionsMappingConfig> client_dimensions_mappings = new HashMap<>();

    @Autowired
    public DimensionsMappingManager(ClientConfigHandler configHandler) {
        if (configHandler != null) {
            configHandler.addListener(this);
        }
    }

    public void updateDimensionsMappingConfig(String client, String json) {

        try {
            DimensionsMappingConfig dimensionsMappingConfig = getDimensionsMappingConfigFromJson(json);
            client_dimensions_mappings.put(client, dimensionsMappingConfig);
            logger.info(String.format("Updated client_dimensions_mappings for client[%s] value[%s]", client, dimensionsMappingConfig));
        } catch (Exception e) {
            logger.error("Failed update json config!", e);
        }
    }

    public void removeDimensionsMappingConfig(String client) {
        client_dimensions_mappings.remove(client);
        logger.info(String.format("Removed client_dimensions_mappings for client[%s]", client));
    }

    public Set<Integer> getMappedDimensionsByLocale(String client, Set<Integer> dimensions, String locale) {

        Set<Integer> mapped_dimensions = dimensions;

        DimensionsMappingConfig dimensionsMappingConfig = client_dimensions_mappings.get(client);

        if ((locale != null) && (dimensionsMappingConfig != null)) {
            Map<String, Object> mappings_for_the_locale = (Map<String, Object>) dimensionsMappingConfig.mappings_by_locale.get(locale);
            if (mappings_for_the_locale != null) {
                mapped_dimensions = new HashSet<Integer>();
                for (int dimension : dimensions) {
                    Object mapped_dimension_obj = mappings_for_the_locale.get(String.valueOf(dimension));
                    // @formatter:off
                    int mapped_dimension = (mapped_dimension_obj != null) ? Integer.valueOf((String) mapped_dimension_obj) : dimension; // only map if there is an available mapping otherwise use original dimension
                    // @formatter:on
                    mapped_dimensions.add(mapped_dimension);
                }
            }
        }

        return mapped_dimensions;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    private DimensionsMappingConfig getDimensionsMappingConfigFromJson(String json) throws IOException {
        DimensionsMappingConfig dmc = null;

        dmc = objMapper.readValue(json, DimensionsMappingConfig.class);

        return dmc;
    }

    @Override
    public void configUpdated(String client, String configKey, String configValue) {
        if (configKey.equals(DIMENSIONS_MAPPING_KEY)) {
            logger.info("Received new dimensions_mapping config for " + client + ": " + configValue);
            updateDimensionsMappingConfig(client, configValue);
        }
    }

    @Override
    public void configRemoved(String client, String configKey) {
        if (configKey.equals(DIMENSIONS_MAPPING_KEY)) {
            removeDimensionsMappingConfig(client);
        }
    }

}
