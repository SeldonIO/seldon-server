package io.seldon.api.locale;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DimensionsMappingManager {

    public static class DimensionsMappingConfig {
        public Map<String, Object> mappings_by_locale;

        @Override
        public String toString() {
            return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        }
    }

    private ObjectMapper objMapper = new ObjectMapper();
    private DimensionsMappingConfig dimensionsMappingConfig = null;

    public void updateDimensionsMappingConfig(String json) {
        dimensionsMappingConfig = getDimensionsMappingConfigFromJson(json);
    }

    public Set<Integer> getMappedDimensionsByLocale(Set<Integer> dimensions, String locale) {

        Set<Integer> mapped_dimensions = dimensions;

        if (locale != null) {
            Map<String, Object> mappings_for_the_locale = (Map<String, Object>) dimensionsMappingConfig.mappings_by_locale.get(locale);
            if (mappings_for_the_locale != null) {
                mapped_dimensions = new HashSet<Integer>();
                for (int dimension : dimensions) {
                    Object mapped_dimension_obj = mappings_for_the_locale.get(String.valueOf(dimension));
                    // @formatter:off
                    int mapped_dimension = (mapped_dimension_obj != null) ? Integer.valueOf((String) mapped_dimension_obj) : dimension; // only map if there is an avaliable mapping otherwise use original dimension
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

    private DimensionsMappingConfig getDimensionsMappingConfigFromJson(String json) {
        DimensionsMappingConfig dmc = null;

        try {
            dmc = objMapper.readValue(json, DimensionsMappingConfig.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return dmc;
    }

}
