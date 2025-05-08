package de.ii.xtraplatform.features.domain


import com.fasterxml.jackson.databind.ObjectMapper
import de.ii.xtraplatform.base.domain.JacksonProvider
import de.ii.xtraplatform.values.api.ValueEncodingJackson

class YamlSerialization {

    //TODO: from jackson or store?
    static ObjectMapper createYamlMapper() {
        def jackson = new JacksonProvider(() -> Set.of(), false)
        def encoder = new ValueEncodingJackson<?>(jackson, null, false)

        /*def yamlFactory = new YAMLFactory()
                .disable(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID)
                .disable(YAMLGenerator.Feature.USE_NATIVE_OBJECT_ID)
                .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
                .enable(YAMLGenerator.Feature.INDENT_ARRAYS)

        def objectMapper = jackson.getNewObjectMapper(yamlFactory)
                .enable(SerializationFeature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED)
                .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)*/

        return encoder.getMapper(encoder.getDefaultFormat());
    }
}
