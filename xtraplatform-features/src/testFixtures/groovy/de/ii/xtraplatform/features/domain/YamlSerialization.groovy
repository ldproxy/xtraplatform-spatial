package de.ii.xtraplatform.features.domain

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.io.Resources
import de.ii.xtraplatform.base.domain.JacksonProvider
import de.ii.xtraplatform.values.api.ValueEncodingJackson

import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.function.Function

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

    static final ObjectMapper YAML = YamlSerialization.createYamlMapper();

    static String fromYamlRaw(String name, String target) {
        def resource = Resources.getResource("${target}/${name}.yml");
        return Resources.toString(resource, StandardCharsets.UTF_8);
    }

    static <T> T fromYaml(Class<T> type, String name, String target) {
        def resource = Resources.getResource("${target}/${name}.yml");
        return YAML.readValue(Resources.toByteArray(resource), type);
    }

    static <T> T fromYaml(TypeReference<T> tr, String name, String target) {
        def resource = Resources.getResource("${target}/${name}.yml");
        return YAML.readValue(Resources.toByteArray(resource), tr);
    }

    static <T, U> U fromYaml(Class<T> type, String name, String target, Function<T, U> transform) {
        def val = fromYaml(type, name, target)
        return transform.apply(val)
    }

    static <T, U> U fromYaml(TypeReference<T> tr, String name, String target, Function<T, U> transform) {
        def val = fromYaml(tr, name, target)
        return transform.apply(val)
    }

    static <T> void toYaml(T value, String name, String target) {
        def resource = Resources.getResource("${target}");
        if (resource.protocol != "file") {
            throw new IllegalArgumentException("Resource must be a directory: " + resource);
        }

        YAML.writeValue(Path.of(resource.path, "${name}.yml").toFile(), value);
    }

    static String toYamlRaw(Object value) {
        return YAML.writeValueAsString(value);
    }
}
