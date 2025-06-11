package de.ii.xtraplatform.features.domain

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.io.Resources

interface YamlFeatureSchemas {

    static final ObjectMapper YAML = YamlSerialization.createYamlMapper();

    default FeatureSchema fromYaml(String name) {
        def resource = Resources.getResource("feature-schemas/" + name + ".yml");
        ImmutableFeatureSchema.Builder builder = YAML.readValue(Resources.toByteArray(resource), ImmutableFeatureSchema.Builder.class);
        return builder.name(name).build()
    }

    default void toYaml(FeatureSchema schema, String name, String target) {
        def resource = Resources.getResource("${target}/${name}.yml");
        if (resource.protocol != "file") {
            throw new IllegalArgumentException("Resource must be a file: " + resource);
        }
        YAML.writeValue(new File(resource.path), schema);
    }

    default void toYaml(FeatureSchema schema, String name) {
        toYaml(schema, name, "feature-schemas");
    }
}