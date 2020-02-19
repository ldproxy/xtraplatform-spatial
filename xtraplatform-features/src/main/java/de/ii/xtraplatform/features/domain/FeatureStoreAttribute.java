package de.ii.xtraplatform.features.domain;

import org.immutables.value.Value;

import java.util.List;
import java.util.Optional;

@Value.Immutable
public interface FeatureStoreAttribute {

    String getName();

    List<String> getPath();

    @Value.Default
    default String getQueryable() {
        return getName().replaceAll("\\[", "").replaceAll("\\]", "");
    }

    @Value.Default
    default boolean isId() {
        return false;
    }

    @Value.Default
    default boolean isSpatial() {
        return false;
    }
}
