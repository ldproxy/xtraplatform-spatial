/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonMerge;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.entities.domain.maptobuilder.Buildable;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableBuilder;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableMap;
import de.ii.xtraplatform.entities.domain.maptobuilder.encoding.BuildableMapEncodingEnabled;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(deepImmutablesDetection = true, builder = "new", attributeBuilderDetection = true)
@BuildableMapEncodingEnabled
@JsonDeserialize(builder = ImmutableFeatureTypeV2.Builder.class)
public interface FeatureTypeV2 extends Buildable<FeatureTypeV2> {

  @JsonIgnore
  String getName();

  String getPath();

  Optional<String> getLabel();

  Optional<String> getDescription();

  // behaves exactly like Map<String, FeaturePropertyV2>, but supports mergeable builder
  // deserialization
  // (immutables attributeBuilder does not work with maps yet)
  @JsonMerge
  @SuppressWarnings("PMD.LooseCoupling")
  BuildableMap<FeatureSchema, ImmutableFeatureSchema.Builder> getProperties();

  Map<String, String> getAdditionalInfo();

  // custom builder to automatically use keys of types as name of FeaturePropertyV2
  abstract class Builder implements BuildableBuilder<FeatureTypeV2> {
    public abstract ImmutableFeatureTypeV2.Builder putProperties(
        String key, ImmutableFeatureSchema.Builder builder);

    @JsonProperty("properties")
    public ImmutableFeatureTypeV2.Builder putProperties2(
        String key, ImmutableFeatureSchema.Builder builder) {
      return putProperties(key, builder.name(key));
    }
  }

  @Override
  default ImmutableFeatureTypeV2.Builder getBuilder() {
    return new ImmutableFeatureTypeV2.Builder().from(this);
  }

  // TODO
  @JsonIgnore
  @Value.Derived
  default Map<List<String>, List<FeatureSchema>> getPropertiesByPath() {
    Map<List<String>, List<FeatureSchema>> builder = new LinkedHashMap<>();

    getProperties()
        .values()
        .forEach(
            featureProperty -> {
              // TODO
              List<String> path =
                  Splitter.on('/')
                      .omitEmptyStrings()
                      .splitToList(featureProperty.getSourcePath().orElse(""))
                      .stream()
                      .map(
                          element -> {
                            String resolvedElement = element.replaceAll("\\{.*?\\}", "");

                            for (Map.Entry<String, String> entry : getAdditionalInfo().entrySet()) {
                              String prefix = entry.getKey();
                              String uri = entry.getValue();
                              resolvedElement = resolvedElement.replaceAll(prefix + ":", uri + ":");
                            }

                            return resolvedElement;
                          })
                      .collect(Collectors.toList());

              // TODO: better double col support
              String column = path.get(path.size() - 1);
              if (column.contains(":")) {
                List<String> columns = Splitter.on(':').splitToList(column);

                List<String> parentPath = path.subList(0, path.size() - 1);

                List<String> path1 =
                    new ImmutableList.Builder<String>()
                        .addAll(parentPath)
                        .add(columns.get(0))
                        .build();
                List<String> path2 =
                    new ImmutableList.Builder<String>()
                        .addAll(parentPath)
                        .add(columns.get(1))
                        .build();

                builder.putIfAbsent(path1, new ArrayList<>());
                builder.get(path1).add(featureProperty);
                builder.putIfAbsent(path2, new ArrayList<>());
                builder.get(path2).add(featureProperty);

                return;
              }

              builder.putIfAbsent(path, new ArrayList<>());

              builder.get(path).add(featureProperty);
            });

    return builder;
  }

  default List<FeatureSchema> findPropertiesForPath(List<String> path) {
    return getPropertiesByPath().getOrDefault(path, ImmutableList.of());
  }
}
