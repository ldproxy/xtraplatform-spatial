/**
 * Copyright 2020 interactive instruments GmbH
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
import de.ii.xtraplatform.entity.api.maptobuilder.ValueBuilder;
import de.ii.xtraplatform.entity.api.maptobuilder.ValueBuilderMap;
import de.ii.xtraplatform.entity.api.maptobuilder.ValueInstance;
import de.ii.xtraplatform.entity.api.maptobuilder.encoding.ValueBuilderMapEncodingEnabled;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Value.Immutable
@Value.Style(deepImmutablesDetection = true, builder = "new", attributeBuilderDetection = true)
@ValueBuilderMapEncodingEnabled
@JsonDeserialize(builder = ImmutableFeatureTypeV2.Builder.class)
public interface FeatureTypeV2 extends ValueInstance {

    @JsonIgnore
    String getName();

    String getPath();

    String getLabel();

    String getDescription();

    //behaves exactly like Map<String, FeaturePropertyV2>, but supports mergeable builder deserialization
    // (immutables attributeBuilder does not work with maps yet)
    @JsonMerge
    ValueBuilderMap<FeaturePropertyV2, ImmutableFeaturePropertyV2.Builder> getProperties();

    Map<String, String> getAdditionalInfo();


    // custom builder to automatically use keys of types as name of FeaturePropertyV2
    abstract static class Builder implements ValueBuilder<FeatureTypeV2> {
        public abstract ImmutableFeatureTypeV2.Builder putProperties(String key,
                                                                     ImmutableFeaturePropertyV2.Builder builder);

        @JsonProperty(value = "properties")
        public ImmutableFeatureTypeV2.Builder putProperties2(String key, ImmutableFeaturePropertyV2.Builder builder) {
            return putProperties(key, builder.name(key));
        }
    }

    @Override
    default ImmutableFeatureTypeV2.Builder toBuilder() {
        return new ImmutableFeatureTypeV2.Builder().from(this);
    }

    //TODO
    @JsonIgnore
    @Value.Derived
    default Map<List<String>, List<FeaturePropertyV2>> getPropertiesByPath() {
        Map<List<String>, List<FeaturePropertyV2>> builder = new LinkedHashMap<>();

        getProperties().values()
                       .forEach(featureProperty -> {
                           //TODO
                           List<String> path = Splitter.on('/')
                                                       .omitEmptyStrings()
                                                       .splitToList(featureProperty.getPath())
                                                       .stream()
                                                       .map(element -> {
                                                           String resolvedElement = element.replaceAll("\\{.*?\\}", "");

                                                           for (Map.Entry<String, String> entry : getAdditionalInfo().entrySet()) {
                                                               String prefix = entry.getKey();
                                                               String uri = entry.getValue();
                                                               resolvedElement = resolvedElement.replaceAll(prefix + ":", uri + ":");
                                                           }

                                                           return resolvedElement;
                                                       })
                                                       .collect(Collectors.toList());

                           //TODO: better double col support
                           String column = path.get(path.size() - 1);
                           if (column.contains(":")) {
                               List<String> columns = Splitter.on(':')
                                                              .splitToList(column);

                               List<String> parentPath = path.subList(0, path.size() - 1);

                               List<String> path1 = new ImmutableList.Builder<String>().addAll(parentPath)
                                                                                       .add(columns.get(0))
                                                                                       .build();
                               List<String> path2 = new ImmutableList.Builder<String>().addAll(parentPath)
                                                                                       .add(columns.get(1))
                                                                                       .build();

                               builder.putIfAbsent(path1, new ArrayList<>());
                               builder.get(path1)
                                      .add(featureProperty);
                               builder.putIfAbsent(path2, new ArrayList<>());
                               builder.get(path2)
                                      .add(featureProperty);

                               return;
                           }


                           builder.putIfAbsent(path, new ArrayList<>());

                           builder.get(path)
                                  .add(featureProperty);
                       });

        return builder;
    }


    default List<FeaturePropertyV2> findPropertiesForPath(List<String> path) {
        return getPropertiesByPath().getOrDefault(path, ImmutableList.of());
    }

}
