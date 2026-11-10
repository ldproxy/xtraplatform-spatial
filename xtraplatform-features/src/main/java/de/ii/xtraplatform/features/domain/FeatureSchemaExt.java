/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.docs.DocIgnore;
import de.ii.xtraplatform.entities.domain.maptobuilder.Buildable;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableBuilder;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableMap;
import de.ii.xtraplatform.entities.domain.maptobuilder.encoding.BuildableMapEncodingEnabled;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(
    builder = "new",
    deepImmutablesDetection = true,
    attributeBuilderDetection = true,
    passAnnotations = DocIgnore.class)
@BuildableMapEncodingEnabled
@JsonDeserialize(builder = ImmutableFeatureSchemaExt.Builder.class)
@JsonPropertyOrder({
  "sourcePath",
  "name",
  "type",
  "schema",
  "role",
  "valueType",
  "geometryType",
  "objectType",
  "unit",
  "ignore",
  "transformations",
  "constraints",
  "properties"
})
public interface FeatureSchemaExt
    extends FeatureSchemaBase<FeatureSchemaExt>, Buildable<FeatureSchemaExt> {

  @Override
  @Value.Default
  default Type getType() {
    return Type.UNKNOWN;
  }

  Optional<String> getSchema();

  @Value.Default
  default boolean getIgnore() {
    return false;
  }

  @Override
  Optional<Boolean> getForcePolygonCCW();

  /**
   * @langEn Only for `OBJECT` and `OBJECT_ARRAY`. Object with the property names as keys and schema
   *     objects as values.
   * @langDe Nur bei `OBJECT` und `OBJECT_ARRAY`. Ein Objekt mit einer Eigenschaft pro
   *     Objekteigenschaft. Der Schüssel ist der Name der Objekteigenschaft, der Wert das
   *     Schema-Objekt zu der Objekteigenschaft.
   */
  // behaves exactly like Map<String, FeaturePropertyV2>, but supports mergeable builder
  // deserialization
  // (immutables attributeBuilder does not work with maps yet)
  @JsonProperty("properties")
  @SuppressWarnings("PMD.LooseCoupling")
  BuildableMap<FeatureSchemaExt, ImmutableFeatureSchemaExt.Builder> getPropertyMap();

  // custom builder to automatically use keys of properties as name
  abstract class Builder implements BuildableBuilder<FeatureSchemaExt> {

    public abstract ImmutableFeatureSchemaExt.Builder putPropertyMap(
        String key, ImmutableFeatureSchemaExt.Builder builder);

    // @JsonMerge
    @JsonProperty("properties")
    public ImmutableFeatureSchemaExt.Builder putProperties2(
        Map<String, ImmutableFeatureSchemaExt.Builder> builderMap) {
      ImmutableFeatureSchemaExt.Builder builder1 = null;
      for (Map.Entry<String, ImmutableFeatureSchemaExt.Builder> entry : builderMap.entrySet()) {
        String key = entry.getKey();
        ImmutableFeatureSchemaExt.Builder builder = entry.getValue();
        builder1 = putPropertyMap(key, builder.name(key));
      }
      return builder1;
      // return putPropertyMap(key, builder.name(key));
    }

    // @JsonProperty("properties")
    // @JsonAnySetter
    public ImmutableFeatureSchemaExt.Builder putProperties2(
        String key, ImmutableFeatureSchemaExt.Builder builder) {
      return putPropertyMap(key, builder.name(key));
    }
  }

  @Override
  default ImmutableFeatureSchemaExt.Builder getBuilder() {
    return new ImmutableFeatureSchemaExt.Builder().from(this);
  }

  @JsonIgnore
  @Value.Derived
  @Value.Auxiliary
  @Override
  default List<FeatureSchemaExt> getProperties() {
    return getPropertyMap().values().stream()
        .map(
            featureSchema -> {
              ImmutableFeatureSchemaExt.Builder builder =
                  new ImmutableFeatureSchemaExt.Builder().from(featureSchema);

              if (getFullPath().size() > featureSchema.getParentPath().size()) {
                builder.parentPath(getFullPath());
              }

              if (featureSchema.getPath().isEmpty()) {
                builder.addPath(featureSchema.getName());
              }

              return builder.build();
            })
        .collect(Collectors.toList());
  }
}
