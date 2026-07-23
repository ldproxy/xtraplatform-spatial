/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import de.ii.xtraplatform.entities.domain.maptobuilder.Buildable;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableBuilder;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableMap;
import de.ii.xtraplatform.features.domain.PropertiesSchema.BuilderWithName;
import java.util.Map;

public interface PropertiesSchema<
        T extends Buildable<T>,
        U extends BuilderWithName<T, U>,
        V extends PropertiesSchema<T, U, V>>
    extends Buildable<V> {

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
  BuildableMap<T, ? extends BuilderWithName<T, U>> getPropertyMap();

  interface BuilderWithName<T extends Buildable<T>, U extends BuilderWithName<T, U>>
      extends BuildableBuilder<T> {
    U name(String name);
  }

  // custom builder to automatically use keys of properties as name
  abstract class Builder<
          T extends Buildable<T>,
          U extends BuilderWithName<T, U>,
          V extends PropertiesSchema<T, U, V>>
      implements BuildableBuilder<V> {

    public abstract Builder<T, U, V> putPropertyMap(String key, U builder);

    // @JsonMerge
    @JsonProperty("properties")
    public Builder<T, U, V> putProperties2(Map<String, U> builderMap) {
      Builder<T, U, V> builder1 = null;
      for (Map.Entry<String, U> entry : builderMap.entrySet()) {
        String key = entry.getKey();
        U builder = entry.getValue();
        builder1 = putPropertyMap(key, builder.name(key));
      }
      return builder1;
      // return putPropertyMap(key, builder.name(key));
    }

    // @JsonProperty(value = "properties")
    // @JsonAnySetter
    public Builder<T, U, V> putProperties2(String key, U builder) {
      return putPropertyMap(key, builder.name(key));
    }
  }
}
