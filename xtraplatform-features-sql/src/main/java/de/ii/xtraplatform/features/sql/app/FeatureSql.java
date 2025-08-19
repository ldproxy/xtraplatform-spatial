/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.features.domain.FeatureBase;
import de.ii.xtraplatform.features.domain.FeatureTransactions;
import de.ii.xtraplatform.features.domain.PropertyBase.Type;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.sql.domain.SchemaSql;
import java.util.Objects;
import java.util.Optional;
import org.immutables.value.Value;

// TODO: delete
@Value.Modifiable
@Value.Style(set = "*")
public interface FeatureSql extends FeatureBase<PropertySql, SchemaSql>, ObjectSql {

  default FeatureSql patchWith(FeatureSql partial) {
    ModifiableFeatureSql merged = ModifiableFeatureSql.create().from(this);

    for (PropertySql patch : partial.getProperties()) {
      Optional<PropertySql> original =
          getProperties().stream()
              .filter(o -> Objects.equals(o.getPropertyPath(), patch.getPropertyPath()))
              .findFirst();

      if (original.isPresent()) {
        PropertySql prop = original.get();
        if (prop.getSchema().filter(SchemaBase::isSpatial).isPresent()) {
          if (Objects.equals(patch.getValue(), FeatureTransactions.PATCH_NULL_VALUE)) {
            merged.getProperties().remove(prop);
            continue;
          }

          prop.type(Type.OBJECT);
          prop.geometryType(patch.getGeometryType());
          prop.getNestedProperties().clear();
          prop.getNestedProperties().addAll(patch.getNestedProperties());
          prop.value(null);
        } else if (prop.isValue()) {
          if (Objects.equals(patch.getValue(), FeatureTransactions.PATCH_NULL_VALUE)) {
            merged.getProperties().remove(prop);
            continue;
          }

          prop.value(patch.getValue());
        } else {
          // joins not supported yet
          continue;
        }

      } else if (!Objects.equals(patch.getValue(), FeatureTransactions.PATCH_NULL_VALUE)) {
        merged.addProperties(patch);
      }
    }

    return merged;
  }
}
