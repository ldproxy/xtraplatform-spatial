/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import de.ii.xtraplatform.features.domain.SchemaMappingBase;
import de.ii.xtraplatform.geometries.domain.GeometryType;
import java.util.List;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(deepImmutablesDetection = true, builder = "new", attributeBuilderDetection = true)
public interface SchemaMappingSql extends SchemaMappingBase<SchemaSql> {

  @Override
  default SchemaSql schemaWithGeometryType(SchemaSql schema, GeometryType geometryType) {
    return new ImmutableSchemaSql.Builder().from(schema).geometryType(geometryType).build();
  }

  @Override
  default List<String> cleanPath(List<String> path) {
    if (path.stream().anyMatch(elem -> elem.contains("{"))) {
      return path.stream().map(this::cleanPath).collect(Collectors.toList());
    }

    return path;
  }

  // TODO: static cleanup method in PathParser
  @Override
  default String cleanPath(String path) {
    if (path.contains("{")) {
      int i = path.indexOf("{");
      if (path.startsWith("filter", i + 1)) {
        return path.substring(0, i + 2) + cleanPath(path.substring(i + 2));
      }
      return path.substring(0, path.indexOf("{"));
    }
    return path;
  }
}
