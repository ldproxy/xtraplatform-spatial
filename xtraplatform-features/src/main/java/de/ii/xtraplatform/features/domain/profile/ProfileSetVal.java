/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.profile;

import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaConstraints;
import de.ii.xtraplatform.features.domain.transform.ImmutablePropertyTransformation;
import java.util.List;

public class ProfileSetVal implements ProfileSet {

  public static final String VAL = "val";
  public static final String AS_CODE = "val-as-code";
  public static final String AS_TITLE = "val-as-title";

  @Override
  public String getPrefix() {
    return VAL;
  }

  @Override
  public List<String> getValues() {
    return List.of(AS_CODE, AS_TITLE);
  }

  @Override
  public void addPropertyTransformations(
      String value,
      FeatureSchema schema,
      String mediaType,
      ImmutableProfileTransformations.Builder builder) {
    if (!getValues().contains(value)) {
      return;
    }

    if (AS_TITLE.equals(value)) {
      schema.getAllNestedProperties().stream()
          .filter(p -> p.getConstraints().map(c -> c.getCodelist().isPresent()).orElse(false))
          .forEach(property -> mapToTitle(property, builder));
    }
  }

  public static boolean usesCodedValue(FeatureSchema featuretype) {
    // only consider codelist transformations in the provider constraints as the other
    // transformations are fixed and cannot be disabled.
    return featuretype.getAllNestedProperties().stream()
        .anyMatch(p -> p.getConstraints().flatMap(SchemaConstraints::getCodelist).isPresent());
  }

  public static void mapToTitle(
      FeatureSchema property, ImmutableProfileTransformations.Builder builder) {
    property
        .getConstraints()
        .flatMap(SchemaConstraints::getCodelist)
        .ifPresent(
            codelist -> {
              builder.putTransformations(
                  property.getFullPathAsString(),
                  ImmutableList.of(
                      new ImmutablePropertyTransformation.Builder().codelist(codelist).build()));
            });
  }
}
