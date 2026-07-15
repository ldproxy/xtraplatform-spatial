/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import com.google.common.base.Preconditions;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaBase.Role;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.SchemaVariants;
import de.ii.xtraplatform.features.domain.TypesResolver;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Validates {@code variants} declarations on geometry properties and completes the referenced
 * sibling properties: each member of a variants group carries its role in the group ({@code
 * ORIGINAL_CRS_IDENTIFIER}, {@code ORIGINAL_HEIGHT}, {@code ORIGINAL_GEOMETRY}) unless declared
 * explicitly — the roles make the members internal. Runs once per provider start, after fragment
 * resolution, so a {@code variants} block that arrives via a schema fragment is handled the same
 * way as an inline one.
 */
public class VariantsResolver implements TypesResolver {

  @Override
  public boolean needsResolving(
      FeatureSchema property, boolean isFeature, boolean isInConcat, boolean isInCoalesce) {
    Map<String, Role> impliedRoles = impliedRoles(property);
    return impliedRoles.keySet().stream()
        .anyMatch(
            name -> {
              FeatureSchema sibling = property.getPropertyMap().get(name);
              return sibling == null || sibling.getRole().isEmpty();
            });
  }

  @Override
  public FeatureSchema resolve(FeatureSchema property, List<FeatureSchema> parents) {
    property.getProperties().stream()
        .filter(prop -> prop.getVariants().isPresent())
        .forEach(prop -> validate(property, prop));

    Map<String, Role> impliedRoles = impliedRoles(property);

    ImmutableFeatureSchema.Builder builder =
        new ImmutableFeatureSchema.Builder().from(property).propertyMap(new HashMap<>());

    property
        .getPropertyMap()
        .forEach(
            (name, prop) -> {
              Role impliedRole = impliedRoles.get(prop.getName());
              if (impliedRole != null && prop.getRole().isEmpty()) {
                builder.putPropertyMap(
                    name,
                    new ImmutableFeatureSchema.Builder().from(prop).role(impliedRole).build());
              } else {
                builder.putPropertyMap(name, prop);
              }
            });

    return builder.build();
  }

  /** The group members referenced from any {@code variants} declaration, with the implied role. */
  private Map<String, Role> impliedRoles(FeatureSchema parent) {
    Map<String, Role> implied = new LinkedHashMap<>();
    parent.getProperties().stream()
        .flatMap(prop -> prop.getVariants().stream())
        .forEach(
            variants -> {
              variants
                  .getCrsProperty()
                  .ifPresent(name -> implied.put(name, Role.ORIGINAL_CRS_IDENTIFIER));
              variants
                  .getVerticalProperty()
                  .ifPresent(name -> implied.put(name, Role.ORIGINAL_HEIGHT));
              variants
                  .getGeometryProperties()
                  .forEach(name -> implied.put(name, Role.ORIGINAL_GEOMETRY));
            });
    return implied;
  }

  private Stream<String> referencedProperties(SchemaVariants variants) {
    return Stream.concat(
        Stream.concat(variants.getCrsProperty().stream(), variants.getVerticalProperty().stream()),
        variants.getGeometryProperties().stream());
  }

  private void validate(FeatureSchema parent, FeatureSchema geometryProperty) {
    SchemaVariants variants = geometryProperty.getVariants().get();

    Preconditions.checkState(
        geometryProperty.isSpatial(),
        "'variants' may only be declared on a property of type GEOMETRY. Path: %s.",
        geometryProperty.getFullPathAsString());

    referencedProperties(variants)
        .forEach(
            name ->
                Preconditions.checkState(
                    parent.getProperties().stream().anyMatch(prop -> prop.getName().equals(name)),
                    "'variants' of property '%s' references '%s', which is not a property of the same object.",
                    geometryProperty.getFullPathAsString(),
                    name));

    variants
        .getCrsProperty()
        .map(name -> sibling(parent, name))
        .ifPresent(
            prop -> {
              Preconditions.checkState(
                  prop.getType() == Type.STRING,
                  "The 'crsProperty' of a 'variants' declaration must be of type STRING. Found: %s. Path: %s.",
                  prop.getType(),
                  prop.getFullPathAsString());
              checkRole(prop, Role.ORIGINAL_CRS_IDENTIFIER);
            });

    variants
        .getVerticalProperty()
        .map(name -> sibling(parent, name))
        .ifPresent(
            prop -> {
              Preconditions.checkState(
                  prop.getType() == Type.FLOAT,
                  "The 'verticalProperty' of a 'variants' declaration must be of type FLOAT. Found: %s. Path: %s.",
                  prop.getType(),
                  prop.getFullPathAsString());
              checkRole(prop, Role.ORIGINAL_HEIGHT);
              Preconditions.checkState(
                  !prop.getOriginalCrsIdentifiers().isEmpty(),
                  "The 'verticalProperty' of a 'variants' declaration must declare the identifiers of its vertical reference systems in 'originalCrsIdentifiers'. Path: %s.",
                  prop.getFullPathAsString());
            });

    variants.getGeometryProperties().stream()
        .map(name -> sibling(parent, name))
        .forEach(
            prop -> {
              Preconditions.checkState(
                  prop.isSpatial(),
                  "A geometry variant of a 'variants' declaration must be of type GEOMETRY. Found: %s. Path: %s.",
                  prop.getType(),
                  prop.getFullPathAsString());
              Preconditions.checkState(
                  prop.getNativeCrs().isPresent(),
                  "A geometry variant of a 'variants' declaration must declare the CRS it is stored in in 'nativeCrs'. Path: %s.",
                  prop.getFullPathAsString());
              checkRole(prop, Role.ORIGINAL_GEOMETRY);
              Preconditions.checkState(
                  !prop.getOriginalCrsIdentifiers().isEmpty(),
                  "A geometry variant of a 'variants' declaration must declare the identifiers of its reference systems in 'originalCrsIdentifiers'. Path: %s.",
                  prop.getFullPathAsString());
            });

    Preconditions.checkState(
        geometryProperty.getFalseEastingDifference().isEmpty(),
        "'falseEastingDifference' may only be declared on a geometry variant (role ORIGINAL_GEOMETRY), not on the main geometry property. Path: %s.",
        geometryProperty.getFullPathAsString());
  }

  private void checkRole(FeatureSchema prop, Role expected) {
    Optional<Role> role = prop.getRole();
    Preconditions.checkState(
        role.isEmpty() || role.get() == expected,
        "A property referenced from a 'variants' declaration must have the role %s (or none, the role is implied). Found: %s. Path: %s.",
        expected,
        role.orElse(null),
        prop.getFullPathAsString());
  }

  private FeatureSchema sibling(FeatureSchema parent, String name) {
    return parent.getProperties().stream()
        .filter(prop -> prop.getName().equals(name))
        .findFirst()
        .orElseThrow();
  }
}
