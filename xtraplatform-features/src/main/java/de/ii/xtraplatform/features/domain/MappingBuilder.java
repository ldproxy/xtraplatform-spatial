/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.google.common.base.Joiner;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableBuilder;
import de.ii.xtraplatform.geometries.domain.GeometryType;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MappingBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(MappingBuilder.class);
  private static final Joiner PATH_JOINER = Joiner.on('/').skipNulls();

  private final NestingTrackerBase<FeatureSchema.Builder> nestingTracker;
  private final Map<String, FeatureSchema.Builder> mapping;
  private FeatureSchema.Builder last;

  public MappingBuilder() {
    this.nestingTracker = new NestingTrackerBase<>();
    this.mapping = new LinkedHashMap<>();
  }

  public List<FeatureSchema> getFeatureTypes() {
    return mapping.values().stream().map(BuildableBuilder::build).collect(Collectors.toList());
  }

  public Optional<FeatureSchema> getPrev() {
    return Optional.ofNullable(last).map(BuildableBuilder::build);
  }

  public void openType(String name, List<String> path) {
    nestingTracker.reset();

    ImmutableFeatureSchema.Builder current =
        new ImmutableFeatureSchema.Builder()
            .name(name)
            .type(SchemaBase.Type.OBJECT)
            .sourcePath(asSourcePath(path, true));

    nestingTracker.openObject(List.of(), current);

    mapping.put(name, current);

    this.last = current;

    // LOGGER.debug("TYPE {}", name);
  }

  public void openObject(String name, List<String> path, FeatureSchema.Type type) {
    nestingTracker.closeAuto(path);

    FeatureSchema.Builder current = nestingTracker.getCurrentPayload();

    ImmutableFeatureSchema.Builder next =
        new ImmutableFeatureSchema.Builder().name(name).type(type).sourcePath(asSourcePath(path));

    current.putProperties2(name, next);

    nestingTracker.openObject(path, next);

    this.last = next;

    // LOGGER.debug("{} {}", type, nestingTracker.getCurrentNestingPath());
  }

  public void addValue(String name, List<String> path, FeatureSchema.Type type) {
    addValue(name, path, type, Optional.empty(), Optional.empty(), false);
  }

  public void addValue(
      String name, List<String> path, FeatureSchema.Type type, FeatureSchema.Role role) {
    addValue(name, path, type, Optional.of(role), Optional.empty(), false);
  }

  public void addValueArray(String name, List<String> path, FeatureSchema.Type type) {
    addValue(name, path, type, Optional.empty(), Optional.empty(), true);
  }

  public void addGeometry(String name, List<String> path, GeometryType type) {
    addValue(
        name,
        path,
        FeatureSchema.Type.GEOMETRY,
        Optional.of(FeatureSchema.Role.PRIMARY_GEOMETRY),
        Optional.of(type),
        false);
  }

  private void addValue(
      String name,
      List<String> path,
      FeatureSchema.Type type,
      Optional<FeatureSchema.Role> role,
      Optional<GeometryType> geometryType,
      boolean isArray) {
    nestingTracker.closeAuto(path);

    FeatureSchema.Builder current = nestingTracker.getCurrentPayload();

    ImmutableFeatureSchema.Builder value =
        new ImmutableFeatureSchema.Builder()
            .name(name)
            .sourcePath(asSourcePath(path))
            .type(isArray ? FeatureSchema.Type.VALUE_ARRAY : type)
            .valueType(isArray ? Optional.of(type) : Optional.empty())
            .geometryType(geometryType)
            .role(role);

    current.putProperties2(name, value);

    this.last = value;

    // LOGGER.debug("{} {} {} {}", isArray ? "ARRAY" : "VALUE", type,
    // nestingTracker.getCurrentNestingPath(), path);
  }

  private String asSourcePath(List<String> path) {
    return asSourcePath(path, "");
  }

  private String asSourcePath(List<String> path, boolean isRoot) {
    return asSourcePath(path, "/");
  }

  private String asSourcePath(List<String> path, String prefix) {
    return prefix + PATH_JOINER.join(nestingTracker.relativize(path));
  }
}
