/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.crs.domain.BoundingBox;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureTokenTransformerAudit extends FeatureTokenTransformer {
  private final Consumer<Instant> lastModifiedSetter;
  private final Consumer<BoundingBox> spatialExtentSetter;
  private final Consumer<Tuple<Instant, Instant>> temporalExtentSetter;

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureTokenTransformerAudit.class);

  public FeatureTokenTransformerAudit(ImmutableResult.Builder resultBuilder) {
    this.lastModifiedSetter = resultBuilder::lastModified;
    this.spatialExtentSetter = resultBuilder::spatialExtent;
    this.temporalExtentSetter = resultBuilder::temporalExtent;
  }

  public <X> FeatureTokenTransformerAudit(ImmutableResultReduced.Builder<X> resultBuilder) {
    this.lastModifiedSetter = resultBuilder::lastModified;
    this.spatialExtentSetter = resultBuilder::spatialExtent;
    this.temporalExtentSetter = resultBuilder::temporalExtent;
  }

  private final Map<String, Map<String, Set<String>>> toAudit = new LinkedHashMap<>();

  @Override
  public void onStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    for (String type : context.mappings().keySet()) {
      Map<String, Set<String>> auditProps =
          toAudit.computeIfAbsent(type, t -> new LinkedHashMap<>());

      for (FeatureSchema prop : context.mappings().get(type).getTargetSchema().getProperties()) {
        if (prop.getAudit().isPresent()) {
          auditProps.computeIfAbsent(prop.getName(), p -> new LinkedHashSet<>());
        }
      }
    }

    super.onStart(context);
  }

  @Override
  public void onValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    String type = context.type();
    String property = context.pathTracker().toString();
    Map<String, Set<String>> props = toAudit.get(type);
    if (props != null) {
      Set<String> valueSet = props.get(property);
      if (valueSet != null) {
        valueSet.add(context.value());
      }
    }
    super.onValue(context);
  }

  @Override
  public void onEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    toAudit.forEach(
        (type, properties) ->
            properties.forEach(
                (property, values) -> {
                  if (!values.isEmpty()) {
                    LOGGER.info(
                        "Type '{}', property '{}', accessed values {}", type, property, values);
                  }
                }));
    super.onEnd(context);
  }
}
