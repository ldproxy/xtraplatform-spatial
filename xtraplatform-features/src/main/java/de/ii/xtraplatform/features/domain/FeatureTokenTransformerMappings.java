/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.codelists.domain.Codelist;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.transform.DynamicTargetSchemaTransformer;
import de.ii.xtraplatform.features.domain.transform.FeatureEventBuffer;
import de.ii.xtraplatform.features.domain.transform.FeaturePropertyTransformerFlatten;
import de.ii.xtraplatform.features.domain.transform.FeaturePropertyValueTransformer;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformations;
import de.ii.xtraplatform.features.domain.transform.SchemaTransformerChain;
import de.ii.xtraplatform.features.domain.transform.TokenSliceTransformerChain;
import de.ii.xtraplatform.features.domain.transform.TransformerChain;
import de.ii.xtraplatform.geometries.domain.Geometry;
import java.time.ZoneId;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureTokenTransformerMappings extends FeatureTokenTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FeatureTokenTransformerMappings.class);

  private final Map<String, PropertyTransformations> propertyTransformations;
  private final Map<String, Codelist> codelists;
  private final ZoneId nativeTimeZone;
  private Map<String, SchemaTransformerChain> schemaTransformerChains;
  private Map<String, TokenSliceTransformerChain> sliceTransformerChains;
  private Map<String, TransformerChain<String, FeaturePropertyValueTransformer>>
      valueTransformerChains;
  private FeatureEventBuffer<
          FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>>
      downstream;
  private ModifiableContext<FeatureSchema, SchemaMapping> newContext;
  private TransformerChain<String, FeaturePropertyValueTransformer> currentValueTransformerChain;

  public FeatureTokenTransformerMappings(
      Map<String, PropertyTransformations> propertyTransformations,
      Map<String, Codelist> codelists,
      ZoneId nativeTimeZone) {
    this.propertyTransformations = propertyTransformations;
    this.codelists = codelists;
    this.nativeTimeZone = nativeTimeZone;
  }

  @Override
  protected void init() {
    super.init();
  }

  @Override
  public void onStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    this.schemaTransformerChains =
        context.mappings().entrySet().stream()
            .map(
                entry ->
                    new SimpleImmutableEntry<>(
                        entry.getKey(),
                        propertyTransformations
                            .get(entry.getKey())
                            .getSchemaTransformations(
                                entry.getValue(),
                                (!(context.query() instanceof FeatureQuery)
                                    || !((FeatureQuery) context.query()).returnsSingleFeature()))))
            .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));

    this.sliceTransformerChains =
        context.mappings().entrySet().stream()
            .map(
                entry ->
                    new SimpleImmutableEntry<>(
                        entry.getKey(),
                        propertyTransformations
                            .get(entry.getKey())
                            .getTokenSliceTransformations(
                                entry.getValue(), codelists, nativeTimeZone)))
            .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));

    this.valueTransformerChains =
        context.mappings().entrySet().stream()
            .map(
                entry ->
                    new SimpleImmutableEntry<>(
                        entry.getKey(),
                        propertyTransformations
                            .get(entry.getKey())
                            .getValueTransformations(entry.getValue(), codelists, nativeTimeZone)))
            .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));

    Map<String, SchemaMapping> newMappings =
        context.mappings().entrySet().stream()
            .map(
                entry ->
                    Map.entry(
                        entry.getKey(),
                        new ImmutableSchemaMapping.Builder()
                            .from(entry.getValue())
                            .dynamicTransformers(
                                sliceTransformerChains
                                        .get(entry.getKey())
                                        .has(PropertyTransformations.WILDCARD)
                                    ? sliceTransformerChains
                                        .get(entry.getKey())
                                        .get(PropertyTransformations.WILDCARD)
                                        .stream()
                                        .filter(
                                            transformer ->
                                                transformer
                                                    instanceof FeaturePropertyTransformerFlatten
                                            /*|| transformer
                                            instanceof FeaturePropertyTransformerConcat*/ )
                                        .map(flatten -> (DynamicTargetSchemaTransformer) flatten)
                                        .collect(Collectors.toList())
                                    : List.of())
                            .targetSchema(
                                entry
                                    .getValue()
                                    .getTargetSchema()
                                    .accept(schemaTransformerChains.get(entry.getKey()))
                                    .accept(sliceTransformerChains.get(entry.getKey())))
                            .build()))
            .collect(ImmutableMap.toImmutableMap(Entry::getKey, Entry::getValue));

    this.newContext =
        createContext()
            .setMappings(newMappings)
            .setQuery(context.query())
            .setMetadata(context.metadata())
            .setIsUseTargetPaths(true);

    this.downstream = new FeatureEventBuffer<>(getDownstream(), newContext, context.mappings());

    downstream.onStart(newContext);
  }

  @Override
  public void onEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {

    downstream.onEnd(newContext);
  }

  @Override
  public void onFeatureStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    newContext.pathTracker().track(List.of());
    newContext.setType(context.type());

    downstream.onFeatureStart(newContext);
    downstream.bufferStart();
    downstream.next(0);

    this.currentValueTransformerChain = valueTransformerChains.get(context.type());
  }

  @Override
  public void onFeatureEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    applyTokenSliceTransformers(context.type());

    downstream.bufferStop(true);

    newContext.pathTracker().track(List.of());
    downstream.onFeatureEnd(newContext);
  }

  private void applyTokenSliceTransformers(String type) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Token buffer before transformations:\n{}\n", downstream);
    }

    Map<String, String> transformed = sliceTransformerChains.get(type).transform(downstream);

    newContext.setTransformed(transformed);

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Token buffer after transformations ({}):\n{}\n", transformed, downstream);
    }
  }

  @Override
  public void onObjectStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    int pos = context.pos();
    if (pos > -1) {
      downstream.next(pos, context.parentPos());

      if (context.schema().filter(schema -> schema.isObject() || schema.isSpatial()).isPresent()) {
        FeatureSchema schema = context.schema().get();

        downstream.onObjectStart(schema.getFullPath());
      }
    }
  }

  @Override
  public void onObjectEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    int pos = context.pos();
    if (pos > -1) {
      downstream.next(pos, context.parentPos());

      if (context.schema().filter(schema -> schema.isObject() || schema.isSpatial()).isPresent()) {
        FeatureSchema schema = context.schema().get();
        downstream.onObjectEnd(schema.getFullPath());
      }
    }
  }

  @Override
  public void onArrayStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    int pos = context.pos();
    if (pos > -1) {
      downstream.next(pos, context.parentPos());

      if (context.schema().filter(schema -> schema.isArray() || schema.isSpatial()).isPresent()) {
        FeatureSchema schema = context.schema().get();

        downstream.onArrayStart(schema.getFullPath());
      }
    }
  }

  @Override
  public void onArrayEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    int pos = context.pos();
    if (pos > -1) {
      downstream.next(pos, context.parentPos());

      if (context.schema().filter(schema -> schema.isArray() || schema.isSpatial()).isPresent()) {
        FeatureSchema schema = context.schema().get();
        downstream.onArrayEnd(schema.getFullPath());
      }
    }
  }

  @Override
  public void onGeometry(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    int pos = context.pos();
    if (pos > -1) {
      downstream.next(pos, context.parentPos());

      if (context.schema().filter(FeatureSchema::isSpatial).isPresent()) {
        FeatureSchema schema = context.schema().get();
        Geometry<?> geometry = context.geometry();
        downstream.onGeometry(schema.getFullPath(), geometry);
      }
    }
  }

  @Override
  public void onValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    int pos = context.pos();
    if (pos > -1) {
      downstream.next(pos, context.parentPos());

      if (context.schema().filter(FeatureSchema::isValue).isPresent()) {
        FeatureSchema schema = context.schema().get();

        Type valueType =
            schema.isSpatial()
                ? context.valueType()
                : schema.getValueType().orElse(schema.getType());

        String path = schema.getFullPathAsString();
        String value = context.value();

        if (Objects.nonNull(value)) {
          value = currentValueTransformerChain.transform(path, value);
        }

        downstream.onValue(schema.getFullPath(), value, valueType);
      }
    }
  }
}
