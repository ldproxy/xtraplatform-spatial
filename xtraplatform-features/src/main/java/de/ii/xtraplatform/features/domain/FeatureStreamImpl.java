/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import static de.ii.xtraplatform.features.domain.transform.FeaturePropertyTransformerDateFormat.DATETIME_FORMAT;
import static de.ii.xtraplatform.features.domain.transform.PropertyTransformations.WILDCARD;

import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.base.domain.ETag;
import de.ii.xtraplatform.codelists.domain.Codelist;
import de.ii.xtraplatform.crs.domain.CrsTransformer;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import de.ii.xtraplatform.features.domain.transform.ImmutablePropertyTransformation;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformation;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformations;
import de.ii.xtraplatform.services.domain.AuditLog;
import de.ii.xtraplatform.streams.domain.Reactive;
import de.ii.xtraplatform.streams.domain.Reactive.Sink;
import de.ii.xtraplatform.streams.domain.Reactive.SinkReduced;
import de.ii.xtraplatform.streams.domain.Reactive.Stream;
import java.time.ZoneId;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

public class FeatureStreamImpl implements FeatureStream {

  private final Query query;
  private final FeatureProviderDataV2 data;
  private final CrsTransformerFactory crsTransformerFactory;
  private final boolean nativeCrsIs3d;
  private final Map<String, Codelist> codelists;
  private final QueryRunner runner;
  private final boolean doTransform;
  private final boolean stepMappingSchema;
  private final boolean stepMappingValues;
  private final boolean stepGeometry;
  private final boolean stepCoordinates;
  private final boolean stepClean;
  private final boolean stepEtag;
  private final boolean stepMetadata;
  private final boolean stepAudit;

  private final AuditLog auditLog;

  public FeatureStreamImpl(
      Query query,
      FeatureProviderDataV2 data,
      CrsTransformerFactory crsTransformerFactory,
      boolean nativeCrsIs3d,
      Map<String, Codelist> codelists,
      QueryRunner runner,
      boolean doTransform,
      AuditLog auditLog) {
    this.query = query;
    this.data = data;
    this.crsTransformerFactory = crsTransformerFactory;
    this.nativeCrsIs3d = nativeCrsIs3d;
    this.codelists = codelists;
    this.runner = runner;
    this.doTransform = doTransform;
    this.auditLog = auditLog;

    this.stepMappingSchema =
        !query.skipPipelineSteps().contains(PipelineSteps.MAPPING_SCHEMA)
            && !query.skipPipelineSteps().contains(PipelineSteps.ALL);
    this.stepMappingValues =
        stepMappingSchema || !query.skipPipelineSteps().contains(PipelineSteps.MAPPING_VALUES);
    this.stepGeometry =
        !query.skipPipelineSteps().contains(PipelineSteps.GEOMETRY)
            && !query.skipPipelineSteps().contains(PipelineSteps.ALL);
    this.stepCoordinates =
        !query.skipPipelineSteps().contains(PipelineSteps.COORDINATES)
            && !query.skipPipelineSteps().contains(PipelineSteps.ALL);
    this.stepClean =
        !query.skipPipelineSteps().contains(PipelineSteps.CLEAN)
            && !query.skipPipelineSteps().contains(PipelineSteps.ALL);
    this.stepEtag =
        !query.skipPipelineSteps().contains(PipelineSteps.ETAG)
            && !query.skipPipelineSteps().contains(PipelineSteps.ALL);
    this.stepMetadata =
        !query.skipPipelineSteps().contains(PipelineSteps.METADATA)
            && !query.skipPipelineSteps().contains(PipelineSteps.ALL);
    this.stepAudit =
        auditLog.isEnabled()
            && !query.skipPipelineSteps().contains(PipelineSteps.AUDIT)
            && !query.skipPipelineSteps().contains(PipelineSteps.ALL);
  }

  static Map<String, PropertyTransformations> getMergedTransformations(
      Map<String, FeatureSchema> featureSchemas,
      Query query,
      Map<String, PropertyTransformations> propertyTransformations) {
    if (query instanceof FeatureQuery featureQuery) {
      return ImmutableMap.of(
          featureQuery.getType(),
          getPropertyTransformations(
              featureSchemas,
              featureQuery,
              Optional.ofNullable(propertyTransformations.get(featureQuery.getType()))));
    }

    if (query instanceof MultiFeatureQuery multiFeatureQuery) {
      return multiFeatureQuery.getQueries().stream()
          .map(
              typeQuery ->
                  new SimpleImmutableEntry<>(
                      typeQuery.getType(),
                      getPropertyTransformations(
                          featureSchemas,
                          typeQuery,
                          Optional.ofNullable(propertyTransformations.get(typeQuery.getType())))))
          .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    return ImmutableMap.of();
  }

  static PropertyTransformations getPropertyTransformations(
      Map<String, FeatureSchema> featureSchemas,
      TypeQuery typeQuery,
      Optional<PropertyTransformations> propertyTransformations) {
    if (typeQuery instanceof FeatureQuery
        && ((FeatureQuery) typeQuery).getSchemaScope() == SchemaBase.Scope.RECEIVABLE) {
      return () ->
          getProviderTransformations(
              featureSchemas.get(typeQuery.getType()), SchemaBase.Scope.RECEIVABLE);
    }

    PropertyTransformations providerTransformations =
        () ->
            getProviderTransformations(
                featureSchemas.get(typeQuery.getType()), SchemaBase.Scope.RETURNABLE);

    PropertyTransformations merged =
        propertyTransformations
            .map(p -> p.mergeInto(providerTransformations))
            .orElse(providerTransformations);

    return applyRename(merged);
  }

  private static PropertyTransformations applyRename(
      PropertyTransformations propertyTransformations) {
    // Collect every rename keyed by its original full path.
    Map<String, String> renames = new LinkedHashMap<>();
    propertyTransformations
        .getTransformations()
        .forEach(
            (key, value) ->
                value.stream()
                    .filter(pt -> pt.getRename().isPresent())
                    .map(pt -> pt.getRename().get())
                    .findFirst()
                    .ifPresent(rename -> renames.put(key, rename)));

    if (renames.isEmpty()) {
      return propertyTransformations;
    }

    // Re-key every transformation by the cumulative renamed full path so that lookups
    // by the renamed target path (e.g. "qualitaetsangaben.herkunft.gmd:processStep.gmd:dateTime")
    // still find the right transformation (auto DATETIME formatter, value transformers,
    // wrap transformers, ...).
    Map<String, List<PropertyTransformation>> renamed = new LinkedHashMap<>();
    propertyTransformations
        .getTransformations()
        .forEach(
            (key, value) -> {
              String newKey = renameFullPath(key, renames);
              renamed.put(newKey, value);
            });

    return propertyTransformations.mergeInto(() -> renamed);
  }

  private static String renameFullPath(String path, Map<String, String> renames) {
    String[] segments = path.split("\\.");
    StringBuilder result = new StringBuilder();
    StringBuilder running = new StringBuilder();
    for (int i = 0; i < segments.length; i++) {
      if (i > 0) {
        running.append(".");
      }
      running.append(segments[i]);
      String renamedSegment = renames.getOrDefault(running.toString(), segments[i]);
      if (i > 0) {
        result.append(".");
      }
      result.append(renamedSegment);
    }
    return result.toString();
  }

  private static Map<String, List<PropertyTransformation>> getProviderTransformations(
      FeatureSchema featureSchema, SchemaBase.Scope scope) {
    return featureSchema
        .accept(
            scope == SchemaBase.Scope.RECEIVABLE
                ? AbstractFeatureProvider.WITH_SCOPE_RECEIVABLE
                : AbstractFeatureProvider.WITH_SCOPE_RETURNABLE)
        .accept(
            (schema, visitedProperties) ->
                java.util.stream.Stream.concat(
                        getProviderTransformationsForProperty(schema, scope),
                        visitedProperties.stream().flatMap(m -> m.entrySet().stream()))
                    .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  private static java.util.stream.Stream<Map.Entry<String, List<PropertyTransformation>>>
      getProviderTransformationsForProperty(FeatureSchema schema, SchemaBase.Scope scope) {
    if (schema.getTransformations().isEmpty()) {
      if (schema.isTemporal() && schema.getType() == SchemaBase.Type.DATETIME) {
        return java.util.stream.Stream.of(
            Map.entry(
                schema.getFullPathAsString(),
                List.of(
                    new ImmutablePropertyTransformation.Builder()
                        .dateFormat(DATETIME_FORMAT)
                        .build())));
      }
      return java.util.stream.Stream.empty();
    }

    return java.util.stream.Stream.of(
        Map.entry(
            schema.getFullPath().isEmpty() ? WILDCARD : schema.getFullPathAsString(),
            schema.getTransformations().stream()
                // TODO: mark transformations with scope?
                .filter(pt -> scope != SchemaBase.Scope.RECEIVABLE || pt.getWrap().isPresent())
                .toList()));
  }

  @Override
  public CompletionStage<Result> runWith(
      Sink<Object> sink,
      Map<String, PropertyTransformations> propertyTransformations,
      CompletableFuture<CollectionMetadata> onCollectionMetadata,
      Optional<String> requestId) {

    Map<String, PropertyTransformations> mergedTransformations =
        getMergedTransformations(data.getTypes(), query, propertyTransformations);

    BiFunction<FeatureTokenSource, Map<String, String>, Stream<Result>> stream =
        (tokenSource, virtualTables) -> {
          FeatureTokenSource source =
              doTransform
                  ? getFeatureTokenSourceTransformed(tokenSource, mergedTransformations)
                  : tokenSource;
          ImmutableResult.Builder resultBuilder = ImmutableResult.builder();
          final ETag.Incremental eTag = ETag.incremental();
          final boolean strongETag =
              query instanceof FeatureQuery
                  && ((FeatureQuery) query)
                      .getETag()
                      .filter(type -> type == ETag.Type.STRONG)
                      .isPresent();

          if (stepEtag
              && query instanceof FeatureQuery
              && ((FeatureQuery) query)
                  .getETag()
                  .filter(type -> type == ETag.Type.WEAK)
                  .isPresent()) {
            source = source.via(new FeatureTokenTransformerWeakETag(resultBuilder));
          }

          if (stepMetadata) {
            source = source.via(new FeatureTokenTransformerMetadata(resultBuilder));
          }

          FeatureTokenTransformerAudit auditTransformer = null;
          if (stepAudit) {
            if (requestId.isEmpty()) {
              LOGGER.error("Audit logging not possible, no request-id provided!");
            } else if (auditLog.logIsAvailable(requestId.get())) {
              auditTransformer = new FeatureTokenTransformerAudit(requestId.get(), auditLog);
              source = source.via(auditTransformer);
            }
          }
          final Runnable finishAuditLog =
              auditTransformer != null ? auditTransformer::appendToLog : () -> {};

          source =
              source.via(new FeatureTokenTransformerHooks(resultBuilder, onCollectionMetadata));

          Reactive.BasicStream<?, Void> basicStream =
              sink instanceof Reactive.SinkTransformed
                  ? source.to((Reactive.SinkTransformed<Object, ?>) sink)
                  : source.to(sink);

          return basicStream
              .withResult(resultBuilder.isEmpty(true).hasFeatures(false))
              .handleError(ImmutableResult.Builder::error)
              .handleItem(
                  (builder, x) -> {
                    if (strongETag && x instanceof byte[]) {
                      eTag.put((byte[]) x);
                    }
                    return builder.isEmpty(x instanceof byte[] && ((byte[]) x).length <= 0);
                  })
              .handleEnd(
                  (ImmutableResult.Builder builder1) -> {
                    finishAuditLog.run();

                    if (strongETag) {
                      builder1.eTag(eTag.build(ETag.Type.STRONG));
                    }

                    return builder1.build();
                  });
        };

    return runner.runQuery(stream, query, mergedTransformations, !doTransform);
  }

  @Override
  public <X> CompletionStage<ResultReduced<X>> runWith(
      SinkReduced<Object, X> sink,
      Map<String, PropertyTransformations> propertyTransformations,
      CompletableFuture<CollectionMetadata> onCollectionMetadata,
      Optional<String> requestId) {

    Map<String, PropertyTransformations> mergedTransformations =
        getMergedTransformations(data.getTypes(), query, propertyTransformations);

    BiFunction<FeatureTokenSource, Map<String, String>, Reactive.Stream<ResultReduced<X>>> stream =
        (tokenSource, virtualTables) -> {
          FeatureTokenSource source =
              doTransform
                  ? getFeatureTokenSourceTransformed(tokenSource, mergedTransformations)
                  : tokenSource;
          ImmutableResultReduced.Builder<X> resultBuilder = ImmutableResultReduced.builder();
          final ETag.Incremental eTag = ETag.incremental();
          final boolean strongETag =
              query instanceof FeatureQuery
                  && ((FeatureQuery) query)
                      .getETag()
                      .filter(type -> type == ETag.Type.STRONG)
                      .isPresent();

          if (stepEtag
              && query instanceof FeatureQuery
              && ((FeatureQuery) query)
                  .getETag()
                  .filter(type -> type == ETag.Type.WEAK)
                  .isPresent()) {
            source = source.via(new FeatureTokenTransformerWeakETag(resultBuilder));
          }
          if (stepMetadata) {
            source = source.via(new FeatureTokenTransformerMetadata(resultBuilder));
          }

          FeatureTokenTransformerAudit auditTransformer = null;
          if (stepAudit) {
            if (requestId.isEmpty()) {
              LOGGER.error("Audit logging not possible, no request-id provided!");
            } else if (auditLog.logIsAvailable(requestId.get())) {
              auditTransformer = new FeatureTokenTransformerAudit(requestId.get(), auditLog);
              source = source.via(auditTransformer);
            }
          }
          final Runnable finishAuditLog =
              auditTransformer != null ? auditTransformer::appendToLog : () -> {};

          source =
              source.via(new FeatureTokenTransformerHooks(resultBuilder, onCollectionMetadata));

          Reactive.BasicStream<?, X> basicStream =
              sink instanceof Reactive.SinkReducedTransformed
                  ? source.to((Reactive.SinkReducedTransformed<Object, ?, X>) sink)
                  : source.to(sink);

          return basicStream
              .withResult(resultBuilder.isEmpty(true).hasFeatures(false))
              .handleError(ImmutableResultReduced.Builder::error)
              .handleItem(
                  (builder, x) -> {
                    if (strongETag && x instanceof byte[]) {
                      eTag.put((byte[]) x);
                    }
                    return builder
                        .reduced((X) x)
                        .isEmpty(x instanceof byte[] && ((byte[]) x).length <= 0);
                  })
              .handleEnd(
                  (ImmutableResultReduced.Builder<X> xBuilder) -> {
                    finishAuditLog.run();

                    if (strongETag) {
                      xBuilder.eTag(eTag.build(ETag.Type.STRONG));
                    }
                    return xBuilder.build();
                  });
        };

    return runner.runQuery(stream, query, mergedTransformations, !doTransform);
  }

  private FeatureTokenSource getFeatureTokenSourceTransformed(
      FeatureTokenSource featureTokenSource,
      Map<String, PropertyTransformations> propertyTransformations) {
    FeatureTokenSource tokenSourceTransformed = featureTokenSource;

    if (stepMappingSchema) {
      FeatureTokenTransformerMappings schemaMapper =
          new FeatureTokenTransformerMappings(
              propertyTransformations,
              codelists,
              data.getNativeTimeZone().orElse(ZoneId.of("UTC")));
      tokenSourceTransformed = tokenSourceTransformed.via(schemaMapper);
    } else if (stepMappingValues) {
      FeatureTokenTransformerMappingValuesOnly valueMapper =
          new FeatureTokenTransformerMappingValuesOnly(
              propertyTransformations,
              codelists,
              data.getNativeTimeZone().orElse(ZoneId.of("UTC")));
      tokenSourceTransformed = tokenSourceTransformed.via(valueMapper);
    }
    if (stepGeometry) {
      FeatureTokenTransformerGeometry geometryMapper = new FeatureTokenTransformerGeometry();
      tokenSourceTransformed = tokenSourceTransformed.via(geometryMapper);
    }
    if (stepCoordinates) {
      Optional<CrsTransformer> crsTransformer =
          query
              .getCrs()
              .flatMap(
                  targetCrs ->
                      crsTransformerFactory.getTransformer(
                          data.getNativeCrs().orElse(OgcCrs.CRS84), targetCrs));
      Optional<CrsTransformer> crsTransformerWgs84 =
          query
              .getCrs()
              .flatMap(
                  targetCrs ->
                      crsTransformerFactory.getTransformer(
                          data.getNativeCrs().orElse(OgcCrs.CRS84),
                          nativeCrsIs3d ? OgcCrs.CRS84h : OgcCrs.CRS84));
      FeatureTokenTransformerCoordinates coordinatesMapper =
          new FeatureTokenTransformerCoordinates(crsTransformer, crsTransformerWgs84);
      tokenSourceTransformed = tokenSourceTransformed.via(coordinatesMapper);
    }
    if (stepClean) {
      FeatureTokenTransformerRemoveEmptyOptionals cleaner =
          new FeatureTokenTransformerRemoveEmptyOptionals(propertyTransformations);
      tokenSourceTransformed = tokenSourceTransformed.via(cleaner);
    }
    if (FeatureTokenValidator.LOGGER.isTraceEnabled()) {
      tokenSourceTransformed = tokenSourceTransformed.via(new FeatureTokenValidator());
    }

    return tokenSourceTransformed;
  }
}
