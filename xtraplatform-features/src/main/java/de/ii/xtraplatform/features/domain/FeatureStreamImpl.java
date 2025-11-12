/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import static de.ii.xtraplatform.features.domain.transform.FeaturePropertyTransformerDateFormat.DATETIME_FORMAT;
import static de.ii.xtraplatform.features.domain.transform.FeaturePropertyTransformerDateFormat.DATE_FORMAT;
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
import de.ii.xtraplatform.streams.domain.Reactive;
import de.ii.xtraplatform.streams.domain.Reactive.Sink;
import de.ii.xtraplatform.streams.domain.Reactive.SinkReduced;
import de.ii.xtraplatform.streams.domain.Reactive.Stream;
import java.time.ZoneId;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
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
  private final boolean stepMapping;
  private final boolean stepGeometry;
  private final boolean stepCoordinates;
  private final boolean stepClean;
  private final boolean stepEtag;
  private final boolean stepMetadata;

  public FeatureStreamImpl(
      Query query,
      FeatureProviderDataV2 data,
      CrsTransformerFactory crsTransformerFactory,
      boolean nativeCrsIs3d,
      Map<String, Codelist> codelists,
      QueryRunner runner,
      boolean doTransform) {
    this.query = query;
    this.data = data;
    this.crsTransformerFactory = crsTransformerFactory;
    this.nativeCrsIs3d = nativeCrsIs3d;
    this.codelists = codelists;
    this.runner = runner;
    this.doTransform = doTransform;

    this.stepMapping =
        !query.skipPipelineSteps().contains(PipelineSteps.MAPPING)
            && !query.skipPipelineSteps().contains(PipelineSteps.ALL);
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
  }

  @Override
  public CompletionStage<Result> runWith(
      Sink<Object> sink,
      Map<String, PropertyTransformations> propertyTransformations,
      CompletableFuture<CollectionMetadata> onCollectionMetadata) {

    Map<String, PropertyTransformations> mergedTransformations =
        getMergedTransformations(propertyTransformations);

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
                    return builder.isEmpty(x instanceof byte[] ? ((byte[]) x).length <= 0 : false);
                  })
              .handleEnd(
                  (ImmutableResult.Builder builder1) -> {
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
      CompletableFuture<CollectionMetadata> onCollectionMetadata) {

    Map<String, PropertyTransformations> mergedTransformations =
        getMergedTransformations(propertyTransformations);

    BiFunction<FeatureTokenSource, Map<String, String>, Reactive.Stream<ResultReduced<X>>> stream =
        (tokenSource, virtualTables) -> {
          FeatureTokenSource source =
              doTransform
                  ? getFeatureTokenSourceTransformed(tokenSource, mergedTransformations)
                  : tokenSource;
          ImmutableResultReduced.Builder<X> resultBuilder = ImmutableResultReduced.<X>builder();
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
    FeatureTokenTransformerMappings schemaMapper =
        new FeatureTokenTransformerMappings(
            propertyTransformations, codelists, data.getNativeTimeZone().orElse(ZoneId.of("UTC")));

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
    FeatureTokenTransformerGeometry geometryMapper = new FeatureTokenTransformerGeometry();

    FeatureTokenTransformerCoordinates coordinatesMapper =
        new FeatureTokenTransformerCoordinates(crsTransformer, crsTransformerWgs84);

    FeatureTokenTransformerRemoveEmptyOptionals cleaner =
        new FeatureTokenTransformerRemoveEmptyOptionals(propertyTransformations);

    FeatureTokenSource tokenSourceTransformed = featureTokenSource;

    if (stepMapping) {
      tokenSourceTransformed = tokenSourceTransformed.via(schemaMapper);
    }
    if (stepGeometry) {
      tokenSourceTransformed = tokenSourceTransformed.via(geometryMapper);
    }
    if (stepCoordinates) {
      tokenSourceTransformed = tokenSourceTransformed.via(coordinatesMapper);
    }
    if (stepClean) {
      tokenSourceTransformed = tokenSourceTransformed.via(cleaner);
    }
    if (FeatureTokenValidator.LOGGER.isTraceEnabled()) {
      tokenSourceTransformed = tokenSourceTransformed.via(new FeatureTokenValidator());
    }

    return tokenSourceTransformed;
  }

  private Map<String, PropertyTransformations> getMergedTransformations(
      Map<String, PropertyTransformations> propertyTransformations) {
    if (query instanceof FeatureQuery) {
      FeatureQuery featureQuery = (FeatureQuery) query;

      return ImmutableMap.of(
          featureQuery.getType(),
          getPropertyTransformations(
              (FeatureQuery) query,
              Optional.ofNullable(propertyTransformations.get(featureQuery.getType()))));
    }

    if (query instanceof MultiFeatureQuery) {
      MultiFeatureQuery multiFeatureQuery = (MultiFeatureQuery) query;

      return multiFeatureQuery.getQueries().stream()
          .map(
              typeQuery ->
                  new SimpleImmutableEntry<>(
                      typeQuery.getType(),
                      getPropertyTransformations(
                          typeQuery,
                          Optional.ofNullable(propertyTransformations.get(typeQuery.getType())))))
          .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    return ImmutableMap.of();
  }

  private PropertyTransformations getPropertyTransformations(
      TypeQuery typeQuery, Optional<PropertyTransformations> propertyTransformations) {
    FeatureSchema featureSchema = data.getTypes().get(typeQuery.getType());

    if (typeQuery instanceof FeatureQuery
        && ((FeatureQuery) typeQuery).getSchemaScope() == SchemaBase.Scope.RECEIVABLE) {
      return () -> getProviderTransformations(featureSchema, SchemaBase.Scope.RECEIVABLE);
    }

    PropertyTransformations providerTransformations =
        () -> getProviderTransformations(featureSchema, SchemaBase.Scope.RETURNABLE);

    PropertyTransformations merged =
        propertyTransformations
            .map(p -> p.mergeInto(providerTransformations))
            .orElse(providerTransformations);

    return applyRename(merged);
  }

  private PropertyTransformations applyRename(PropertyTransformations propertyTransformations) {
    if (propertyTransformations.getTransformations().values().stream()
        .flatMap(Collection::stream)
        .anyMatch(propertyTransformation -> propertyTransformation.getRename().isPresent())) {
      Map<String, List<PropertyTransformation>> renamed = new LinkedHashMap<>();

      propertyTransformations
          .getTransformations()
          .forEach(
              (key, value) -> {
                Optional<String> rename =
                    value.stream()
                        .filter(
                            propertyTransformation ->
                                propertyTransformation.getRename().isPresent())
                        .map(propertyTransformation -> propertyTransformation.getRename().get())
                        .findFirst();

                if (rename.isPresent()) {
                  renamed.put(rename.get(), value);

                  String prefix = key + ".";

                  propertyTransformations
                      .getTransformations()
                      .forEach(
                          (key2, value2) -> {
                            if (key2.startsWith(prefix)) {
                              renamed.put(key2.replace(key, rename.get()), value2);
                            }
                          });
                }
              });

      return propertyTransformations.mergeInto(() -> renamed);
    }

    return propertyTransformations;
  }

  private Map<String, List<PropertyTransformation>> getProviderTransformations(
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

  private java.util.stream.Stream<Map.Entry<String, List<PropertyTransformation>>>
      getProviderTransformationsForProperty(FeatureSchema schema, SchemaBase.Scope scope) {
    if (schema.getTransformations().isEmpty()) {
      if (schema.isTemporal()) {
        return java.util.stream.Stream.of(
            Map.entry(
                schema.getFullPathAsString(),
                List.of(
                    new ImmutablePropertyTransformation.Builder()
                        .dateFormat(
                            schema.getType() == SchemaBase.Type.DATETIME
                                ? DATETIME_FORMAT
                                : DATE_FORMAT)
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
}
