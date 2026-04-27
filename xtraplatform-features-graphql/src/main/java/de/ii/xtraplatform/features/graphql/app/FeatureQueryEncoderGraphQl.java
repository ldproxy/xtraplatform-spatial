/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.graphql.app;

import de.ii.xtraplatform.cql.domain.Cql;
import de.ii.xtraplatform.cql.domain.Cql2Expression;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.FeatureProviderCapabilities;
import de.ii.xtraplatform.features.domain.FeatureProviderCapabilities.Level;
import de.ii.xtraplatform.features.domain.FeatureProviderConnector.QueryOptions;
import de.ii.xtraplatform.features.domain.FeatureQuery;
import de.ii.xtraplatform.features.domain.FeatureQueryEncoder;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureProviderCapabilities;
import de.ii.xtraplatform.features.domain.Query;
import de.ii.xtraplatform.features.domain.TypeQuery;
import de.ii.xtraplatform.features.graphql.domain.ConnectionInfoGraphQlHttp;
import de.ii.xtraplatform.features.graphql.domain.GraphQlQueries;
import de.ii.xtraplatform.strings.domain.StringTemplateFilters;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureQueryEncoderGraphQl implements FeatureQueryEncoder<String, QueryOptions> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureQueryEncoderGraphQl.class);

  private final Map<String, FeatureSchema> featureSchemas;
  private final Map<String, List<FeatureSchema>> sourceSchemas;
  private final GraphQlQueries queryGeneration;
  private final FilterEncoderGraphQl filterEncoder;

  public FeatureQueryEncoderGraphQl(
      Map<String, FeatureSchema> featureSchemas,
      Map<String, List<FeatureSchema>> sourceSchemas,
      ConnectionInfoGraphQlHttp connectionInfo,
      GraphQlQueries queryGeneration,
      EpsgCrs nativeCrs,
      CrsTransformerFactory crsTransformerFactory,
      Cql cql) {
    this.featureSchemas = featureSchemas;
    this.sourceSchemas = sourceSchemas;
    Objects.requireNonNull(connectionInfo);
    this.queryGeneration = queryGeneration;
    this.filterEncoder =
        new FilterEncoderGraphQl(nativeCrs, crsTransformerFactory, cql, queryGeneration);
  }

  @Override
  public String encode(Query query, Map<String, String> additionalQueryParameters) {
    if (query instanceof FeatureQuery) {
      return encodeFeatureQuery((FeatureQuery) query, additionalQueryParameters);
    }

    throw new IllegalArgumentException();
  }

  @Override
  public QueryOptions getOptions(TypeQuery typeQuery, Query query) {
    return new QueryOptions() {};
  }

  @Override
  public FeatureProviderCapabilities getCapabilities() {
    // TODO: derive from queryGeneration //NOPMD ForbiddenContent
    return ImmutableFeatureProviderCapabilities.builder().level(Level.BASIC).build();
  }

  public boolean isValid(final FeatureQuery query) {
    return featureSchemas.containsKey(query.getType());
  }

  public String getTypeName(final FeatureQuery query) {
    FeatureSchema featureSchema = featureSchemas.get(query.getType());
    return featureSchema.getSourcePath().map(sourcePath -> sourcePath.substring(1)).orElse(null);
  }

  private String getNestedFields(String sourcePath) {
    if (!sourcePath.contains("/")) {
      return sourcePath;
    }

    String[] split = sourcePath.split("/");
    StringBuilder fields = new StringBuilder(split[split.length - 1]);

    for (int i = split.length - 2; i >= 0; i--) {
      fields.insert(0, " { ");
      fields.insert(0, split[i]);
      fields.append(" }");
    }

    return fields.toString();
  }

  public String getFields(FeatureSchema featureSchema, String indentation) {
    return featureSchema.getProperties().stream()
        .filter(FeatureSchema::returnable)
        .map(
            prop -> {
              if (prop.isValue()) {
                return prop.getSourcePath().map(this::getNestedFields);
              } else if (prop.isObject()) {
                return prop.getSourcePath()
                    .map(obj -> obj + " " + getFields(prop, indentation + "  "));
              }

              return Optional.<String>empty();
            })
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(
            Collectors.joining(
                "\\n  " + indentation, "{\\n  " + indentation, "\\n" + indentation + "}"));
  }

  public String encodeFeatureQuery(
      FeatureQuery query, Map<String, String> additionalQueryParameters) {
    String queryTemplate = "{\"query\":\"{\\n  %s %s\\n}\"}";
    String name =
        query.returnsSingleFeature() && queryGeneration.getSingle().isPresent()
            ? queryGeneration.getSingle().get().getName(getTypeName(query))
            : queryGeneration.getCollection().getName(getTypeName(query));
    String arguments = getArguments(query);
    String fields = getFields(sourceSchemas.get(query.getType()).get(0), "  ");

    String q = String.format(queryTemplate, name + arguments, fields);

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("GraphQL Request\n{}", q.replaceAll("\\\\n", "\n"));
    }

    return q;
  }

  private String getArguments(FeatureQuery query) {
    List<String> paging = getPaging(query.getLimit(), query.getOffset());
    List<String> filter =
        getFilter(query.getFilter(), query.getType(), query.returnsSingleFeature());

    if (paging.isEmpty() && filter.isEmpty()) {
      return "";
    }

    return Stream.concat(paging.stream(), filter.stream())
        .collect(Collectors.joining(", ", "(", ")"));
  }

  private List<String> getPaging(int limit, int offset) {
    List<String> arguments = new ArrayList<>();

    queryGeneration
        .getCollection()
        .getArguments()
        .getLimit()
        .map(
            template ->
                StringTemplateFilters.applyTemplate(
                    template, Map.of("value", String.valueOf(limit))::get))
        .ifPresent(arguments::add);

    queryGeneration
        .getCollection()
        .getArguments()
        .getOffset()
        .map(
            template ->
                StringTemplateFilters.applyTemplate(
                    template, Map.of("value", String.valueOf(offset))::get))
        .ifPresent(arguments::add);

    return arguments;
  }

  private List<String> getFilter(
      Optional<Cql2Expression> optionalFilter, String type, boolean isSingle) {
    if (optionalFilter.isEmpty()) {
      return List.of();
    }

    Map<String, String> filter =
        filterEncoder.encode(optionalFilter.get(), featureSchemas.get(type));

    if (filter.isEmpty()) {
      return List.of();
    }

    String filterString = FilterEncoderGraphQl.asString(filter, false, isSingle);

    if (isSingle) {
      return List.of(filterString);
    }

    String argument =
        StringTemplateFilters.applyTemplate(
            queryGeneration.getCollection().getArguments().getFilter().orElse("{{value}}"),
            Map.of("value", filterString)::get);

    return List.of(argument);
  }
}
