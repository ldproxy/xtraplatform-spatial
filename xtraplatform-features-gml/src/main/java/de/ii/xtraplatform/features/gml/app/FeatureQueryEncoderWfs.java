/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.app;

import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.base.domain.util.Tuple;
import de.ii.xtraplatform.cql.domain.Cql;
import de.ii.xtraplatform.cql.domain.CqlParseException;
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
import de.ii.xtraplatform.features.gml.domain.ConnectionInfoWfsHttp;
import de.ii.xtraplatform.features.gml.domain.XMLNamespaceNormalizer;
import de.ii.xtraplatform.features.gml.infra.WfsConnectorHttp;
import de.ii.xtraplatform.features.gml.infra.req.GetFeature;
import de.ii.xtraplatform.features.gml.infra.req.GetFeatureBuilder;
import de.ii.xtraplatform.features.gml.infra.req.WFS;
import de.ii.xtraplatform.features.gml.infra.req.WfsQuery;
import de.ii.xtraplatform.features.gml.infra.req.WfsQueryBuilder;
import java.net.URI;
import java.util.Map;
import javax.xml.namespace.QName;

public class FeatureQueryEncoderWfs implements FeatureQueryEncoder<String, QueryOptions> {

  private final Map<String, FeatureSchema> featureSchemas;
  private final XMLNamespaceNormalizer namespaceNormalizer;
  private final WfsRequestEncoder wfsRequestEncoder;
  private final EpsgCrs nativeCrs;
  private final FilterEncoderWfs filterEncoder;

  public FeatureQueryEncoderWfs(
      Map<String, FeatureSchema> featureSchemas,
      ConnectionInfoWfsHttp connectionInfo,
      EpsgCrs nativeCrs,
      CrsTransformerFactory crsTransformerFactory,
      Cql cql) {
    this.featureSchemas = featureSchemas;
    this.namespaceNormalizer = new XMLNamespaceNormalizer(connectionInfo.getNamespaces());

    Map<String, Map<WFS.METHOD, URI>> urls =
        ImmutableMap.of(
            "default",
            ImmutableMap.of(
                WFS.METHOD.GET,
                WfsConnectorHttp.parseAndCleanWfsUrl(connectionInfo.getUri()),
                WFS.METHOD.POST,
                WfsConnectorHttp.parseAndCleanWfsUrl(connectionInfo.getUri())));

    this.wfsRequestEncoder =
        new WfsRequestEncoder(
            connectionInfo.getVersion(),
            connectionInfo.getGmlVersion(),
            connectionInfo.getNamespaces(),
            urls);
    this.nativeCrs = nativeCrs;
    this.filterEncoder = new FilterEncoderWfs(nativeCrs, crsTransformerFactory, cql);
  }

  // TODO: add cql2 classes
  @Override
  public FeatureProviderCapabilities getCapabilities() {
    return ImmutableFeatureProviderCapabilities.builder().level(Level.DEFAULT).build();
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

  public boolean isValid(final FeatureQuery query) {
    return featureSchemas.containsKey(query.getType());
  }

  public QName getFeatureTypeName(final FeatureQuery query) {
    FeatureSchema featureSchema = featureSchemas.get(query.getType());
    String name =
        featureSchema.getSourcePath().map(sourcePath -> sourcePath.substring(1)).orElse(null);

    return new QName(
        namespaceNormalizer.getNamespaceURI(namespaceNormalizer.extractURI(name)),
        namespaceNormalizer.getLocalName(name));
  }

  public String encodeFeatureQuery(
      FeatureQuery query, Map<String, String> additionalQueryParameters) {
    return wfsRequestEncoder.getAsUrl(encodeGetFeature(query, additionalQueryParameters));
  }

  public Tuple<String, String> encodeFeatureQueryPost(
      FeatureQuery query, Map<String, String> additionalQueryParameters) {
    return wfsRequestEncoder.getAsUrlAndBody(encodeGetFeature(query, additionalQueryParameters));
  }

  GetFeature encodeGetFeature(
      final FeatureQuery query, Map<String, String> additionalQueryParameters) {
    try {
      final QName featureTypeName = getFeatureTypeName(query);

      return encodeGetFeature(
          query, featureTypeName, featureSchemas.get(query.getType()), additionalQueryParameters);
    } catch (CqlParseException e) {
      throw new IllegalArgumentException("Filter is invalid", e);
    }
  }

  public WfsRequestEncoder getWfsRequestEncoder() {
    return wfsRequestEncoder;
  }

  private GetFeature encodeGetFeature(
      FeatureQuery query,
      QName featureTypeName,
      FeatureSchema featureSchema,
      Map<String, String> additionalQueryParameters) {
    final String featureTypeNameFull =
        namespaceNormalizer.getQualifiedName(
            featureTypeName.getNamespaceURI(), featureTypeName.getLocalPart());

    final WfsQuery wfsQuery =
        new WfsQueryBuilder()
            .typeName(featureTypeNameFull)
            .crs(nativeCrs)
            .filter(
                query.getFilter().map(cqlFilter -> filterEncoder.encode(cqlFilter, featureSchema)))
            .build();
    final GetFeatureBuilder getFeature = new GetFeatureBuilder();

    getFeature.query(wfsQuery);

    if (query.getLimit() > 0) {
      getFeature.count(query.getLimit());
    }
    if (query.getOffset() > 0) {
      getFeature.startIndex(query.getOffset());
    }
    if (query.hitsOnly()) {
      getFeature.hitsOnly();
    }

    if (!additionalQueryParameters.isEmpty()) {
      getFeature.additionalOperationParameters(additionalQueryParameters);
    }

    return getFeature.build();
  }
}
