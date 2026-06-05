/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra;

import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.aalto.stax.InputFactoryImpl;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import dagger.assisted.Assisted;
import dagger.assisted.AssistedInject;
import de.ii.xtraplatform.base.domain.resiliency.AbstractVolatile;
import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry;
import de.ii.xtraplatform.features.domain.ConnectionInfo;
import de.ii.xtraplatform.features.domain.Metadata;
import de.ii.xtraplatform.features.gml.app.FeatureProviderWfs;
import de.ii.xtraplatform.features.gml.app.WfsRequestEncoder;
import de.ii.xtraplatform.features.gml.domain.ConnectionInfoWfsHttp;
import de.ii.xtraplatform.features.gml.domain.FeatureProviderWfsData;
import de.ii.xtraplatform.features.gml.domain.WfsConnector;
import de.ii.xtraplatform.features.gml.infra.req.GetCapabilities;
import de.ii.xtraplatform.features.gml.infra.req.WFS;
import de.ii.xtraplatform.features.gml.infra.req.WfsOperation;
import de.ii.xtraplatform.streams.domain.Reactive;
import de.ii.xtraplatform.web.domain.Http;
import de.ii.xtraplatform.web.domain.HttpClient;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.hc.core5.net.URIBuilder;
import org.codehaus.staxmate.SMInputFactory;

/**
 * @author zahnen
 */
public class WfsConnectorHttp extends AbstractVolatile implements WfsConnector {

  public static final String CONNECTOR_TYPE = "HTTP";

  private static final SMInputFactory STAX_FACTORY = new SMInputFactory(new InputFactoryImpl());

  private final HttpClient httpClient;
  private final WfsRequestEncoder wfsRequestEncoder;
  private final Optional<Metadata> metadata;
  private Optional<Throwable> connectionError;
  private final String providerId;
  private final ConnectionInfoWfsHttp connectionInfo;

  @AssistedInject
  @SuppressWarnings("PMD.ConstructorCallsOverridableMethod")
  WfsConnectorHttp(
      Http http,
      VolatileRegistry volatileRegistry,
      @Assisted String providerId,
      @Assisted ConnectionInfoWfsHttp connectionInfo) {
    // TODO
    super(volatileRegistry);

    Map<String, Map<WFS.METHOD, URI>> urls =
        ImmutableMap.of(
            "default",
            ImmutableMap.of(
                WFS.METHOD.GET,
                parseAndCleanWfsUrl(connectionInfo.getUri()),
                WFS.METHOD.POST,
                parseAndCleanWfsUrl(connectionInfo.getUri())));

    this.wfsRequestEncoder =
        new WfsRequestEncoder(
            connectionInfo.getVersion(),
            connectionInfo.getGmlVersion(),
            connectionInfo.getNamespaces(),
            urls);

    /*
     workaround for https://github.com/interactive-instruments/ldproxy/issues/225
     TODO: remove when fixed
    */
    Optional.ofNullable(
            Strings.emptyToNull(
                connectionInfo
                    .getUri()
                    .toString()
                    .replace(FeatureProviderWfsData.PLACEHOLDER_URI, "")))
        .orElseThrow(
            () -> new IllegalArgumentException("No 'uri' given, required for WFS connection"));

    URI host = connectionInfo.getUri();

    // TODO: get maxParallelRequests and idleTimeout from connectionInfo
    this.httpClient = http.getHostClient(host, 16, 30);

    this.metadata = crawlMetadata();
    this.providerId = providerId;
    this.connectionInfo = connectionInfo;
  }

  WfsConnectorHttp() {
    super(null);
    httpClient = null;
    wfsRequestEncoder = null;
    metadata = Optional.empty();
    providerId = null;
    connectionInfo = null;
  }

  public static URI parseAndCleanWfsUrl(URI inUri) {
    URIBuilder outUri = new URIBuilder(inUri).removeQuery();

    if (inUri.getQuery() != null && !inUri.getQuery().isEmpty()) {
      for (String inParam : inUri.getQuery().split("&")) {
        String[] param = inParam.split("=");
        if (!WFS.hasKVPKey(param[0].toUpperCase(Locale.ROOT))) {
          outUri.addParameter(param[0], param[1]);
        }
      }
    }

    try {
      return outUri.build();
    } catch (URISyntaxException e) {
      return inUri;
    }
  }

  @Override
  public String getType() {
    return String.format("%s/%s", FeatureProviderWfs.PROVIDER_SUB_TYPE, CONNECTOR_TYPE);
  }

  @Override
  public void start() {
    // TODO: implement polling
    setState(State.AVAILABLE);
  }

  @Override
  public void stop() {}

  private Optional<Metadata> crawlMetadata() {
    try (InputStream inputStream = runWfsOperation(new GetCapabilities())) {
      WfsCapabilitiesAnalyzer metadataConsumer = new WfsCapabilitiesAnalyzer();
      WFSCapabilitiesParser gmlSchemaParser =
          new WFSCapabilitiesParser(metadataConsumer, STAX_FACTORY);
      gmlSchemaParser.parse(inputStream);

      this.connectionError = Optional.empty();

      return Optional.of(metadataConsumer.getMetadata());
    } catch (IOException | RuntimeException e) {
      this.connectionError = Optional.of(e);
    }

    return Optional.empty();
  }

  @Override
  public Optional<Metadata> getMetadata() {
    return metadata;
  }

  @Override
  public String getProviderId() {
    return providerId;
  }

  @Override
  public boolean isConnected() {
    return metadata.isPresent();
  }

  @Override
  public Optional<Throwable> getConnectionError() {
    return connectionError;
  }

  @Override
  public Reactive.Source<byte[]> getSourceStream(String query) {
    return httpClient.get(query);
  }

  @Override
  public Reactive.Source<byte[]> getSourceStream(String query, QueryOptions options) {
    return httpClient.get(query);
  }

  @Override
  public boolean isSameDataset(ConnectionInfo connectionInfo) {
    return Objects.equals(
        connectionInfo.getDatasetIdentifier(), this.connectionInfo.getDatasetIdentifier());
  }

  @Override
  public String getDatasetIdentifier() {
    return this.connectionInfo.getDatasetIdentifier();
  }

  @Override
  public InputStream runWfsOperation(final WfsOperation operation) {
    return httpClient.getAsInputStream(wfsRequestEncoder.getAsUrl(operation));
  }

  @Override
  public Optional<String> getInstanceId() {
    return Optional.of(providerId);
  }

  @Override
  public Optional<HealthCheck> asHealthCheck() {
    return Optional.empty();
  }
}
