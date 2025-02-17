/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.docs.DocIgnore;
import de.ii.xtraplatform.features.domain.ConnectionInfo;
import de.ii.xtraplatform.features.gml.infra.WfsConnectorHttp;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * @examplesAll <code>
 * ```yaml
 * id: wfs
 * providerType: FEATURE
 * providerSubType: WFS
 * connectionInfo:
 *   connectorType: HTTP
 *   version: 2.0.0
 *   gmlVersion: 3.2.1
 *   namespaces:
 *     ave: http://rexample.com/ns/app/1.0
 *     wfs: http://www.opengis.net/wfs/2.0
 *     fes: http://www.opengis.net/fes/2.0
 *     gml: http://www.opengis.net/gml/3.2
 *     xsd: http://www.w3.org/2001/XMLSchema
 *     ows: http://www.opengis.net/ows/1.1
 *     xlink: http://www.w3.org/1999/xlink
 *     xsi: http://www.w3.org/2001/XMLSchema-instance
 *   uri: https://example.com/pfad/zum/wfs?
 *   method: GET
 * ```
 * </code>
 */
@Value.Immutable
@Value.Style(
    builder = "new",
    deepImmutablesDetection = true,
    attributeBuilderDetection = true,
    passAnnotations = DocIgnore.class)
@JsonDeserialize(builder = ImmutableConnectionInfoWfsHttp.Builder.class)
public interface ConnectionInfoWfsHttp extends ConnectionInfo, WfsInfo {

  enum METHOD {
    GET,
    POST
  }

  /**
   * @langEn Always `HTTP`.
   * @langDe Stets `HTTP`.
   * @default
   */
  @Override
  @Value.Derived
  default String getConnectorType() {
    return WfsConnectorHttp.CONNECTOR_TYPE;
  }

  /**
   * @langEn The URI of the GetCapabilities operation for the WFS.
   * @langDe Die URI der GetCapabilities-Operation des WFS.
   */
  @Nullable
  URI getUri();

  /**
   * @langEn The HTTP method to use, `GET` or `POST`.
   * @langDe Die bevorzugt zu verwendende HTTP-Methode, `GET` oder `POST`.
   * @default GET
   */
  @JsonProperty(
      access = JsonProperty.Access.WRITE_ONLY,
      value = "method") // means only read from json
  @Value.Default
  default METHOD getMethod() {
    return METHOD.GET;
  }

  /**
   * @langEn The user name for HTTP Basic Auth.
   * @langDe Der Benutzername.
   */
  Optional<String> getUser();

  /**
   * @langEn The base64 encoded password for HTTP Basic Auth.
   * @langDe Das mit base64 verschüsselte Passwort des Benutzers.
   */
  Optional<String> getPassword();

  @DocIgnore
  Map<String, String> getOtherUrls();

  @Override
  @JsonIgnore
  @Value.Lazy
  default String getDatasetIdentifier() {
    return Optional.ofNullable(getUri()).map(URI::toString).orElse("");
  }
}
