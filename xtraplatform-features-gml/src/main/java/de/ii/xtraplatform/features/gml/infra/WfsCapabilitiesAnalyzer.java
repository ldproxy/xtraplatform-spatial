/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra;

import com.google.common.base.Strings;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import de.ii.xtraplatform.features.domain.AbstractFeatureProviderMetadataConsumer;
import de.ii.xtraplatform.features.domain.BoundingBoxStrings;
import de.ii.xtraplatform.features.domain.ImmutableMetadata;
import de.ii.xtraplatform.features.domain.Metadata;
import de.ii.xtraplatform.features.gml.domain.XMLNamespaceNormalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

@SuppressWarnings("PMD.TooManyMethods")
public class WfsCapabilitiesAnalyzer extends AbstractFeatureProviderMetadataConsumer {

  private final XMLNamespaceNormalizer namespaceNormalizer;
  private final ImmutableMetadata.Builder metadata;
  private final List<String> usedShortNames;
  private String lastVersion;

  public WfsCapabilitiesAnalyzer() {
    super();
    this.namespaceNormalizer = new XMLNamespaceNormalizer();
    this.metadata = new ImmutableMetadata.Builder();
    this.usedShortNames = new ArrayList<>();
  }

  public Metadata getMetadata() {
    return metadata.build();
  }

  @Override
  public void analyzeNamespace(String prefix, String uri) {
    if (!prefix.isBlank() && !namespaceNormalizer.getNamespaces().containsKey(prefix)) {
      metadata.putNamespaces(prefix, uri);
      namespaceNormalizer.addNamespace(prefix, uri);
    }
  }

  @Override
  public void analyzeVersion(String version) {
    if (Objects.isNull(lastVersion) || version.compareTo(lastVersion) > 0) {
      this.lastVersion = version;
      metadata.version(version);
    }
  }

  @Override
  public void analyzeFeatureType(String featureTypeName) {
    metadata.addFeatureTypes(namespaceNormalizer.getQName(featureTypeName));
  }

  @Override
  public void analyzeFeatureTypeDefaultCrs(String featureTypeName, String crs) {
    metadata.putFeatureTypesCrs(namespaceNormalizer.getQName(featureTypeName), crs);
  }

  @Override
  public void analyzeFeatureTypeBoundingBox(String featureTypeName, BoundingBoxStrings bbox) {
    BoundingBox boundingBox =
        BoundingBox.of(
            Double.parseDouble(bbox.getXmin()),
            Double.parseDouble(bbox.getYmin()),
            Double.parseDouble(bbox.getXmax()),
            Double.parseDouble(bbox.getYmax()),
            OgcCrs.CRS84);

    metadata.putFeatureTypesBoundingBox(
        getLongFeatureTypeId(featureTypeName, namespaceNormalizer), boundingBox);

    // whether the short or long id is used as type id is decided later in WfsSchemaAnalyzer
    // we add both here as long as there is no conflict so that ExtentReaderWfs can access the
    // BoundingBox in any case
    String shortId = getShortFeatureTypeId(featureTypeName, namespaceNormalizer);
    if (!usedShortNames.contains(shortId)) {
      usedShortNames.add(shortId);
      metadata.putFeatureTypesBoundingBox(shortId, boundingBox);
    }
  }

  @Override
  public void analyzeTitle(String title) {
    metadata.label(title);
  }

  @Override
  public void analyzeAbstract(String abstrct) {
    metadata.description(abstrct);
  }

  @Override
  public void analyzeKeywords(String... keywords) {
    metadata.addKeywords(keywords);
  }

  @Override
  public void analyzeFees(String fees) {
    if (!Strings.isNullOrEmpty(fees)) {
      metadata.fees(fees);
    }
  }

  @Override
  public void analyzeAccessConstraints(String accessConstraints) {
    if (!Strings.isNullOrEmpty(accessConstraints) && !"none".equalsIgnoreCase(accessConstraints)) {
      metadata.accessConstraints(accessConstraints);
    }
  }

  @Override
  public void analyzeProviderName(String providerName) {
    metadata.contactName(providerName);
  }

  @Override
  public void analyzeProviderSite(String providerSite) {
    if (!Strings.isNullOrEmpty(providerSite)) {
      metadata.contactUrl(providerSite);
    }
  }

  @Override
  public void analyzeServiceContactEmail(String email) {
    metadata.contactEmail(email);
  }

  @Override
  public void analyzeServiceContactOnlineResource(String onlineResource) {
    if (!metadata.build().getContactUrl().isPresent() && !Strings.isNullOrEmpty(onlineResource)) {
      metadata.contactUrl(onlineResource);
    }
  }

  public static String getShortFeatureTypeId(
      String prefixedName, XMLNamespaceNormalizer namespaceNormalizer) {
    return namespaceNormalizer.getLocalName(prefixedName).toLowerCase(Locale.ROOT);
  }

  public static String getLongFeatureTypeId(
      String prefixedName, XMLNamespaceNormalizer namespaceNormalizer) {
    if (!prefixedName.contains(":")) {
      return getShortFeatureTypeId(prefixedName, namespaceNormalizer);
    }
    return String.format(
        "%s_%s",
        namespaceNormalizer.extractPrefix(prefixedName).toLowerCase(Locale.ROOT),
        namespaceNormalizer.getLocalName(prefixedName).toLowerCase(Locale.ROOT));
  }
}
