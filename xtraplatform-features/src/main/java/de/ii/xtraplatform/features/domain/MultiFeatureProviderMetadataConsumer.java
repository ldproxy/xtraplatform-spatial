/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

/**
 * @author zahnen
 */
@SuppressWarnings({"PMD.CyclomaticComplexity", "PMD.TooManyMethods"})
public class MultiFeatureProviderMetadataConsumer implements FeatureProviderMetadataConsumer {

  private final FeatureProviderMetadataConsumer[] analyzers;

  public MultiFeatureProviderMetadataConsumer(FeatureProviderMetadataConsumer... analyzers) {
    this.analyzers = analyzers.clone();
  }

  @Override
  public void analyzeStart() {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeStart();
    }
  }

  @Override
  public void analyzeEnd() {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeEnd();
    }
  }

  @Override
  public void analyzeFailed(Exception ex) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeFailed(ex);
    }
  }

  @Override
  public void analyzeFailed(String exceptionCode, String exceptionText) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeFailed(exceptionCode, exceptionText);
    }
  }

  @Override
  public void analyzeNamespace(String prefix, String uri) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeNamespace(prefix, uri);
    }
  }

  @Override
  public void analyzeVersion(String version) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeVersion(version);
    }
  }

  @Override
  public void analyzeTitle(String title) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeTitle(title);
    }
  }

  @Override
  public void analyzeAbstract(String abstrct) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeAbstract(abstrct);
    }
  }

  @Override
  public void analyzeKeywords(String... keywords) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeKeywords(keywords);
    }
  }

  @Override
  public void analyzeFees(String fees) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeFees(fees);
    }
  }

  @Override
  public void analyzeAccessConstraints(String accessConstraints) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeAccessConstraints(accessConstraints);
    }
  }

  @Override
  public void analyzeProviderName(String providerName) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeProviderName(providerName);
    }
  }

  @Override
  public void analyzeProviderSite(String providerSite) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeProviderSite(providerSite);
    }
  }

  @Override
  public void analyzeServiceContactIndividualName(String individualName) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeServiceContactIndividualName(individualName);
    }
  }

  @Override
  public void analyzeServiceContactOrganizationName(String organizationName) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeServiceContactOrganizationName(organizationName);
    }
  }

  @Override
  public void analyzeServiceContactPositionName(String positionName) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeServiceContactPositionName(positionName);
    }
  }

  @Override
  public void analyzeServiceContactRole(String role) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeServiceContactRole(role);
    }
  }

  @Override
  public void analyzeServiceContactPhone(String phone) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeServiceContactPhone(phone);
    }
  }

  @Override
  public void analyzeServiceContactFacsimile(String facsimile) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeServiceContactFacsimile(facsimile);
    }
  }

  @Override
  public void analyzeServiceContactDeliveryPoint(String deliveryPoint) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeServiceContactDeliveryPoint(deliveryPoint);
    }
  }

  @Override
  public void analyzeServiceContactCity(String city) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeServiceContactCity(city);
    }
  }

  @Override
  public void analyzeServiceContactAdministrativeArea(String administrativeArea) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeServiceContactAdministrativeArea(administrativeArea);
    }
  }

  @Override
  public void analyzeServiceContactPostalCode(String postalCode) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeServiceContactPostalCode(postalCode);
    }
  }

  @Override
  public void analyzeServiceContactCountry(String country) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeServiceContactCountry(country);
    }
  }

  @Override
  public void analyzeServiceContactEmail(String email) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeServiceContactEmail(email);
    }
  }

  @Override
  public void analyzeServiceContactOnlineResource(String onlineResource) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeServiceContactOnlineResource(onlineResource);
    }
  }

  @Override
  public void analyzeServiceContactHoursOfService(String hoursOfService) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeServiceContactHoursOfService(hoursOfService);
    }
  }

  @Override
  public void analyzeServiceContactInstructions(String instructions) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeServiceContactInstructions(instructions);
    }
  }

  @Override
  public void analyzeOperationGetUrl(String operation, String url) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeOperationGetUrl(operation, url);
    }
  }

  @Override
  public void analyzeOperationPostUrl(String operation, String url) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeOperationPostUrl(operation, url);
    }
  }

  @Override
  public void analyzeOperationParameter(String operation, String parameterName, String value) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeOperationParameter(operation, parameterName, value);
    }
  }

  @Override
  public void analyzeOperationConstraint(String operation, String constraintName, String value) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeOperationConstraint(operation, constraintName, value);
    }
  }

  @Override
  public void analyzeOperationMetadata(String operation, String url) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeOperationMetadata(operation, url);
    }
  }

  @Override
  public void analyzeInspireMetadataUrl(String metadataUrl) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeInspireMetadataUrl(metadataUrl);
    }
  }

  @Override
  public void analyzeFeatureType(String featureTypeName) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeFeatureType(featureTypeName);
    }
  }

  @Override
  public void analyzeFeatureTypeTitle(String featureTypeName, String title) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeFeatureTypeTitle(featureTypeName, title);
    }
  }

  @Override
  public void analyzeFeatureTypeAbstract(String featureTypeName, String abstrct) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeFeatureTypeAbstract(featureTypeName, abstrct);
    }
  }

  @Override
  public void analyzeFeatureTypeKeywords(String featureTypeName, String... keywords) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeFeatureTypeKeywords(featureTypeName, keywords);
    }
  }

  @Override
  @SuppressWarnings("PMD.UseObjectForClearerAPI")
  public void analyzeFeatureTypeBoundingBox(
      String featureTypeName, String xmin, String ymin, String xmax, String ymax) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeFeatureTypeBoundingBox(featureTypeName, xmin, ymin, xmax, ymax);
    }
  }

  @Override
  public void analyzeFeatureTypeDefaultCrs(String featureTypeName, String crs) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeFeatureTypeDefaultCrs(featureTypeName, crs);
    }
  }

  @Override
  public void analyzeFeatureTypeOtherCrs(String featureTypeName, String crs) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeFeatureTypeOtherCrs(featureTypeName, crs);
    }
  }

  @Override
  public void analyzeFeatureTypeMetadataUrl(String featureTypeName, String url) {
    for (FeatureProviderMetadataConsumer analyzer : analyzers) {
      analyzer.analyzeFeatureTypeMetadataUrl(featureTypeName, url);
    }
  }
}
