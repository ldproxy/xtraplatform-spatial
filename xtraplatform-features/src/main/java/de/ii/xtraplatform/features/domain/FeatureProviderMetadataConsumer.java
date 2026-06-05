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
public interface FeatureProviderMetadataConsumer {

  void analyzeStart();

  void analyzeEnd();

  void analyzeFailed(Exception ex);

  void analyzeFailed(String exceptionCode, String exceptionText);

  void analyzeNamespace(String prefix, String uri);

  void analyzeTitle(String title);

  void analyzeAbstract(String abstrct);

  void analyzeKeywords(String... keywords);

  void analyzeFees(String fees);

  void analyzeAccessConstraints(String accessConstraints);

  void analyzeVersion(String version);

  void analyzeProviderName(String providerName);

  void analyzeProviderSite(String providerSite);

  void analyzeServiceContactIndividualName(String individualName);

  void analyzeServiceContactOrganizationName(String organizationName);

  void analyzeServiceContactPositionName(String positionName);

  void analyzeServiceContactRole(String role);

  void analyzeServiceContactPhone(String phone);

  void analyzeServiceContactFacsimile(String facsimile);

  void analyzeServiceContactDeliveryPoint(String deliveryPoint);

  void analyzeServiceContactCity(String city);

  void analyzeServiceContactAdministrativeArea(String administrativeArea);

  void analyzeServiceContactPostalCode(String postalCode);

  void analyzeServiceContactCountry(String country);

  void analyzeServiceContactEmail(String email);

  void analyzeServiceContactOnlineResource(String onlineResource);

  void analyzeServiceContactHoursOfService(String hoursOfService);

  void analyzeServiceContactInstructions(String instructions);

  void analyzeOperationGetUrl(String operation, String url);

  void analyzeOperationPostUrl(String operation, String url);

  void analyzeOperationParameter(String operation, String parameterName, String value);

  void analyzeOperationConstraint(String operation, String constraintName, String value);

  void analyzeOperationMetadata(String operation, String url);

  void analyzeInspireMetadataUrl(String metadataUrl);

  void analyzeFeatureType(String featureTypeName);

  void analyzeFeatureTypeTitle(String featureTypeName, String title);

  void analyzeFeatureTypeAbstract(String featureTypeName, String abstrct);

  void analyzeFeatureTypeKeywords(String featureTypeName, String... keywords);

  void analyzeFeatureTypeBoundingBox(String featureTypeName, BoundingBoxStrings boundingBox);

  void analyzeFeatureTypeDefaultCrs(String featureTypeName, String crs);

  void analyzeFeatureTypeOtherCrs(String featureTypeName, String crs);

  void analyzeFeatureTypeMetadataUrl(String featureTypeName, String url);
}
