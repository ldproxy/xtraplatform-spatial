/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zahnen
 */
public class LoggingFeatureProviderMetadataConsumer implements FeatureProviderMetadataConsumer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(LoggingFeatureProviderMetadataConsumer.class);

  @Override
  public void analyzeStart() {
    LOGGER.debug("analyzeStart");
  }

  @Override
  public void analyzeEnd() {
    LOGGER.debug("analyzeEnd");
  }

  @Override
  public void analyzeFailed(Exception ex) {
    LOGGER.debug("analyzeFailed", ex);
  }

  @Override
  public void analyzeFailed(String exceptionCode, String exceptionText) {
    LOGGER.debug("analyzeFailed {} {}", exceptionCode, exceptionText);
  }

  @Override
  public void analyzeNamespace(String prefix, String uri) {
    LOGGER.debug("analyzeNamespace {} {}", prefix, uri);
  }

  @Override
  public void analyzeVersion(String version) {
    LOGGER.debug("analyzeVersion {}", version);
  }

  @Override
  public void analyzeTitle(String title) {
    LOGGER.debug("analyzeTitle {}", title);
  }

  @Override
  public void analyzeAbstract(String abstrct) {
    LOGGER.debug("analyzeAbstract {}", abstrct);
  }

  @Override
  public void analyzeKeywords(String... keywords) {
    LOGGER.debug("analyzeKeywords {}", (Object) keywords);
  }

  @Override
  public void analyzeFees(String fees) {
    LOGGER.debug("analyzeFees {}", fees);
  }

  @Override
  public void analyzeAccessConstraints(String accessConstraints) {
    LOGGER.debug("analyzeAccessConstraints {}", accessConstraints);
  }

  @Override
  public void analyzeProviderName(String providerName) {
    LOGGER.debug("analyzeProviderName {}", providerName);
  }

  @Override
  public void analyzeProviderSite(String providerSite) {
    LOGGER.debug("analyzeProviderSite {}", providerSite);
  }

  @Override
  public void analyzeServiceContactIndividualName(String individualName) {
    LOGGER.debug("analyzeServiceContactIndividualName {}", individualName);
  }

  @Override
  public void analyzeServiceContactOrganizationName(String organizationName) {
    LOGGER.debug("analyzeServiceContactOrganizationName {}", organizationName);
  }

  @Override
  public void analyzeServiceContactPositionName(String positionName) {
    LOGGER.debug("analyzeServiceContactPositionName {}", positionName);
  }

  @Override
  public void analyzeServiceContactRole(String role) {
    LOGGER.debug("analyzeServiceContactRole {}", role);
  }

  @Override
  public void analyzeServiceContactPhone(String phone) {
    LOGGER.debug("analyzeServiceContactPhone {}", phone);
  }

  @Override
  public void analyzeServiceContactFacsimile(String facsimile) {
    LOGGER.debug("analyzeServiceContactFacsimile {}", facsimile);
  }

  @Override
  public void analyzeServiceContactDeliveryPoint(String deliveryPoint) {
    LOGGER.debug("analyzeServiceContactDeliveryPoint {}", deliveryPoint);
  }

  @Override
  public void analyzeServiceContactCity(String city) {
    LOGGER.debug("analyzeServiceContactCity {}", city);
  }

  @Override
  public void analyzeServiceContactAdministrativeArea(String administrativeArea) {
    LOGGER.debug("analyzeServiceContactAdministrativeArea {}", administrativeArea);
  }

  @Override
  public void analyzeServiceContactPostalCode(String postalCode) {
    LOGGER.debug("analyzeServiceContactPostalCode {}", postalCode);
  }

  @Override
  public void analyzeServiceContactCountry(String country) {
    LOGGER.debug("analyzeServiceContactCountry {}", country);
  }

  @Override
  public void analyzeServiceContactEmail(String email) {
    LOGGER.debug("analyzeServiceContactEmail {}", email);
  }

  @Override
  public void analyzeServiceContactOnlineResource(String onlineResource) {
    LOGGER.debug("analyzeServiceContactOnlineResource {}", onlineResource);
  }

  @Override
  public void analyzeServiceContactHoursOfService(String hoursOfService) {
    LOGGER.debug("analyzeServiceContactHoursOfService {}", hoursOfService);
  }

  @Override
  public void analyzeServiceContactInstructions(String instructions) {
    LOGGER.debug("analyzeServiceContactInstructions {}", instructions);
  }

  @Override
  public void analyzeOperationGetUrl(String operation, String url) {
    LOGGER.debug("analyzeOperationGetUrl {} {}", operation, url);
  }

  @Override
  public void analyzeOperationPostUrl(String operation, String url) {
    LOGGER.debug("analyzeOperationPostUrl {} {}", operation, url);
  }

  @Override
  public void analyzeOperationParameter(String operation, String parameterName, String value) {
    LOGGER.debug("analyzeOperationParameter {} {} {}", operation, parameterName, value);
  }

  @Override
  public void analyzeOperationConstraint(String operation, String constraintName, String value) {
    LOGGER.debug("analyzeOperationConstraint {} {} {}", operation, constraintName, value);
  }

  @Override
  public void analyzeOperationMetadata(String operation, String url) {
    LOGGER.debug("analyzeOperationMetadata {} {}", operation, url);
  }

  @Override
  public void analyzeInspireMetadataUrl(String metadataUrl) {
    LOGGER.debug("analyzeInspireMetadataUrl {}", metadataUrl);
  }

  @Override
  public void analyzeFeatureType(String featureTypeName) {
    LOGGER.debug("analyzeFeatureType {}", featureTypeName);
  }

  @Override
  public void analyzeFeatureTypeTitle(String featureTypeName, String title) {
    LOGGER.debug("analyzeFeatureTypeTitle {} {}", featureTypeName, title);
  }

  @Override
  public void analyzeFeatureTypeAbstract(String featureTypeName, String abstrct) {
    LOGGER.debug("analyzeFeatureTypeAbstract {} {}", featureTypeName, abstrct);
  }

  @Override
  public void analyzeFeatureTypeKeywords(String featureTypeName, String... keywords) {
    LOGGER.debug("analyzeFeatureTypeKeywords {} {}", featureTypeName, (Object) keywords);
  }

  @Override
  public void analyzeFeatureTypeBoundingBox(
      String featureTypeName, BoundingBoxStrings boundingBox) {
    LOGGER.debug(
        "analyzeFeatureTypeBoundingBox {} {} {} {} {}",
        featureTypeName,
        boundingBox.getXmin(),
        boundingBox.getYmin(),
        boundingBox.getXmax(),
        boundingBox.getYmax());
  }

  @Override
  public void analyzeFeatureTypeDefaultCrs(String featureTypeName, String crs) {
    LOGGER.debug("analyzeFeatureTypeDefaultCrs {} {}", featureTypeName, crs);
  }

  @Override
  public void analyzeFeatureTypeOtherCrs(String featureTypeName, String crs) {
    LOGGER.debug("analyzeFeatureTypeOtherCrs {} {}", featureTypeName, crs);
  }

  @Override
  public void analyzeFeatureTypeMetadataUrl(String featureTypeName, String url) {
    LOGGER.debug("analyzeFeatureTypeMetadataUrl {} {}", featureTypeName, url);
  }
}
