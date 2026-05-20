/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.base.domain.AuditLogger;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureTokenTransformerAudit extends FeatureTokenTransformer {

  private final String requestId;
  private final AuditLogger auditLogger;

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureTokenTransformerAudit.class);

  public FeatureTokenTransformerAudit(
      ImmutableResult.Builder resultBuilder, String requestId, AuditLogger auditLogger) {
    this.requestId = requestId;
    this.auditLogger = auditLogger;
  }

  public <X> FeatureTokenTransformerAudit(
      ImmutableResultReduced.Builder<X> resultBuilder, String requestId, AuditLogger auditLogger) {
    this.requestId = requestId;
    this.auditLogger = auditLogger;
  }

  @Override
  public void onStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    for (String type : context.mappings().keySet()) {
      for (FeatureSchema prop : context.mappings().get(type).getTargetSchema().getProperties()) {
        if (prop.getAudit().isPresent()) {
          String audit = prop.getAudit().get().toLowerCase();
          switch (audit) {
            case "access":
              auditLogger.initPropertyToAccessTrack(requestId, type, prop.getName());
              continue;
            case "value":
              auditLogger.initPropertyToValueTrack(requestId, type, prop.getName());
              continue;
            case "none":
              continue;
            default:
              LOGGER.error("Wrong entry for audit: {}", audit);
          }
        }
      }
    }

    super.onStart(context);
  }

  @Override
  public void onValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    String type = context.type();
    Optional<FeatureSchema> prop =
        context.mappings().get(type).getTargetSchema().getProperties().stream()
            .filter(p -> p.getFullPathAsString().equals(context.pathTracker().toString()))
            .findFirst();

    if (prop.isPresent()) {
      if (prop.get().getAudit().isPresent()) {
        String audit = prop.get().getAudit().get().toLowerCase();
        switch (audit) {
          case "access":
            auditLogger.markPropertyAccessed(requestId, type, prop.get().getName());
            break;
          case "value":
            auditLogger.appendPropertyValue(requestId, type, prop.get().getName(), context.value());
            break;
          case "none":
            break;
          default:
            LOGGER.error("Wrong entry for audit: {}", audit);
        }
      }
    } else {
      LOGGER.error("No property found with path: {}", context.pathTracker().toString());
    }

    super.onValue(context);
  }

  @Override
  public void onEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    auditLogger.saveToFileAndRemove(requestId);
    super.onEnd(context);
  }
}
