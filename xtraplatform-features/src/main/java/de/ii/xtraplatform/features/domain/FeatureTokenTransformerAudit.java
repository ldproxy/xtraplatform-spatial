/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.base.domain.AuditLogger;

public class FeatureTokenTransformerAudit extends FeatureTokenTransformer {

  private final String requestUuid;
  private final AuditLogger auditLogger;

  public FeatureTokenTransformerAudit(
      ImmutableResult.Builder resultBuilder, String requestUuid, AuditLogger auditLogger) {
    this.requestUuid = requestUuid;
    this.auditLogger = auditLogger;
  }

  public <X> FeatureTokenTransformerAudit(
      ImmutableResultReduced.Builder<X> resultBuilder,
      String requestUuid,
      AuditLogger auditLogger) {
    this.requestUuid = requestUuid;
    this.auditLogger = auditLogger;
  }

  @Override
  public void onStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    // Annahme: Types -> Properties mapping hat immer size==1
    if (context.mappings().size() != 1)
      throw new IllegalStateException("Types zu Properties Mapping ist ungleich 1.");

    String type = context.mappings().keySet().stream().findFirst().get();
    auditLogger.initType(requestUuid, type);

    for (FeatureSchema prop : context.mappings().get(type).getTargetSchema().getProperties()) {
      if (prop.getAudit().isPresent()) {
        auditLogger.initPropertyToValueTrack(requestUuid, prop.getName());
      }
    }

    super.onStart(context);
  }

  @Override
  public void onValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    String property = context.pathTracker().toString();
    auditLogger.appendPropertyValue(requestUuid, property, context.value());
    super.onValue(context);
  }
}
