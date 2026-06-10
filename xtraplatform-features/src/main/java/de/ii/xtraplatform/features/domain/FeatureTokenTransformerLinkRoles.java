/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Strips property values whose schema role declares a {@link SchemaBase.Role#getLinkRelation() link
 * relation} (e.g. {@code PREDECESSOR_INTERVAL_START}, {@code SUCCESSOR_INTERVAL_START}) from the
 * token stream so downstream encoders do not emit them as inline feature properties. The captured
 * {@code (linkRelation, value)} pairs are exposed on the {@link FeatureStream.Result} / {@link
 * FeatureStream.ResultReduced} via {@code getRoleLinks()} so the response handler can build HTTP
 * {@code Link} headers and per-feature link items in the response body.
 *
 * <p>The result-level map captures the most recent feature's role values; for the single-feature
 * endpoint that is the only feature, which is the primary use case.
 */
public class FeatureTokenTransformerLinkRoles extends FeatureTokenTransformer {

  private static final DateTimeFormatter FLEXIBLE_PARSER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd[['T'][' ']HH:mm:ss[.SSS]][X]");

  private final Consumer<Map<String, String>> roleLinksSetter;
  private Map<String, String> current = new LinkedHashMap<>();

  public FeatureTokenTransformerLinkRoles(ImmutableResult.Builder resultBuilder) {
    this.roleLinksSetter = resultBuilder::putAllRoleLinks;
  }

  public <X> FeatureTokenTransformerLinkRoles(ImmutableResultReduced.Builder<X> resultBuilder) {
    this.roleLinksSetter = resultBuilder::putAllRoleLinks;
  }

  @Override
  public void onFeatureStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    this.current = new LinkedHashMap<>();
    context.setRoleLinks(Map.of());
    super.onFeatureStart(context);
  }

  @Override
  public void onValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    Optional<String> linkRelation =
        context.schema().flatMap(SchemaBase::getRole).flatMap(SchemaBase.Role::getLinkRelation);
    if (linkRelation.isPresent() && Objects.nonNull(context.value())) {
      current.put(linkRelation.get(), normalizeToIso(context.value()));
      return;
    }
    super.onValue(context);
  }

  static String normalizeToIso(String value) {
    try {
      TemporalAccessor ta =
          FLEXIBLE_PARSER.parseBest(
              value, OffsetDateTime::from, LocalDateTime::from, LocalDate::from);
      OffsetDateTime odt;
      if (ta instanceof OffsetDateTime) {
        odt = ((OffsetDateTime) ta).withOffsetSameInstant(ZoneOffset.UTC);
      } else if (ta instanceof LocalDateTime) {
        odt = ((LocalDateTime) ta).atZone(ZoneId.of("UTC")).toOffsetDateTime();
      } else {
        odt = ((LocalDate) ta).atStartOfDay(ZoneId.of("UTC")).toOffsetDateTime();
      }
      return DateTimeFormatter.ISO_INSTANT.format(odt.toInstant());
    } catch (Throwable ignore) {
      return value;
    }
  }

  @Override
  public void onFeatureEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (!current.isEmpty()) {
      roleLinksSetter.accept(current);
      context.setRoleLinks(current);
    }
    super.onFeatureEnd(context);
  }
}
