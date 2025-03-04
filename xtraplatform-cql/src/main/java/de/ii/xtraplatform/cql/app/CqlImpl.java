/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.app;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdDelegatingSerializer;
import com.fasterxml.jackson.databind.util.StdConverter;
import com.github.azahnen.dagger.annotations.AutoBind;
import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.cql.domain.BooleanValue2;
import de.ii.xtraplatform.cql.domain.Cql;
import de.ii.xtraplatform.cql.domain.Cql2Expression;
import de.ii.xtraplatform.cql.domain.CqlParseException;
import de.ii.xtraplatform.cql.domain.CqlToText;
import de.ii.xtraplatform.cql.domain.Operation;
import de.ii.xtraplatform.cql.domain.TemporalFunction;
import de.ii.xtraplatform.cql.domain.TemporalLiteral;
import de.ii.xtraplatform.cql.infra.CqlTextParser;
import de.ii.xtraplatform.crs.domain.CrsInfo;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import io.dropwizard.jackson.Jackson;
import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;

@Singleton
@AutoBind
public class CqlImpl implements Cql {

  private static final Logger LOGGER = LoggerFactory.getLogger(CqlImpl.class);

  private final CqlTextParser cqlTextParser;
  private final ObjectMapper cqlJsonMapper;

  @Inject
  public CqlImpl() {
    this.cqlTextParser = new CqlTextParser();

    SimpleModule module = new SimpleModule();
    module.addSerializer(Interval.class, new StdDelegatingSerializer(new IntervalConverter()));
    module.addSerializer(Instant.class, new StdDelegatingSerializer(new InstantConverter()));
    module.setSerializerModifier(new TemporalLiteral.TemporalLiteralSerializerModifier());

    this.cqlJsonMapper =
        Jackson.newObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .registerModule(module);
  }

  @Override
  public Cql2Expression read(String cql, Format format) throws CqlParseException {
    return read(cql, format, OgcCrs.CRS84);
  }

  @Override
  public Cql2Expression read(String cql, Format format, EpsgCrs crs) throws CqlParseException {
    switch (format) {
      case TEXT:
        return cqlTextParser.parse(cql, crs);
      case JSON:
        // handle boolean value
        if (cql.trim().equalsIgnoreCase("true")) {
          return BooleanValue2.of(true);
        } else if (cql.trim().equalsIgnoreCase("false")) {
          return BooleanValue2.of(false);
        }
        cqlJsonMapper.setInjectableValues(
            new InjectableValues.Std().addValue("filterCrs", Optional.ofNullable(crs)));
        try {
          return cqlJsonMapper.readValue(cql, Operation.class);
        } catch (IOException e) {
          throw new CqlParseException(e.getMessage());
        }
    }

    throw new IllegalStateException();
  }

  @Override
  public String write(Cql2Expression cql, Format format) {
    switch (format) {
      case TEXT:
        return cql.accept(new CqlToText(), true);
      case JSON:
        try {
          return cqlJsonMapper.writeValueAsString(cql);
        } catch (IOException e) {
          throw new IllegalStateException();
        }
    }

    throw new IllegalStateException();
  }

  @Override
  public List<String> findInvalidProperties(
      Cql2Expression cqlPredicate, Collection<String> validProperties) {
    CqlPropertyChecker visitor = new CqlPropertyChecker(validProperties);

    return cqlPredicate.accept(visitor, true);
  }

  @Override
  public void checkTypes(Cql2Expression cqlPredicate, Map<String, String> propertyTypes) {
    CqlTypeChecker visitor = new CqlTypeChecker(propertyTypes, this);

    cqlPredicate.accept(visitor, true);
  }

  @Override
  public void checkCoordinates(
      Cql2Expression cqlPredicate,
      CrsTransformerFactory crsTransformerFactory,
      CrsInfo crsInfo,
      EpsgCrs filterCrs,
      EpsgCrs nativeCrs) {
    long start = System.currentTimeMillis();
    CqlCoordinateChecker visitor =
        new CqlCoordinateChecker(crsTransformerFactory, crsInfo, filterCrs, nativeCrs);

    cqlPredicate.accept(visitor, true);
    LOGGER.debug("Coordinate validation took {}ms.", System.currentTimeMillis() - start);
  }

  @Override
  public Cql2Expression mapTemporalOperators(
      Cql2Expression cqlFilter, Set<TemporalFunction> supportedOperators) {
    CqlVisitorMapTemporalOperators visitor = new CqlVisitorMapTemporalOperators(supportedOperators);

    return (Cql2Expression) cqlFilter.accept(visitor, true);
  }

  @Override
  public Cql2Expression mapEnvelopes(Cql2Expression cqlFilter, CrsInfo crsInfo) {
    CqlVisitorMapEnvelopes visitor = new CqlVisitorMapEnvelopes(crsInfo);

    return (Cql2Expression) cqlFilter.accept(visitor, true);
  }

  @Override
  public Cql2Expression mapNots(Cql2Expression cqlFilter) {
    CqlVisitorMapNots visitor = new CqlVisitorMapNots();

    return (Cql2Expression) cqlFilter.accept(visitor, true);
  }

  static class IntervalConverter extends StdConverter<Interval, List<String>> {

    @Override
    public List<String> convert(Interval value) {
      return ImmutableList.of(
          value.getStart() == Instant.MIN ? ".." : value.getStart().toString(),
          value.getEnd() == Instant.MAX ? ".." : value.getEnd().toString());
    }
  }

  static class InstantConverter extends StdConverter<Instant, String> {

    @Override
    public String convert(Instant value) {

      return value.toString();
    }
  }
}
