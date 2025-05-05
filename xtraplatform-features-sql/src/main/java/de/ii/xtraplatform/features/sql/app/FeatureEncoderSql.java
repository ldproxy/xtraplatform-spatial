/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.crs.domain.CrsTransformer;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.MappingRule;
import de.ii.xtraplatform.features.domain.PropertyBase;
import de.ii.xtraplatform.features.domain.PropertyBase.Type;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.pipeline.FeatureEventHandlerSimple.ModifiableContext;
import de.ii.xtraplatform.features.domain.pipeline.FeatureTokenEncoderBaseSimple;
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn.Operation;
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping;
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema;
import de.ii.xtraplatform.geometries.domain.ImmutableCoordinatesTransformer;
import de.ii.xtraplatform.geometries.domain.ImmutableCoordinatesTransformer.Builder;
import de.ii.xtraplatform.geometries.domain.ImmutableCoordinatesWriterWkt;
import de.ii.xtraplatform.geometries.domain.SimpleFeatureGeometry;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.immutables.value.Value;
import org.immutables.value.Value.Modifiable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureEncoderSql
    extends FeatureTokenEncoderBaseSimple<
        SqlQuerySchema,
        SqlQueryMapping,
        ModifiableContext<SqlQuerySchema, SqlQueryMapping>,
        FeatureDataSql> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureEncoderSql.class);

  private final SqlQueryMapping mapping;
  private final EpsgCrs nativeCrs;
  private final Optional<CrsTransformer> crsTransformer;
  private final Optional<String> nullValue;

  private ModifiableFeatureDataSql currentFeature;
  private List<List<String>> currentGeometry;

  public FeatureEncoderSql(
      SqlQueryMapping mapping,
      EpsgCrs inputCrs,
      EpsgCrs nativeCrs,
      CrsTransformerFactory crsTransformerFactory,
      Optional<String> nullValue) {
    this.mapping = mapping;
    this.crsTransformer = crsTransformerFactory.getTransformer(inputCrs, nativeCrs);
    this.nativeCrs = nativeCrs;
    this.nullValue = nullValue;
  }

  @Modifiable
  @Value.Style(deepImmutablesDetection = true)
  interface FeatureEncoderSqlContext extends ModifiableContext<SqlQuerySchema, SqlQueryMapping> {}

  @Override
  public void onStart(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {}

  @Override
  public void onEnd(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {}

  @Override
  public void onFeatureStart(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    currentFeature = ModifiableFeatureDataSql.create().setMapping(mapping);
    currentFeature.addRow(mapping.getMainTable());
    LOGGER.debug("onFeatureStart: {}", context.pathAsString());
  }

  @Override
  public void onFeatureEnd(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    push(currentFeature);
    LOGGER.debug("onFeatureEnd: {}", context.pathAsString());

    currentFeature
        .getRows()
        .forEach(
            row -> {
              LOGGER.debug("push: {} {}", row.first().getFullPathAsString(), row.second());
            });
  }

  @Override
  public void onObjectStart(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    LOGGER.debug("onObjectStart: {} {}", context.pathAsString(), context.inGeometry());

    if (context.inGeometry()) {
      currentGeometry = new ArrayList<>();
    }
  }

  @Override
  public void onObjectEnd(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    LOGGER.debug("onObjectEnd: {} {}", context.pathAsString(), context.inGeometry());

    if (context.inGeometry()) {
      LOGGER.debug("geometry: {} {}", context.pathAsString(), currentGeometry);

      mapping
          .getColumnForValue(context.pathAsString(), MappingRule.Scope.W)
          .ifPresentOrElse(
              column -> {
                mapping
                    .getSchemaForValue(context.pathAsString())
                    .ifPresentOrElse(
                        schema -> {
                          String value = toWkt(schema, currentGeometry, crsTransformer, nativeCrs);

                          currentFeature.addColumn(column.first(), column.second(), value);

                          LOGGER.debug("onValue: {} {}", context.pathAsString(), value);
                        },
                        () -> {
                          LOGGER.warn("onValue: {} not found in mapping", context.pathAsString());
                        });
              },
              () -> {
                LOGGER.warn("onValue: {} not found in mapping", context.pathAsString());
              });

      currentGeometry = null;
    }
  }

  @Override
  public void onArrayStart(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    LOGGER.debug("onArrayStart: {} {}", context.pathAsString(), context.inGeometry());

    if (context.inGeometry()) {
      currentGeometry.add(new ArrayList<>());
    }
  }

  @Override
  public void onArrayEnd(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    LOGGER.debug("onArrayEnd: {} {}", context.pathAsString(), context.inGeometry());
  }

  @Override
  public void onValue(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    if (context.inGeometry() && Objects.nonNull(currentGeometry)) {
      currentGeometry.get(currentGeometry.size() - 1).add(context.value());
      LOGGER.debug(
          "onValue: {} {} {}", context.pathAsString(), context.value(), context.inGeometry());
      return;
    }

    mapping
        .getColumnForValue(context.pathAsString(), MappingRule.Scope.W)
        .ifPresentOrElse(
            column -> {
              if (column.second().hasOperation(Operation.CONSTANT)) {
                return;
              }
              boolean needsQuotes =
                  column.second().getType() == SchemaBase.Type.STRING
                      || column.second().getType() == SchemaBase.Type.DATETIME;
              /* (schemaSql.getType() == SchemaBase.Type.FEATURE_REF
              && schemaSql.getValueType().orElse(SchemaBase.Type.STRING)
              == SchemaBase.Type.STRING)*/

              String value =
                  needsQuotes && Objects.nonNull(context.value())
                      ? String.format("'%s'", context.value().replaceAll("'", "''"))
                      : context.value();

              currentFeature.addColumn(column.first(), column.second(), value);

              LOGGER.debug("onValue: {} {}", context.pathAsString(), value);
            },
            () -> {
              LOGGER.warn("onValue: {} not found in mapping", context.pathAsString());
            });
  }

  @Override
  public Class<? extends ModifiableContext<SqlQuerySchema, SqlQueryMapping>> getContextInterface() {
    return FeatureEncoderSqlContext.class;
  }

  @Override
  public ModifiableContext<SqlQuerySchema, SqlQueryMapping> createContext() {
    return ModifiableFeatureEncoderSqlContext.create();
  }

  private static String toWkt(
      FeatureSchema schema,
      List<List<String>> coordinates,
      Optional<CrsTransformer> crsTransformer,
      EpsgCrs nativeCrs) {
    SimpleFeatureGeometry geometryType =
        schema
            .getGeometryType()
            .filter(SimpleFeatureGeometry::isSpecific)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Cannot encode geometry as WKT, no specific geometry type found."));
    SimpleFeatureGeometryFromToWkt wktType =
        SimpleFeatureGeometryFromToWkt.fromSimpleFeatureGeometry(geometryType);
    Integer dimension = 2;

    StringWriter geometryWriter = new StringWriter();
    ImmutableCoordinatesTransformer.Builder coordinatesTransformerBuilder =
        ImmutableCoordinatesTransformer.builder();
    coordinatesTransformerBuilder.coordinatesWriter(
        ImmutableCoordinatesWriterWkt.of(geometryWriter, Optional.ofNullable(dimension).orElse(2)));

    coordinatesTransformerBuilder.crsTransformer(crsTransformer);

    if (dimension != null) {
      coordinatesTransformerBuilder.sourceDimension(dimension);
      coordinatesTransformerBuilder.targetDimension(dimension);
    }

    geometryWriter.append(wktType.toString());

    try {
      toWktArray(geometryType, coordinates, geometryWriter, coordinatesTransformerBuilder);
    } catch (IOException e) {

    }

    // TODO: functions from Dialect
    return String.format(
        "ST_ForcePolygonCW(ST_GeomFromText('%s',%s))", geometryWriter, nativeCrs.getCode());
  }

  // TODO: test all geo types
  private static void toWktArray(
      SimpleFeatureGeometry geometryType,
      List<List<String>> coordinates,
      Writer structureWriter,
      Builder coordinatesWriterBuilder)
      throws IOException {
    structureWriter.append("(");

    if (geometryType == SimpleFeatureGeometry.POINT) {
      Writer coordinatesWriter = coordinatesWriterBuilder.build();

      for (int i = 0; i < coordinates.get(0).size(); i++) {
        coordinatesWriter.append(coordinates.get(0).get(i));
        if (i < coordinates.get(0).size() - 1) {
          coordinatesWriter.append(",");
        }
      }

      coordinatesWriter.flush();
    }

    structureWriter.append(")");
  }

  // TODO: test all geo types
  private static void toWktArray(
      PropertySql propertySql, Writer structureWriter, Builder coordinatesWriterBuilder)
      throws IOException {
    if (propertySql.getType() == PropertyBase.Type.ARRAY) {
      structureWriter.append("(");
    }

    if (propertySql.getType() == PropertyBase.Type.ARRAY
        && !propertySql.getNestedProperties().isEmpty()
        && propertySql.getNestedProperties().get(0).getType() == Type.VALUE) {
      Writer coordinatesWriter = coordinatesWriterBuilder.build();

      for (int i = 0; i < propertySql.getNestedProperties().size(); i++) {
        coordinatesWriter.append(propertySql.getNestedProperties().get(i).getValue());
        if (i < propertySql.getNestedProperties().size() - 1) {
          coordinatesWriter.append(",");
        }
      }

      coordinatesWriter.flush();
    } else {
      for (int i = 0; i < propertySql.getNestedProperties().size(); i++) {
        PropertySql propertySql1 = propertySql.getNestedProperties().get(i);
        if (propertySql1.getType() == PropertyBase.Type.ARRAY) {
          toWktArray(propertySql1, structureWriter, coordinatesWriterBuilder);
          if (i < propertySql.getNestedProperties().size() - 1) {
            structureWriter.append(",");
          }
        }
      }
    }

    if (propertySql.getType() == PropertyBase.Type.ARRAY) {
      structureWriter.append(")");
    }
  }
}
