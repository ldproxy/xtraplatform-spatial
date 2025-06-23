/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.features.domain.Decoder;
import de.ii.xtraplatform.features.domain.DecoderFactory;
import de.ii.xtraplatform.features.domain.FeatureEventHandler;
import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext;
import de.ii.xtraplatform.features.domain.FeatureQuery;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.FeatureStoreMultiplicityTracker;
import de.ii.xtraplatform.features.domain.FeatureTokenDecoder;
import de.ii.xtraplatform.features.domain.NestingTracker;
import de.ii.xtraplatform.features.domain.Query;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.SchemaMapping;
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping;
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema;
import de.ii.xtraplatform.features.sql.domain.SqlRow;
import de.ii.xtraplatform.features.sql.domain.SqlRowMeta;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureDecoderSql
    extends FeatureTokenDecoder<
        SqlRow, FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>>
    implements Decoder.Pipeline {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureDecoderSql.class);

  private final Map<String, SchemaMapping> mappings;
  private final Query query;
  private final List<List<String>> mainTablePaths;
  private final FeatureStoreMultiplicityTracker multiplicityTracker;
  private final boolean isSingleFeature;
  private final Map<String, DecoderFactory> subDecoderFactories;
  private final Map<String, Decoder> subDecoders;
  private final boolean geometryAsWkb;

  private boolean started;
  private boolean featureStarted;
  private Object currentId;
  private boolean isAtLeastOneFeatureWritten;

  private ModifiableContext<FeatureSchema, SchemaMapping> context;
  private GeometryDecoderWkt geometryDecoderWkt;
  private GeometryDecoderWkb geometryDecoderWkb;
  private NestingTracker nestingTracker;
  private Map<List<String>, Integer> schemaIndexes;

  public FeatureDecoderSql(
      Map<String, SchemaMapping> mappings,
      List<SqlQueryMapping> sqlQueryMappings,
      Query query,
      Map<String, DecoderFactory> subDecoderFactories,
      boolean geometryAsWkb) {
    this.mappings = mappings;
    this.query = query;
    this.geometryAsWkb = geometryAsWkb;

    this.mainTablePaths =
        sqlQueryMappings.stream().map(s -> s.getMainTable().getFullPath()).toList();
    List<List<String>> multiTables =
        sqlQueryMappings.stream()
            .flatMap(s -> s.getTables().stream())
            .filter(schema -> !schema.isRoot())
            .map(SqlQuerySchema::getFullPath)
            .distinct()
            .toList();
    this.multiplicityTracker = new SqlMultiplicityTracker(multiTables);
    this.isSingleFeature =
        query instanceof FeatureQuery && ((FeatureQuery) query).returnsSingleFeature();
    this.subDecoderFactories = subDecoderFactories;
    this.subDecoders = new LinkedHashMap<>();
    this.schemaIndexes = new HashMap<>();
  }

  @Override
  protected void init() {
    this.context = createContext().setMappings(mappings).setQuery(query);
    if (geometryAsWkb) {
      this.geometryDecoderWkb = new GeometryDecoderWkb(getDownstream(), context);
    } else {
      this.geometryDecoderWkt = new GeometryDecoderWkt(getDownstream(), context);
    }
    this.nestingTracker =
        new NestingTracker(getDownstream(), context, mainTablePaths, false, false, false);

    // TODO: pass context and downstream
    subDecoderFactories.forEach(
        (connector, factory) -> subDecoders.put(connector, factory.createDecoder()));
  }

  @Override
  protected void cleanup() {
    while (nestingTracker.isNested()) {
      nestingTracker.close();
    }
    schemaIndexes.clear();

    if (isAtLeastOneFeatureWritten) {
      getDownstream().onFeatureEnd(context);
    }

    getDownstream().onEnd(context);
  }

  @Override
  public void onPush(SqlRow sqlRow) {
    if (sqlRow instanceof SqlRowMeta) {
      handleMetaRow((SqlRowMeta) sqlRow);
      return;
    }

    // TODO: should't happen, test with exception in onStart
    if (!started) {
      return;
    }

    // if (sqlRow instanceof SqlRowValues) {
    handleValueRow(sqlRow);
    //    return;
    // }
  }

  private void handleMetaRow(SqlRowMeta sqlRow) {

    context
        .metadata()
        .numberReturned(
            context.metadata().getNumberReturned().orElse(0) + sqlRow.getNumberReturned());
    if (sqlRow.getNumberMatched().isPresent()) {
      context
          .metadata()
          .numberMatched(
              context.metadata().getNumberMatched().orElse(0)
                  + sqlRow.getNumberMatched().getAsLong());
    }
    context.metadata().isSingleFeature(isSingleFeature);

    if (!started) {
      getDownstream().onStart(context);

      this.started = true;
    }
  }

  private void handleValueRow(SqlRow sqlRow) {

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Sql row: {}", sqlRow);
    }

    String featureType = sqlRow.getType().orElse("");
    Object featureId = sqlRow.getIds().get(0);

    if (nestingTracker.isNotMain(sqlRow.getPath())) {
      multiplicityTracker.track(sqlRow.getPath(), sqlRow.getIds());

      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(
            "Multiplicities {} {}",
            sqlRow.getPath(),
            multiplicityTracker.getMultiplicitiesForPath(sqlRow.getPath()));
      }
    } else {
      while (nestingTracker.isNested()) {
        nestingTracker.close();
      }
      schemaIndexes.clear();
    }

    if (!Objects.equals(currentId, featureId) || !Objects.equals(context.type(), featureType)) {
      if (featureStarted) {
        getDownstream().onFeatureEnd(context);
        this.featureStarted = false;
        multiplicityTracker.reset();
        subDecoders.values().forEach(Decoder::reset);
      }

      context.setType(featureType);
      context.pathTracker().track(sqlRow.getPath());
      getDownstream().onFeatureStart(context);
      this.featureStarted = true;
      this.currentId = featureId;
    }

    List<Integer> multiplicitiesForPath =
        multiplicityTracker.getMultiplicitiesForPath(sqlRow.getPath());

    handleNesting(sqlRow, multiplicitiesForPath);

    boolean isFirstRowForPath =
        multiplicitiesForPath.isEmpty()
            || multiplicitiesForPath.get(multiplicitiesForPath.size() - 1) == 1;

    handleColumns(sqlRow, isFirstRowForPath);

    if (!isAtLeastOneFeatureWritten) {
      this.isAtLeastOneFeatureWritten = true;
    }
  }

  // TODO: move general parts to NestingTracker
  private void handleNesting(SqlRow sqlRow, List<Integer> indexes) {
    while (nestingTracker.isNested()
        && (nestingTracker.doesNotStartWithPreviousPath(sqlRow.getPath())
            || (nestingTracker.inObject() && nestingTracker.isSamePath(sqlRow.getPath())
                || (nestingTracker.inArray()
                    && nestingTracker.isSamePath(sqlRow.getPath())
                    && nestingTracker.hasParentIndexChanged(indexes))))) {
      nestingTracker.close();
    }

    if (nestingTracker.inObject()
        && context.inArray()
        && nestingTracker.doesStartWithPreviousPath(sqlRow.getPath())
        && nestingTracker.hasIndexChanged(indexes)) {
      nestingTracker.closeObject();
      nestingTracker.openObject();
    }

    if (nestingTracker.isNotMain(sqlRow.getPath())) {
      context.pathTracker().track(sqlRow.getPath());

      if (nestingTracker.isFirst(indexes)) {
        nestingTracker.openArray();
      }

      context.setIndexes(indexes);

      nestingTracker.openObject();
    }
  }

  private void handleColumns(SqlRow sqlRow, boolean isFirstRowForPath) {
    for (int i = 0; i < sqlRow.getValues().size() && i < sqlRow.getColumnPaths().size(); i++) {
      // TODO: this is a workaround, ideally the paths SchemaMapping would contain the column
      // aliases
      List<String> columnPath =
          sqlRow.isSubDecoderColumn(i)
              ? subDecoderFactories
                  .get(sqlRow.getSubDecoder(i))
                  .resolvePath(sqlRow.getColumnPaths().get(i))
              : sqlRow.getColumnPaths().get(i);

      context.pathTracker().track(columnPath);
      if (!schemaIndexes.containsKey(columnPath)) {
        schemaIndexes.put(columnPath, 0);
      } else if (isFirstRowForPath) {
        schemaIndexes.put(columnPath, schemaIndexes.get(columnPath) + 1);
      }

      if (sqlRow.isSpatialColumn(i)) {
        if (Objects.nonNull(sqlRow.getValues().get(i))) {
          try {
            context.setSchemaIndex(schemaIndexes.get(columnPath));
            if (geometryAsWkb) {
              geometryDecoderWkb.decode((byte[]) sqlRow.getValues().get(i));
            } else {
              geometryDecoderWkt.decode((String) sqlRow.getValues().get(i));
            }
          } catch (IOException e) {
            throw new IllegalStateException("Error parsing WKT or WKB geometry", e);
          }
        }
      } else {
        context.setValueType(Type.STRING);
        context.setValue((String) sqlRow.getValues().get(i));
        context.setSchemaIndex(schemaIndexes.get(columnPath));

        if (sqlRow.isSubDecoderColumn(i) && Objects.nonNull(context.value())) {
          String subDecoder = sqlRow.getSubDecoder(i);
          if (subDecoders.containsKey(subDecoder)) {
            subDecoders
                .get(subDecoder)
                .decode(context.value().getBytes(StandardCharsets.UTF_8), this);
            subDecoders.get(subDecoder).reset(true);
          } else {
            LOGGER.warn("Invalid sub-decoder: {}", subDecoder);
          }
        } else {
          getDownstream().onValue(context);
        }
      }
    }

    context.setSchemaIndex(-1);

    if (nestingTracker.isNotMain(sqlRow.getPath())) {
      context.pathTracker().track(sqlRow.getPath());
    }
  }

  @Override
  public ModifiableContext<FeatureSchema, SchemaMapping> context() {
    return context;
  }

  @Override
  public FeatureEventHandler<
          FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>>
      downstream() {
    return getDownstream();
  }
}
