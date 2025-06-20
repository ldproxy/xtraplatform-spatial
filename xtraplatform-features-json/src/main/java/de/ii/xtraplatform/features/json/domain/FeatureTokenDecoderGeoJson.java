/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.json.domain;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.async.ByteArrayFeeder;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.SchemaMapping;
import de.ii.xtraplatform.features.domain.pipeline.FeatureEventHandlerSimple.ModifiableContext;
import de.ii.xtraplatform.features.domain.pipeline.FeatureTokenBufferSimple;
import de.ii.xtraplatform.features.domain.pipeline.FeatureTokenDecoderSimple;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: how to handle name collisions for id or geometry
public class FeatureTokenDecoderGeoJson
    extends FeatureTokenDecoderSimple<
        byte[], FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureTokenDecoderGeoJson.class);
  private static final JsonFactory JSON_FACTORY = new JsonFactory();

  private final JsonParser parser;
  private final ByteArrayFeeder feeder;
  private final Optional<String> nullValue;

  private boolean started;
  private int depth = -1;
  private int featureDepth = 0;
  private boolean inFeature = false;
  private boolean inProperties = false;
  private int lastNameIsArrayDepth = 0;
  private int startArray = 0;
  private int endArray = 0;
  private List<String> geoPath = new ArrayList<>();

  private ModifiableContext<FeatureSchema, SchemaMapping> context;
  private FeatureTokenBufferSimple<
          FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>>
      downstream;

  public FeatureTokenDecoderGeoJson(Optional<String> nullValue) {
    try {
      this.parser = JSON_FACTORY.createNonBlockingByteArrayParser();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    this.feeder = (ByteArrayFeeder) parser.getNonBlockingInputFeeder();
    this.nullValue = nullValue;
  }

  @Override
  protected void init() {
    this.context = createContext();
    this.downstream = new FeatureTokenBufferSimple<>(getDownstream(), context);
  }

  @Override
  protected void cleanup() {
    feeder.endOfInput();
  }

  @Override
  public void onPush(byte[] bytes) {
    feedInput(bytes);
  }

  // for unit tests
  void parse(String data) throws Exception {
    byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
    feedInput(dataBytes);
    cleanup();
  }

  private void feedInput(byte[] data) {
    try {
      feeder.feedInput(data, 0, data.length);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }

    boolean feedMeMore = false;
    while (!feedMeMore) {
      feedMeMore = advanceParser();
    }
  }

  public boolean advanceParser() {

    boolean feedMeMore = false;

    try {
      JsonToken nextToken = parser.nextToken();
      String currentName = parser.currentName();

      // TODO: null is end-of-input
      if (Objects.isNull(nextToken)) {
        return true; // or completestage???
      }

      switch (nextToken) {
        case NOT_AVAILABLE:
          feedMeMore = true;
          break;

        case FIELD_NAME:
          break;

        case START_OBJECT:
          if (Objects.nonNull(currentName) && !started) {
            switch (currentName) {
              case "properties":
              case "geometry":
                startIfNecessary(false);
                break;
            }
          }
          if (Objects.nonNull(currentName) && started) {
            switch (currentName) {
              case "properties":
                inProperties = true;
                context.pathTracker().track(0);
                break;
              case "geometry":
                context.setInGeometry(true);
                context.pathTracker().track(currentName, 0);
                geoPath = context.path();
                break;
              default:
                if (inProperties) {
                  context.pathTracker().track(currentName);
                }
                break;
            }
            // nested array_object start
          } else if (context.pathTracker().asList().size() > 0 && started) {
            downstream.onObjectStart(context);
            // feature in collection start
          } else if (depth == featureDepth - 2 && inFeature) {
            downstream.onFeatureStart(context);
          }

          // nested object start?
          if (Objects.nonNull(currentName) || lastNameIsArrayDepth == 0) {
            depth += 1;
            if (depth > featureDepth - 1
                && (inProperties)
                && !Objects.equals(currentName, "properties")) {
              downstream.onObjectStart(context);
            }
          }
          break;

        case START_ARRAY:
          // start features array
          if (depth == 0 && Objects.nonNull(currentName)) {
            switch (currentName) {
              case "features":
                startIfNecessary(true);
                break;
            }
            // start prop/coord array
          } else if (Objects.nonNull(currentName) && (inProperties || context.inGeometry())) {
            if (!context.inGeometry()) {
              context.pathTracker().track(currentName);
            }
            lastNameIsArrayDepth += 1;
            depth += 1;

            downstream.onArrayStart(context);
            // start nested geo array
          } else if (context.inGeometry()) {
            if (endArray > 0) {
              for (int i = 0; i < endArray - 1; i++) {
                downstream.onArrayEnd(context);
              }
              endArray = 0;
            }
            depth += 1;
            startArray++;
          }
          break;

        case END_ARRAY:
          // end features array
          if (depth == 0 && Objects.nonNull(currentName)) {
            switch (currentName) {
              case "features":
                inFeature = false;
                break;
            }
            // end prop/coord array
          } else if (Objects.nonNull(currentName) && (inProperties || context.inGeometry())) {
            if (context.inGeometry()) {
              context.pathTracker().track(geoPath);
            }

            if (endArray > 0) {
              for (int i = 0; i < endArray - 1; i++) {
                downstream.onArrayEnd(context);
              }
              endArray = 0;
            }

            downstream.onArrayEnd(context);

            if (context.inGeometry()) {
              checkBufferForDimension();
              context.pathTracker().track(depth - featureDepth);
            }
            depth -= 1;
            if (inProperties) {
              context.pathTracker().track(depth - featureDepth);
            }
            lastNameIsArrayDepth -= 1;
            // end nested geo array
          } else if (context.inGeometry()) {
            checkBufferForDimension();
            endArray++;
            depth -= 1;
            context.pathTracker().track(depth - featureDepth + 1);
          }
          break;

        case END_OBJECT:

          // end nested object
          if (Objects.nonNull(currentName) || lastNameIsArrayDepth == 0) {
            if (depth > featureDepth - 1
                && (inProperties || context.inGeometry())
                && !Objects.equals(currentName, "properties")) {
              if (context.inGeometry()) {
                context.pathTracker().track(geoPath);
              }
              downstream.onObjectEnd(context);
            }

            // end geo
            if (context.inGeometry()) {
              context.pathTracker().track(depth - featureDepth);
            }

            depth -= 1;
          } else if (lastNameIsArrayDepth > 0) {
            downstream.onObjectEnd(context);
          }

          // end all
          if (depth == -1) {
            if (context.metadata().isSingleFeature()) {
              downstream.onFeatureEnd(context);
            }
            downstream.onEnd(context);
            // end feature in collection
          } else if (depth == featureDepth - 2 && inFeature) {
            downstream.onFeatureEnd(context);
          } else if (inFeature) {
            // featureConsumer.onPropertyEnd(pathTracker.asList());
          }

          if (Objects.equals(currentName, "properties")) {
            inProperties = false;
          }
          if (Objects.equals(currentName, "geometry")) {
            context.setInGeometry(false);
            geoPath = List.of();
            downstream.bufferStop(false);
          }
          if (inProperties) {
            context.pathTracker().track(depth - featureDepth);
          }
          break;

        case VALUE_STRING:
        case VALUE_NUMBER_INT:
        case VALUE_NUMBER_FLOAT:
        case VALUE_TRUE:
        case VALUE_FALSE:
        case VALUE_NULL:
          switch (nextToken) {
            case VALUE_STRING:
            case VALUE_NULL:
              context.setValueType(Type.STRING);
              break;
            case VALUE_NUMBER_INT:
              context.setValueType(Type.INTEGER);
              break;
            case VALUE_NUMBER_FLOAT:
              context.setValueType(Type.FLOAT);
              break;
            case VALUE_TRUE:
            case VALUE_FALSE:
              context.setValueType(Type.BOOLEAN);
              break;
          }

          if (nextToken == JsonToken.VALUE_NULL && nullValue.isEmpty()) {
            break;
          }
          if (nextToken == JsonToken.VALUE_NULL
              && nullValue.isPresent()
              && (Objects.equals(currentName, "geometry")
                  || Objects.equals(currentName, "properties"))) {
            if (Objects.equals(currentName, "properties")) {
              context.pathTracker().track(0);
            } else {
              context.pathTracker().track(currentName, 0);
            }
            context.setValue(nullValue.get());
            downstream.onValue(context);
          }

          // feature or collection prop value
          if (depth == 0 && Objects.nonNull(currentName)) {
            switch (currentName) {
              case "numberReturned":
                context.metadata().numberReturned(parser.getLongValue());
                break;
              case "numberMatched":
                context.metadata().numberMatched(parser.getLongValue());
                break;
              case "type":
                if (!parser.getValueAsString().equals("Feature")) {
                  break;
                }
              case "id":
                startIfNecessary(false);
                if (!currentName.equals("id")) {
                  break;
                }

                context.pathTracker().track(currentName, 0);
                context.setValue(parser.getValueAsString());

                downstream.onValue(context);

                context.pathTracker().track(0);
                break;
            }
            // feature id or props or geo value
          } else if (((inProperties || context.inGeometry()))
              || (inFeature && Objects.equals(currentName, "id"))) {

            if (Objects.nonNull(currentName)) {
              if (context.inGeometry() && Objects.equals(currentName, "type")) {
                // wait for dimension, mark insertion point
                downstream.bufferStart();
                context.setGeometryType(
                    GeoJsonGeometryType.forString(parser.getValueAsString())
                        .toSimpleFeatureGeometry());
                downstream.onObjectStart(context);
                downstream.bufferMark();
                break;
              }
              context.pathTracker().track(currentName);
            }

            if (context.inGeometry() && startArray > 0) {
              for (int i = 0; i < startArray - 1; i++) {
                downstream.onArrayStart(context);
              }
              startArray = 0;
            }

            if (nextToken == JsonToken.VALUE_NULL && nullValue.isPresent()) {
              context.setValue(nullValue.get());
            } else {
              context.setValue(parser.getValueAsString());
            }

            if (context.inGeometry()) {
              context.pathTracker().track(geoPath);
            }

            downstream.onValue(context);

            // feature id
            if (Objects.equals(context.pathAsString(), "id")) {
              context.pathTracker().track(0);
            }
            // why reset depth?
            else if (!context.inGeometry()) {
              context.pathTracker().track(depth - featureDepth);
            } else if (context.inGeometry()) {
              context.pathTracker().track(depth - featureDepth + 1);
            }
          }
          break;
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Could not parse GeoJSON: " + e.getMessage());
    }

    return feedMeMore;
  }

  private void checkBufferForDimension() {
    if (downstream.isBuffering()) {
      int dim =
          (int)
              downstream
                  .bufferAsStream()
                  .filter(token -> Objects.equals(token, Type.FLOAT))
                  .count();
      downstream.bufferInsert(dim);
      downstream.bufferStop(true);
    }
  }

  private void startIfNecessary(boolean isCollection) {
    if (!started) {
      started = true;
      inFeature = true;
      context.pathTracker().track(0);

      if (isCollection) {
        featureDepth = 2;
      } else {
        featureDepth = 1;
        context.metadata().isSingleFeature(true);
      }
      downstream.onStart(context);
      if (!isCollection) {
        downstream.onFeatureStart(context);
      }
    }
  }
}
