/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.graphql.app;

import static de.ii.xtraplatform.base.domain.util.LambdaWithException.supplierMayThrow;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.async.ByteArrayFeeder;
import de.ii.xtraplatform.features.domain.Decoder;
import de.ii.xtraplatform.features.domain.FeatureEventHandler;
import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext;
import de.ii.xtraplatform.features.domain.FeatureQuery;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.FeatureTokenDecoder;
import de.ii.xtraplatform.features.domain.SchemaMapping;
import de.ii.xtraplatform.features.json.domain.DecoderJsonProperties;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@SuppressWarnings("PMD.AvoidDeeplyNestedIfStmts")
public class FeatureTokenDecoderGraphQlJson2
    extends FeatureTokenDecoder<
        byte[], FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>>
    implements Decoder.Pipeline {

  private static final JsonFactory JSON_FACTORY = new JsonFactory();

  private final JsonParser parser;
  private final ByteArrayFeeder feeder;
  private final FeatureSchema featureSchema;
  private final FeatureQuery featureQuery;
  private final Map<String, SchemaMapping> mappings;
  private final String type;
  private final String wrapper;

  private final ParsingState state;

  private ModifiableContext<FeatureSchema, SchemaMapping> context;
  private DecoderJsonProperties decoderJsonProperties;

  private static final class ParsingState {
    boolean started;
    int featureDepth;
    boolean inFeatures;
    boolean inProperties;
    boolean isCollection;
    boolean inErrors;
  }

  public FeatureTokenDecoderGraphQlJson2(
      FeatureSchema featureSchema,
      FeatureQuery query,
      Map<String, SchemaMapping> mappings,
      String type,
      String wrapper) {
    this(featureSchema, query, mappings, type, wrapper, Optional.empty());
  }

  public FeatureTokenDecoderGraphQlJson2(
      FeatureSchema featureSchema,
      FeatureQuery query,
      Map<String, SchemaMapping> mappings,
      String type,
      String wrapper,
      Optional<String> nullValue) {
    super();
    try {
      this.parser = JSON_FACTORY.createNonBlockingByteArrayParser();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    this.feeder = (ByteArrayFeeder) parser.getNonBlockingInputFeeder();
    this.featureSchema = featureSchema;
    this.featureQuery = query;
    this.mappings = mappings;
    this.type = type;
    this.wrapper = wrapper;
    this.state = new ParsingState();
    Objects.requireNonNull(nullValue);
  }

  @Override
  protected void init() {
    this.context =
        createContext()
            .setType(featureSchema.getName())
            .setMappings(mappings)
            .setQuery(featureQuery);

    List<List<String>> arrayPaths =
        context.mapping().getSchemasByTargetPath().entrySet().stream()
            .filter(entry -> entry.getValue().get(0).isArray())
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

    this.decoderJsonProperties =
        new DecoderJsonProperties(
            this,
            arrayPaths,
            supplierMayThrow(parser::getValueAsString),
            Optional.empty(),
            Optional.of(new GeometryDecoderWkt()));
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
  void parse(String data) {
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

  @SuppressWarnings({
    "PMD.CognitiveComplexity",
    "PMD.CyclomaticComplexity",
    "PMD.AvoidCatchingThrowable"
  })
  public boolean advanceParser() {

    boolean feedMeMore = false;

    try {
      JsonToken nextToken = parser.nextToken();
      String currentName = parser.currentName();

      switch (nextToken) {
        case NOT_AVAILABLE:
          feedMeMore = true;
          break;

        case FIELD_NAME:
          break;

        case START_OBJECT:
        case START_ARRAY:
          if (!state.inProperties) {
            if (state.inFeatures && context.path().size() == state.featureDepth) {
              state.inProperties = true;
              if (state.isCollection) {
                startFeature();
              }
            } else if (!state.inFeatures && Objects.equals(currentName, wrapper)) {
              startIfNecessary(nextToken == JsonToken.START_ARRAY);
            } else if (!state.inFeatures && Objects.equals(currentName, "errors")) {
              state.inErrors = true;
            }
            break;
          }
          feedMeMore = decoderJsonProperties.parse(nextToken, currentName, state.featureDepth);
          break;
        case END_OBJECT:
          if (state.inProperties && context.path().size() == state.featureDepth) {
            state.inProperties = false;
            getDownstream().onFeatureEnd(context);
            if (!state.isCollection) {
              getDownstream().onEnd(context);
              state.inFeatures = false;
            }
            break;
          }
          feedMeMore = decoderJsonProperties.parse(nextToken, currentName, state.featureDepth);
          break;
        case END_ARRAY:
          if (!state.inProperties && context.path().size() == state.featureDepth) {
            getDownstream().onEnd(context);
            state.inFeatures = false;
            break;
          }
          feedMeMore = decoderJsonProperties.parse(nextToken, currentName, state.featureDepth);
          break;
        default:
          if (!state.inErrors) {
            feedMeMore = decoderJsonProperties.parse(nextToken, currentName, state.featureDepth);
            break;
          }
          if (Objects.equals(currentName, "message")) {
            throw new IllegalStateException("GraphQL error: " + parser.getValueAsString());
          }
          break;
      }

    } catch (Throwable e) {
      throw new IllegalStateException("Could not parse JSON", e);
    }

    return feedMeMore;
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

  private void startIfNecessary(boolean isCollection) {
    if (!state.started) {
      state.started = true;
      state.inFeatures = true;
      state.isCollection = isCollection;

      if (!isCollection) {
        context.metadata().isSingleFeature(true);
      }
      getDownstream().onStart(context);
      if (!isCollection) {
        startFeature();
      }
    }
  }

  private void startFeature() {
    context.pathTracker().track(type, 0);
    context.setIndexes(List.of());
    decoderJsonProperties.reset();
    getDownstream().onFeatureStart(context);
    state.featureDepth = context.path().size();
    state.inProperties = true;
  }
}
