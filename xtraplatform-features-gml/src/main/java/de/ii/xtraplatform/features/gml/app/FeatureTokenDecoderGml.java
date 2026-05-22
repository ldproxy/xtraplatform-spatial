/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.app;

import com.fasterxml.aalto.AsyncByteArrayFeeder;
import com.fasterxml.aalto.AsyncXMLStreamReader;
import com.fasterxml.aalto.stax.InputFactoryImpl;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext;
import de.ii.xtraplatform.features.domain.FeatureQuery;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.FeatureTokenDecoder;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.SchemaMapping;
import de.ii.xtraplatform.features.gml.domain.GeometryDecoderGml;
import de.ii.xtraplatform.features.gml.domain.XMLNamespaceNormalizer;
import de.ii.xtraplatform.geometries.domain.Geometry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

/**
 * @author zahnen
 */
@SuppressWarnings("PMD.TooManyMethods")
public class FeatureTokenDecoderGml
    extends FeatureTokenDecoder<
        byte[], FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>> {

  private final AsyncXMLStreamReader<AsyncByteArrayFeeder> parser;

  private final XMLNamespaceNormalizer namespaceNormalizer;
  private final FeatureSchema featureSchema;
  private final FeatureQuery featureQuery;
  private final Map<String, SchemaMapping> mappings;
  private final List<QName> featureTypes;
  private final GmlMultiplicityTracker multiplicityTracker;
  private final boolean passThrough;
  private final GeometryDecoderGml geometryDecoder;
  private final ParsingState state;

  private final class ParsingState {
    private boolean buffering;
    private int depth;
    private int featureDepth;
    private boolean inFeature;
    private boolean inGeometry;
    private Optional<EpsgCrs> crs = Optional.empty();
    private OptionalInt srsDimension = OptionalInt.empty();
    private ModifiableContext<FeatureSchema, SchemaMapping> context;
    private final List<String> bufferParts = new ArrayList<>();

    private void resetBuffer() {
      this.bufferParts.clear();
    }
  }

  public FeatureTokenDecoderGml(
      Map<String, String> namespaces,
      List<QName> featureTypes,
      FeatureSchema featureSchema,
      FeatureQuery query,
      Map<String, SchemaMapping> mappings,
      boolean passThrough) {
    super();
    this.namespaceNormalizer = new XMLNamespaceNormalizer(namespaces);
    this.featureSchema = featureSchema;
    this.featureQuery = query;
    this.mappings = mappings;
    this.featureTypes = featureTypes;
    this.multiplicityTracker = new GmlMultiplicityTracker();
    this.passThrough = passThrough;
    this.geometryDecoder = new GeometryDecoderGml();
    this.state = new ParsingState();

    try {
      this.parser = new InputFactoryImpl().createAsyncFor(new byte[0]);
    } catch (XMLStreamException e) {
      throw new IllegalStateException("Could not create GML decoder: " + e.getMessage(), e);
    }
  }

  @Override
  protected void init() {
    this.state.context =
        createContext()
            .setType(featureSchema.getName())
            .setMappings(mappings)
            .setQuery(featureQuery);
  }

  @Override
  protected void cleanup() {
    parser.getInputFeeder().endOfInput();
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
      parser.getInputFeeder().feedInput(data, 0, data.length);
    } catch (XMLStreamException e) {
      throw new IllegalStateException(e);
    }

    boolean feedMeMore = false;
    while (!feedMeMore) {
      feedMeMore = advanceParser();
    }
  }

  // TODO: single feature or collection
  @SuppressWarnings("PMD.CyclomaticComplexity")
  protected boolean advanceParser() {

    boolean feedMeMore = false;
    ModifiableContext<FeatureSchema, SchemaMapping> context = state.context;

    try {
      if (!parser.hasNext()) {
        return true;
      }

      switch (parser.next()) {
        case AsyncXMLStreamReader.EVENT_INCOMPLETE:
          feedMeMore = true;
          break;

        case XMLStreamConstants.START_DOCUMENT:
          break;

        case XMLStreamConstants.END_DOCUMENT:
          // completeStage();
          break;

        case XMLStreamConstants.START_ELEMENT:
          feedMeMore = handleStartElement(context);
          break;

        case XMLStreamConstants.END_ELEMENT:
          handleEndElement(context);
          break;

        case XMLStreamConstants.CHARACTERS:
          handleCharacters();
          break;

          // Do not support DTD, SPACE, NAMESPACE, NOTATION_DECLARATION, ENTITY_DECLARATION,
          // PROCESSING_INSTRUCTION, COMMENT, CDATA
          // ATTRIBUTE is handled in START_ELEMENT implicitly

        default:
          // advanceParser(in);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not parse GML: " + e.getMessage(), e);
    }
    return feedMeMore;
  }

  @SuppressWarnings({
    "PMD.NcssCount",
    "PMD.CognitiveComplexity",
    "PMD.CyclomaticComplexity",
    "PMD.NPathComplexity"
  })
  private boolean handleStartElement(ModifiableContext<FeatureSchema, SchemaMapping> context)
      throws XMLStreamException, IOException {
    if (geometryDecoder.isWaitingForInput()) {
      // continue decoding the geometry
      Optional<Geometry<?>> optGeometry =
          geometryDecoder.continueDecoding(
              parser, state.crs, state.srsDimension, parser.getLocalName(), null);
      if (optGeometry.isPresent()) {
        context.setGeometry(optGeometry.get());
        getDownstream().onGeometry(context);
      }
      // Still waiting for more input or just decoded - either way, done with this element
      return false;
    }

    boolean feedMeMore = false;
    if (state.depth == 0) {
      OptionalLong numberMatched;
      OptionalLong numberReturned;
      try {
        numberReturned =
            OptionalLong.of(Long.parseLong(parser.getAttributeValue(null, "numberReturned")));
      } catch (NumberFormatException e) {
        numberReturned = OptionalLong.empty();
      }
      try {
        numberMatched =
            OptionalLong.of(Long.parseLong(parser.getAttributeValue(null, "numberMatched")));
      } catch (NumberFormatException e) {
        numberMatched = OptionalLong.empty();
      }

      context.metadata().numberReturned(numberReturned);
      context.metadata().numberMatched(numberMatched);

      if (numberReturned.orElse(0) == 1 && numberMatched.orElse(1) == 1) {
        context.metadata().isSingleFeature(true);
      }

      context.additionalInfo().clear();
      for (int i = 0; i < parser.getAttributeCount(); i++) {
        context.putAdditionalInfo(
            namespaceNormalizer.getQualifiedName(
                parser.getAttributeNamespace(i), parser.getAttributeLocalName(i)),
            parser.getAttributeValue(i));
      }

      getDownstream().onStart(context);
    } else if ("Envelope".equals(parser.getLocalName()) && state.depth == 2) {
      String srsName = parser.getAttributeValue(null, "srsName");
      if (srsName != null && !srsName.isEmpty()) {
        try {
          state.crs = Optional.of(EpsgCrs.fromString(srsName));
        } catch (IllegalArgumentException e) {
          state.crs = Optional.empty();
        }
      } else {
        state.crs = Optional.empty();
      }
      try {
        state.srsDimension =
            OptionalInt.of(Integer.parseInt(parser.getAttributeValue(null, "srsDimension")));
      } catch (NumberFormatException e) {
        state.srsDimension = OptionalInt.empty();
      }
    } else if (matchesFeatureType(parser.getNamespaceURI(), parser.getLocalName())
        || matchesFeatureType(parser.getLocalName())) {
      state.inFeature = true;
      state.featureDepth = state.depth;

      context.additionalInfo().clear();
      for (int i = 0; i < parser.getAttributeCount(); i++) {
        context.putAdditionalInfo(
            namespaceNormalizer.getQualifiedName(
                parser.getAttributeNamespace(i), parser.getAttributeLocalName(i)),
            parser.getAttributeValue(i));
      }

      context
          .pathTracker()
          .track(
              namespaceNormalizer.getQualifiedName(
                  parser.getNamespaceURI(), parser.getLocalName()));

      getDownstream().onFeatureStart(context);

      if (context.additionalInfo().containsKey("gml:id") && !passThrough) {
        context.pathTracker().track("gml:@id");
        context.setValue(context.additionalInfo().get("gml:id"));
        context.setValueType(Type.STRING);
        getDownstream().onValue(context);
      }
    } else if (state.inFeature) {
      context
          .pathTracker()
          .track(
              namespaceNormalizer.getQualifiedName(parser.getNamespaceURI(), parser.getLocalName()),
              state.depth - state.featureDepth);
      multiplicityTracker.track(context.pathTracker().asList());

      context.additionalInfo().clear();
      for (int i = 0; i < parser.getAttributeCount(); i++) {
        context.putAdditionalInfo(
            namespaceNormalizer.getQualifiedName(
                parser.getAttributeNamespace(i), parser.getAttributeLocalName(i)),
            parser.getAttributeValue(i));
      }

      context.setIndexes(
          multiplicityTracker.getMultiplicitiesForPath(context.pathTracker().asList()));

      if (context.schema().filter(FeatureSchema::isSpatial).isPresent()) {
        state.inGeometry = true;
        Optional<Geometry<?>> optGeometry =
            geometryDecoder.decode(parser, state.crs, state.srsDimension);
        // Was the geometry decoded completely? If not, the rest will be buffered before
        // decoding is continued...
        if (optGeometry.isPresent()) {
          context.setGeometry(optGeometry.get());
          getDownstream().onGeometry(context);
        } else {
          feedMeMore = true;
        }
      } else if (context.schema().filter(FeatureSchema::isObject).isPresent()) {
        if (context.schema().filter(FeatureSchema::isArray).isPresent() && context.index() == 1) {
          getDownstream().onArrayStart(context);
        }
        getDownstream().onObjectStart(context);
      } else if (passThrough) {
        getDownstream().onObjectStart(context);
      }
    }

    state.depth += 1;

    if (state.inFeature
        && state.depth > state.featureDepth + 1
        && !state.inGeometry
        && context.schema().filter(FeatureSchema::isObject).isPresent()) {
      context
          .additionalInfo()
          .forEach(
              (prop, value) -> {
                context
                    .pathTracker()
                    .track(prop.replace(":", ":@"), state.depth - state.featureDepth);
                multiplicityTracker.track(context.pathTracker().asList());
                context.setIndexes(
                    multiplicityTracker.getMultiplicitiesForPath(context.pathTracker().asList()));
                context.setValue(value);
                context.setValueType(Type.STRING);
                getDownstream().onValue(context);
              });
    }

    return feedMeMore;
  }

  @SuppressWarnings({"PMD.CognitiveComplexity", "PMD.CyclomaticComplexity"})
  private void handleEndElement(ModifiableContext<FeatureSchema, SchemaMapping> context)
      throws XMLStreamException, IOException {
    if (state.buffering) {
      state.buffering = false;
      if (geometryDecoder.isWaitingForInput()) {
        // continue decoding the geometry with the new buffered input
        Optional<Geometry<?>> optGeometry =
            geometryDecoder.continueDecoding(
                parser,
                state.crs,
                state.srsDimension,
                parser.getLocalName(),
                String.join("", state.bufferParts));
        state.resetBuffer();
        if (optGeometry.isPresent()) {
          context.setGeometry(optGeometry.get());
          getDownstream().onGeometry(context);
        }
        return;
      } else if (!state.bufferParts.isEmpty()) {
        context.setValue(String.join("", state.bufferParts));
        getDownstream().onValue(context);
        state.resetBuffer();
      }
    } else if (geometryDecoder.isWaitingForInput()) {
      // continue decoding the geometry
      Optional<Geometry<?>> optGeometry =
          geometryDecoder.continueDecoding(
              parser, state.crs, state.srsDimension, parser.getLocalName(), "");
      if (optGeometry.isPresent()) {
        context.setGeometry(optGeometry.get());
        getDownstream().onGeometry(context);
      }
      return;
    }

    state.depth -= 1;
    if (state.depth == 0) {
      getDownstream().onEnd(context);
    } else if (matchesFeatureType(parser.getLocalName())) {
      state.inFeature = false;
      getDownstream().onFeatureEnd(context);
      multiplicityTracker.reset();
    } else if (state.inFeature) {
      if (context.schema().filter(FeatureSchema::isSpatial).isPresent()) {
        state.inGeometry = false;
      } else if (context.schema().filter(FeatureSchema::isObject).isPresent()) {
        getDownstream().onObjectEnd(context);
      } else if (passThrough) {
        getDownstream().onObjectEnd(context);
      }
    }

    context.pathTracker().track(state.depth - state.featureDepth);
  }

  private void handleCharacters() {
    if (state.inFeature && !parser.isWhiteSpace()) {
      state.buffering = true;
      state.bufferParts.add(parser.getText());
    }
  }

  boolean matchesFeatureType(final String namespace, final String localName) {
    return featureTypes.stream()
        .anyMatch(
            featureType ->
                featureType.getLocalPart().equals(localName)
                    && Objects.nonNull(namespace)
                    && featureType.getNamespaceURI().equals(namespace));
  }

  boolean matchesFeatureType(final String localName) {
    return featureTypes.stream()
        .anyMatch(featureType -> featureType.getLocalPart().equals(localName));
  }
}
