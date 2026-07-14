/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.domain;

import com.fasterxml.aalto.AsyncByteArrayFeeder;
import com.fasterxml.aalto.AsyncXMLStreamReader;
import com.fasterxml.aalto.stax.InputFactoryImpl;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.FeatureQuery;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.SchemaConstraints;
import de.ii.xtraplatform.features.domain.SchemaMapping;
import de.ii.xtraplatform.features.domain.pipeline.FeatureEventHandlerSimple.ModifiableContext;
import de.ii.xtraplatform.features.domain.pipeline.FeatureTokenBufferSimple;
import de.ii.xtraplatform.features.domain.pipeline.FeatureTokenDecoderSimple;
import de.ii.xtraplatform.geometries.domain.Geometry;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes GML input into the schema-resolved feature-token stream. Walks the schema in parallel
 * with the XML and resolves each child element to a schema property by name (or by {@code alias}
 * when {@code FeatureTokenDecoderGmlInputProfile#getUseAlias()} is on), then emits values at the
 * matched property's dotted property-name path (e.g. {@code qag.dpl.prs.des}). The feature root
 * itself contributes no path segment, so direct children sit at depth 0. The emitted form is
 * independent of any particular downstream — any consumer keying off the schema's property-name
 * paths will accept the stream (the SQL feature encoder's writable/value column maps are the
 * canonical example). {@code srsName} URN forms on geometries are reverse-mapped via {@link
 * FeatureTokenDecoderGmlInputProfile#getSrsNameMappings()}, and {@code gml:id} is routed (with
 * {@link FeatureTokenDecoderGmlInputProfile#getGmlIdPrefix()} prefix stripping) to the schema
 * property whose role is {@link SchemaBase.Role#ID}. Multi-feature wrappers ({@code
 * wfs:FeatureCollection}, {@code adv:AX_Bestandsdatenauszug}, …) at the root are rejected unless
 * the input profile names the wrapper explicitly via {@link
 * FeatureTokenDecoderGmlInputProfile#getFeatureCollectionElementName()} and/or {@link
 * FeatureTokenDecoderGmlInputProfile#getFeatureMemberElementName()}; in that case the decoder
 * descends through the configured wrappers and processes a single contained feature. A second
 * feature sibling inside the wrapper is rejected as multi-feature ingest.
 *
 * <p>When the feature schema, or a nested OBJECT property's schema, declares an {@code objectType}
 * that has a {@link FeatureTokenDecoderGmlInputProfile#getVariableObjectElementNames()
 * variableObjectElementNames} entry, the wire element name of that GML object may vary across
 * subtype instances. At the feature root the decoder accepts any wire element whose qualified name
 * appears in that entry's mapping; at a nested OBJECT_PROPERTY's inner object element the wire
 * element name is not otherwise validated, so the mapping is only consulted to decide whether to
 * emit a discriminator value. In both cases the mapped source value is emitted at the configured
 * discriminator property's source path, at the level of the OBJECT it discriminates.
 *
 * <p>Namespace handling mirrors the encoder's qualification chain: when the input profile carries
 * {@link FeatureTokenDecoderGmlInputProfile#getDefaultNamespace()} or {@link
 * FeatureTokenDecoderGmlInputProfile#getObjectTypeNamespaces()}, child elements are required to
 * live in the expected namespace (explicit {@code prefix:} in the schema name/alias →
 * objectTypeNamespaces[parent.objectType] → defaultNamespace). Mismatching elements are skipped
 * rather than mis-matched against a same-localName property in another namespace. With no namespace
 * configuration, matching is by local name alone, preserving the simpler test fixtures.
 *
 * <p>GML's object/property alternation only applies inside the GML namespace and the feature-type's
 * application namespace. ISO 19115 ({@code gmd}/{@code gco}) instead allows object elements to
 * directly carry text content (e.g. {@code <gco:CharacterString>}, {@code <gmd:CI_RoleCode>}), and
 * these routinely appear as children of a scalar property element. The decoder hardcodes {@link
 * #GMD_NS} and {@link #GCO_NS} as value-carrying namespaces: any element in those namespaces inside
 * a {@code VALUE_PROPERTY} is treated as a {@code VALUE_WRAPPER} around the property's scalar text,
 * no explicit {@code valueWrap} entry required. Wrappers in other namespaces (e.g. {@code
 * <adv:AX_LI_ProcessStep_Punktort_Description>}) still need explicit {@code valueWrap}
 * configuration on the input profile. A known restriction of the GML building block.
 */
public class FeatureTokenDecoderGml
    extends FeatureTokenDecoderSimple<
        byte[], FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureTokenDecoderGml.class);

  private static final String XSI_NS = "http://www.w3.org/2001/XMLSchema-instance";
  private static final String XLINK_NS = "http://www.w3.org/1999/xlink";

  /**
   * ISO 19115 metadata namespace. GML's object/property alternation rule only applies inside the
   * GML namespace and the feature-type's application namespace; ISO 19115 ({@code gmd}/{@code gco})
   * instead uses object elements that directly carry text content (e.g. {@code
   * <gco:CharacterString>}, {@code <gmd:CI_RoleCode>}). When such an element appears as a child of
   * a scalar property element the decoder treats it as a value wrapper around the scalar text, even
   * without an explicit {@code valueWrap} entry. Any other external namespace that follows the same
   * convention needs a similar entry here when the need arises — a known restriction of the GML
   * building block.
   */
  private static final String GMD_NS = "http://www.isotc211.org/2005/gmd";

  /** ISO 19115 common types namespace; see {@link #GMD_NS}. */
  private static final String GCO_NS = "http://www.isotc211.org/2005/gco";

  private final AsyncXMLStreamReader<AsyncByteArrayFeeder> parser;
  private final XMLNamespaceNormalizer namespaceNormalizer;
  private final FeatureSchema featureSchema;
  private final FeatureQuery featureQuery;
  private final Map<String, SchemaMapping> mappings;
  private final List<QName> featureTypes;
  private final Optional<EpsgCrs> defaultCrs;
  private final Optional<String> nullValue;
  private final FeatureTokenDecoderGmlInputProfile inputProfile;
  private final GeometryDecoderGml geometryDecoder;
  private final StringBuilder buffer;

  /**
   * Qualified names of the configured wrapper elements ({@code featureCollectionElementName} then
   * {@code featureMemberElementName}), ordered outermost-first. Empty when no wrappers are
   * configured: the document root is directly the feature element. Indexed by {@link #depth} during
   * wrapper descent.
   */
  private final List<String> wrapperElementNames;

  /**
   * Depth at which the feature element is expected (i.e. {@code wrapperElementNames.size()}). The
   * first START at this depth is the feature root; the corresponding END pops it back. With no
   * wrappers this is {@code 0}, preserving the original behaviour.
   */
  private final int featureRootDepth;

  /**
   * Maps each property's {@link FeatureSchema#getFullPathAsString()} to the equivalent path with
   * each segment replaced by the property's {@code alias} (falling back to the segment name when no
   * alias is set). The encoder applies {@code alias} as a {@code rename} transformation before
   * consulting {@link FeatureTokenDecoderGmlInputProfile#getValueWrap()}, so its lookup key is the
   * alias-form path. The decoder, which sees the untransformed schema, uses this map to consult
   * {@code valueWrap} under the same key the encoder writes — keeping a single YAML convention
   * (alias-form keys) working symmetrically for read and write. Empty (no aliases declared) when
   * {@code useAlias} is off or no property carries an alias.
   */
  private final Map<String, String> aliasFormPathByPropertyPath;

  private int depth = 0;
  private boolean inFeature = false;
  private boolean featureProcessed = false;
  private boolean isBuffering = false;
  private String currentArrayPath;
  private Optional<EpsgCrs> crs = Optional.empty();
  private Optional<EpsgCrs> featureGeometryCrs = Optional.empty();
  private OptionalInt srsDimension = OptionalInt.empty();
  private ModifiableContext<FeatureSchema, SchemaMapping> context;
  private final ArrayDeque<Frame> frames = new ArrayDeque<>();

  private FeatureTokenBufferSimple<
          FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>>
      downstream;

  /**
   * Per-element state carried while descending into a feature. Pushed on START, popped on END. The
   * feature root itself is not stacked — when {@code frames} is empty we are directly inside the
   * feature element and lookup resolves against {@link #featureSchema}.
   *
   * <p>GML's object/property alternation rule shapes the frame stack: an <em>object element</em>
   * (feature or nested object) has only property elements as direct children; a <em>property
   * element</em> contains either scalar text or exactly one nested object element. The kinds below
   * reflect those two roles.
   */
  private enum FrameKind {
    /** Scalar property element: characters between START/END become the emitted value. */
    VALUE_PROPERTY,
    /** Geometry property element at the feature root. The geometry decoder consumes the subtree. */
    GEOMETRY_PROPERTY,
    /**
     * Object-valued property element — per the alternation rule, its only legal child is a single
     * {@link #OBJECT_ELEMENT} (e.g. {@code <adv:AX_Gemarkung_Schluessel>} inside an {@code
     * <adv:gemarkung>} property).
     */
    OBJECT_PROPERTY,
    /**
     * GML object element nested inside an {@link #OBJECT_PROPERTY}. Contributes no path segment;
     * its child elements are property elements resolved against the parent OBJECT_PROPERTY's
     * schema.
     */
    OBJECT_ELEMENT,
    /**
     * Wrapper element interposed by the encoder's {@code valueWrap} option between a {@link
     * #VALUE_PROPERTY} and its scalar text (e.g. {@code <prop><Wrap1><Wrap2>v</Wrap2></Wrap1>
     * </prop>}). Carries no schema meaning; its purpose is to keep the character buffer alive
     * across the wrappers' end-elements so the enclosing VALUE_PROPERTY can read the inner text on
     * its own end. Only pushed when the enclosing VALUE_PROPERTY's {@code fullPathAsString} is
     * listed in {@link FeatureTokenDecoderGmlInputProfile#getValueWrap()}.
     */
    VALUE_WRAPPER,
    /** Element with no matching schema property; descendants are ignored. */
    UNKNOWN
  }

  private static final class Frame {
    final FrameKind kind;
    final FeatureSchema prop;

    /**
     * For OBJECT_PROPERTY / OBJECT_ELEMENT: schema whose properties are matched against child START
     * elements.
     */
    final FeatureSchema lookupOwner;

    /**
     * Resolved source-path segment contributed by this frame to the path tracker, or {@code null}
     * when no segment is contributed — this is the case for a <em>transparent</em> OBJECT_PROPERTY
     * (no {@code sourcePath}, used to flatten nested objects whose leaves carry columns of the
     * parent table), and for OBJECT_ELEMENT / UNKNOWN frames.
     */
    final String segment;

    /**
     * Path tracker depth for this frame. For non-transparent frames this is the index at which
     * {@link #segment} lives; for transparent OBJECT_PROPERTY / OBJECT_ELEMENT frames this equals
     * the parent's pathDepth so that descendants are tracked at the right depth. {@code -1} when
     * the frame contributes nothing (UNKNOWN).
     */
    final int pathDepth;

    /**
     * For OBJECT_PROPERTY: set to {@code true} once the alternation rule's single permitted child
     * OBJECT_ELEMENT has been seen. A second child START on the same OBJECT_PROPERTY violates the
     * rule and is reported as malformed.
     */
    boolean objectElementSeen;

    boolean nilOnCurrent;
    String pendingXlinkHrefValue;

    /**
     * Temporary fallback for a STRING-typed VALUE / VALUE_ARRAY property whose element carries an
     * {@code xlink:href} attribute but no text content. Used only when the property does not route
     * hrefs via {@link #pendingXlinkHrefValue} (i.e. it is not a feature-ref or codelist property),
     * and the element body produced no buffered text. Supports the workaround where a concat'd
     * {@code FEATURE_REF_ARRAY} is modelled as a plain STRING {@code VALUE_ARRAY}, with the target
     * id still arriving on the wire as {@code xlink:href}.
     */
    String pendingXlinkHrefFallback;

    /**
     * For VALUE_PROPERTY: set to {@code true} when the property's {@code fullPathAsString} is
     * listed in {@link FeatureTokenDecoderGmlInputProfile#getValueWrap()}. Children that appear
     * inside such a frame are pushed as {@link FrameKind#VALUE_WRAPPER}s so that wrapper
     * end-elements do not flush the character buffer before the scalar text is emitted on the
     * VALUE_PROPERTY's own end.
     */
    boolean valueWrapped;

    /**
     * For OBJECT_ELEMENT: the {@link FeatureSchema#getName() name} of the array property currently
     * open as a direct child of this object element, or {@code null} when no array is open at this
     * level. The feature-root level uses the enclosing decoder's {@code currentArrayPath} field for
     * the same purpose. Bracketing is per nesting level: a child OBJECT_ELEMENT does not inherit or
     * affect its parent's open array.
     */
    String openArrayChildPath;

    private Frame(
        FrameKind kind,
        FeatureSchema prop,
        FeatureSchema lookupOwner,
        String segment,
        int pathDepth) {
      this.kind = kind;
      this.prop = prop;
      this.lookupOwner = lookupOwner;
      this.segment = segment;
      this.pathDepth = pathDepth;
    }

    static Frame valueProperty(FeatureSchema prop, String segment, int pathDepth) {
      return new Frame(FrameKind.VALUE_PROPERTY, prop, null, segment, pathDepth);
    }

    static Frame geometryProperty(FeatureSchema prop, String segment, int pathDepth) {
      return new Frame(FrameKind.GEOMETRY_PROPERTY, prop, null, segment, pathDepth);
    }

    static Frame objectProperty(FeatureSchema prop, String segment, int pathDepth) {
      return new Frame(FrameKind.OBJECT_PROPERTY, prop, prop, segment, pathDepth);
    }

    static Frame objectElement(FeatureSchema lookupOwner, int pathDepth) {
      return new Frame(FrameKind.OBJECT_ELEMENT, null, lookupOwner, null, pathDepth);
    }

    static Frame valueWrapper() {
      return new Frame(FrameKind.VALUE_WRAPPER, null, null, null, -1);
    }

    static Frame unknown() {
      return new Frame(FrameKind.UNKNOWN, null, null, null, -1);
    }
  }

  public FeatureTokenDecoderGml(
      Map<String, String> namespaces,
      List<QName> featureTypes,
      FeatureSchema featureSchema,
      FeatureQuery query,
      Map<String, SchemaMapping> mappings,
      EpsgCrs storageCrs,
      Optional<EpsgCrs> headerCrs,
      Optional<String> nullValue,
      FeatureTokenDecoderGmlInputProfile inputProfile) {
    this.namespaceNormalizer = new XMLNamespaceNormalizer(namespaces);
    this.featureSchema = featureSchema;
    this.featureQuery = query;
    this.mappings = mappings;
    this.featureTypes = featureTypes;
    this.defaultCrs = headerCrs.isPresent() ? headerCrs : Optional.of(storageCrs);
    this.nullValue = nullValue;
    this.inputProfile = inputProfile;
    this.geometryDecoder = new GeometryDecoderGml(inputProfile.getSrsNameMappings());
    this.buffer = new StringBuilder();

    List<String> wrappers = new ArrayList<>(2);
    if (!inputProfile.getFeatureCollectionElementName().isEmpty()) {
      wrappers.add(inputProfile.getFeatureCollectionElementName());
    }
    if (!inputProfile.getFeatureMemberElementName().isEmpty()) {
      wrappers.add(inputProfile.getFeatureMemberElementName());
    }
    this.wrapperElementNames = List.copyOf(wrappers);
    this.featureRootDepth = this.wrapperElementNames.size();

    if (inputProfile.getUseAlias()) {
      Map<String, String> aliasPaths = new HashMap<>();
      collectAliasFormPaths(featureSchema, "", "", aliasPaths);
      this.aliasFormPathByPropertyPath = Map.copyOf(aliasPaths);
    } else {
      this.aliasFormPathByPropertyPath = Map.of();
    }

    try {
      this.parser = new InputFactoryImpl().createAsyncFor(new byte[0]);
    } catch (XMLStreamException e) {
      throw new IllegalStateException("Could not create GML decoder: " + e.getMessage());
    }
  }

  /**
   * Mirrors the encoder side, which queries {@link
   * FeatureTokenDecoderGmlInputProfile#getValueWrap()} after {@code alias → rename} injection —
   * i.e. by the alias-form path. We check both the untransformed property path and the alias-form
   * path so a YAML config keyed by either form (alias path when {@code useAlias: true}, or the bare
   * property path) is recognised.
   */
  private boolean isValueWrapped(FeatureSchema prop) {
    Map<String, List<String>> valueWrap = inputProfile.getValueWrap();
    if (valueWrap.isEmpty()) {
      return false;
    }
    String path = prop.getFullPathAsString();
    if (valueWrap.containsKey(path)) {
      return true;
    }
    String aliasPath = aliasFormPathByPropertyPath.get(path);
    return aliasPath != null && valueWrap.containsKey(aliasPath);
  }

  private static void collectAliasFormPaths(
      FeatureSchema schema, String pathPrefix, String aliasPrefix, Map<String, String> out) {
    for (FeatureSchema child : schema.getProperties()) {
      String name = child.getName();
      String alias = child.getAlias().orElse(name);
      String path = pathPrefix.isEmpty() ? name : pathPrefix + "." + name;
      String aliasPath = aliasPrefix.isEmpty() ? alias : aliasPrefix + "." + alias;
      if (!path.equals(aliasPath)) {
        out.put(path, aliasPath);
      }
      if (!child.getProperties().isEmpty()) {
        collectAliasFormPaths(child, path, aliasPath, out);
      }
    }
  }

  @Override
  protected void init() {
    this.context = createContext();
    this.downstream = new FeatureTokenBufferSimple<>(getDownstream(), context);
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
  void parse(String data) throws Exception {
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

  private boolean advanceParser() {
    boolean feedMeMore = false;
    try {
      if (!parser.hasNext()) {
        return true;
      }

      switch (parser.next()) {
        case AsyncXMLStreamReader.EVENT_INCOMPLETE:
          feedMeMore = true;
          break;

        case XMLStreamConstants.START_DOCUMENT:
        case XMLStreamConstants.END_DOCUMENT:
          break;

        case XMLStreamConstants.START_ELEMENT:
          feedMeMore = onStartElement();
          break;

        case XMLStreamConstants.END_ELEMENT:
          onEndElement();
          break;

        case XMLStreamConstants.CHARACTERS:
          if (inFeature && !parser.isWhiteSpace()) {
            isBuffering = true;
            buffer.append(parser.getText());
          }
          break;

        default:
          // ignore: DTD, SPACE, NAMESPACE, NOTATION_DECLARATION, ENTITY_DECLARATION,
          // PROCESSING_INSTRUCTION, COMMENT, CDATA. ATTRIBUTE is implicit in START_ELEMENT.
      }
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not parse GML: " + e.getMessage(), e);
    }
    return feedMeMore;
  }

  private boolean onStartElement() throws XMLStreamException, java.io.IOException {
    if (geometryDecoder.isWaitingForInput()) {
      Optional<Geometry<?>> optGeometry =
          geometryDecoder.continueDecoding(parser, crs, srsDimension, parser.getLocalName(), null);
      if (optGeometry.isPresent()) {
        emitGeometry(optGeometry.get());
      }
      return false;
    }

    if (!isValueWrapChainElement()) {
      rejectXsiType();
    }

    if (!inFeature && depth < featureRootDepth) {
      String expected = wrapperElementNames.get(depth);
      if (!matchesQualifiedElement(parser.getNamespaceURI(), parser.getLocalName(), expected)) {
        throw new IllegalArgumentException(
            "Multi-feature ingest is not supported by this endpoint; "
                + "expected wrapper element <"
                + expected
                + "> at document depth "
                + depth
                + " but found <"
                + parser.getLocalName()
                + ">.");
      }
      depth++;
      return false;
    }

    if (!inFeature && depth == featureRootDepth) {
      if (featureProcessed) {
        throw new IllegalArgumentException(
            "Multi-feature ingest is not supported by this endpoint; "
                + "found a second feature element <"
                + parser.getLocalName()
                + "> inside the configured collection wrapper.");
      }
      if (!matchesFeatureType(parser.getNamespaceURI(), parser.getLocalName())
          && !matchesFeatureType(parser.getLocalName())
          && resolveVariableNameDiscriminator(
                  featureSchema, parser.getNamespaceURI(), parser.getLocalName())
              .isEmpty()) {
        throw new IllegalArgumentException(
            "Multi-feature ingest is not supported by this endpoint; "
                + "expected a single feature element at the document root but found <"
                + parser.getLocalName()
                + ">.");
      }
      onFeatureStart();
      featureProcessed = true;
      depth++;
      return false;
    }

    Frame parent = frames.peek();

    // GML alternation: an OBJECT_PROPERTY normally contains exactly one OBJECT_ELEMENT, which
    // carries no path segment of its own; the OBJECT_ELEMENT's child properties resolve against
    // the OBJECT_PROPERTY's schema. *GML array properties* — one property element wrapping several
    // peer object elements (max=unbounded on the inner element in the application schema) — are
    // also valid and supported here when the schema declares the property as an OBJECT_ARRAY: each
    // peer OBJECT_ELEMENT then emits its own OBJECT / OBJECT_END pair, anchored at the
    // OBJECT_PROPERTY's path. A second peer on a non-array OBJECT_PROPERTY remains a schema/wire
    // mismatch and is reported. The FEATURE_REF-as-OBJECT path (wrap=OBJECT / OBJECT_ARRAY) does
    // not reach this branch because it has no inner OBJECT_ELEMENT — the property element is
    // self-closing with xlink:href.
    if (parent != null && parent.kind == FrameKind.OBJECT_PROPERTY) {
      boolean arrayProperty = parent.prop.isArray() && !parent.prop.isFeatureRef();
      if (parent.objectElementSeen && !arrayProperty) {
        throw new IllegalArgumentException(
            "Unsupported GML shape: object property <"
                + parent.prop.getName()
                + "> is declared as a single-valued OBJECT but contains more than one object"
                + " element; found extra <"
                + parser.getLocalName()
                + ">.");
      }
      if (arrayProperty) {
        // For array OBJECT_PROPERTYs the per-peer OBJECT pair is emitted here (the prop START did
        // not). Re-track the prop's segment first — previous peers' descendants will have moved
        // the path tracker deeper.
        context.pathTracker().track(parent.segment, parent.pathDepth);
        downstream.onObjectStart(context);
      }
      parent.objectElementSeen = true;
      // The inner object element of an OBJECT_PROPERTY carries no path segment itself, but its
      // wire name may match a variableObjectElementNames entry on the OBJECT_PROPERTY's
      // objectType — emit the discriminator value at the nested OBJECT's level before descending.
      emitVariableNameDiscriminator(
          parent.prop, parent.pathDepth + 1, parser.getNamespaceURI(), parser.getLocalName());
      frames.push(Frame.objectElement(parent.prop, parent.pathDepth));
      depth++;
      return false;
    }

    FeatureSchema lookupOwner;
    int parentPathDepth;
    if (parent == null) {
      lookupOwner = featureSchema;
      // No path-tracker segment for the feature root: emitted paths begin at the property
      // name of the first child (depth 0). Property-name paths carry no source-system prefix,
      // so downstreams (e.g. the SQL feature encoder's writable/value column maps) line up
      // directly.
      parentPathDepth = -1;
    } else if (parent.kind == FrameKind.OBJECT_ELEMENT) {
      lookupOwner = parent.lookupOwner;
      parentPathDepth = parent.pathDepth;
    } else {
      // Inside a VALUE_PROPERTY / VALUE_WRAPPER / GEOMETRY_PROPERTY / UNKNOWN frame — descendants
      // carry no schema meaning. (Per GML's alternation rule a scalar property has only text
      // content; if we see an element here it is either unsupported mixed content or an
      // already-skipped subtree.) Exceptions: (a) the encoder's valueWrap option produces a
      // wrapper-element chain around the scalar text of a VALUE_PROPERTY — push VALUE_WRAPPER so
      // the character buffer survives the wrappers' end-elements; (b) gmd/gco object elements
      // (ISO 19115) carry text directly and routinely appear inside a property element — treat
      // them as value wrappers without requiring an explicit valueWrap entry. Once inside a
      // VALUE_WRAPPER, the chain continues regardless of the inner element's namespace.
      frames.push(isValueWrapChainElement() ? Frame.valueWrapper() : Frame.unknown());
      depth++;
      return false;
    }

    String localName = parser.getLocalName();
    String namespaceUri = parser.getNamespaceURI();
    Optional<FeatureSchema> propOpt = lookupChild(lookupOwner, localName, namespaceUri);

    // Array bracketing fires at any container level: the feature root (parent == null) and every
    // OBJECT_ELEMENT (where the inner object element acts as the container for its child
    // properties). The root level uses {@code currentArrayPath}; each OBJECT_ELEMENT frame
    // carries its own {@code openArrayChildPath}. The open array at this level closes when the
    // next sibling property has a different name.
    boolean isArrayContainer = parent == null || parent.kind == FrameKind.OBJECT_ELEMENT;
    if (isArrayContainer) {
      String containerArrayPath = parent == null ? currentArrayPath : parent.openArrayChildPath;
      String childPathSegment = propOpt.map(FeatureSchema::getName).orElse(null);
      if (containerArrayPath != null && !containerArrayPath.equals(childPathSegment)) {
        downstream.onArrayEnd(context);
        if (parent == null) {
          currentArrayPath = null;
        } else {
          parent.openArrayChildPath = null;
        }
      }
    }

    if (propOpt.isEmpty()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Skipping <{}>: no schema property matches.", localName);
      }
      frames.push(Frame.unknown());
      depth++;
      return false;
    }

    FeatureSchema prop = propOpt.get();

    // Every property contributes its name as a path segment, producing a dotted property-name
    // path (e.g. {@code qag.dpl.prs.des} for a deeply nested scalar). The path is independent
    // of any {@code sourcePath} the property declares, so it lines up with whatever downstream
    // keys off property-name paths.
    String segment = prop.getName();
    int segmentPathDepth = parentPathDepth + 1;
    context.pathTracker().track(segment, segmentPathDepth);

    if (isArrayContainer && prop.isArray()) {
      String containerArrayPath = parent == null ? currentArrayPath : parent.openArrayChildPath;
      if (containerArrayPath == null) {
        downstream.onArrayStart(context);
        if (parent == null) {
          currentArrayPath = segment;
        } else {
          parent.openArrayChildPath = segment;
        }
      }
    }

    if (prop.isSpatial()) {
      // Geometry properties decode the full GML geometry subtree via {@link GeometryDecoderGml}
      // regardless of nesting depth; the path tracker is already set to the property's segment
      // path so {@code emitGeometry} routes the resulting Geometry to the right downstream slot.
      Optional<Geometry<?>> optGeometry = geometryDecoder.decode(parser, crs, srsDimension);
      frames.push(Frame.geometryProperty(prop, segment, segmentPathDepth));
      if (optGeometry.isPresent()) {
        emitGeometry(optGeometry.get());
        depth++;
        return false;
      }
      // geometry needs more input; the next push will continue via continueDecoding
      depth++;
      return true;
    } else if (prop.isValue()) {
      Frame frame = Frame.valueProperty(prop, segment, segmentPathDepth);
      frame.nilOnCurrent = readXsiNil();
      frame.pendingXlinkHrefValue = readXlinkHrefAsValue(prop);
      if (frame.pendingXlinkHrefValue == null
          && prop.getValueType().orElse(prop.getType()) == Type.STRING) {
        String raw = readRawXlinkHref();
        frame.pendingXlinkHrefFallback =
            raw == null
                ? null
                : applyReverseTemplate(inputProfile.getFeatureRefTemplate(), raw).orElse(raw);
      }
      frame.valueWrapped = isValueWrapped(prop);
      validateUom(prop);
      frames.push(frame);
    } else if (prop.isObject()) {
      // OBJECT pair anchoring: for non-array OBJECT_PROPERTYs and for FEATURE_REF-as-OBJECT
      // (wrap=OBJECT / OBJECT_ARRAY where the wire is a self-closing prop element with
      // xlink:href), the OBJECT pair is anchored on the property element itself — emit
      // OBJECT_START here and OBJECT_END at the matching property END. For array non-FEATURE_REF
      // OBJECT_PROPERTYs the pair is emitted per peer OBJECT_ELEMENT instead, which lets both
      // shape (a) (many sibling property elements each with one inner object) and shape (b)
      // (one property element wrapping many peer object elements) feed the downstream with the
      // same per-member OBJECT bracketing. The enclosing ARRAY at the parent's level still
      // brackets the whole sequence.
      boolean arrayOfObjects = prop.isArray() && !prop.isFeatureRef();
      if (!arrayOfObjects) {
        downstream.onObjectStart(context);
      }
      // A FEATURE_REF property whose schema has been expanded to an OBJECT (id/title/type
      // children) by an upstream wrap=OBJECT transformation carries its identifier on the
      // property element's xlink:href, not as a nested object element. Reduce the href via
      // featureRefTemplate and emit it at the conventional .id child path so the writable
      // column wired to that child receives the value. The wire payload contains no inner
      // element, so the OBJECT_PROPERTY frame's END just emits onObjectEnd. Pre-expansion
      // FEATURE_REFs (type==FEATURE_REF) take the isValue() branch above and emit the
      // reduced value directly at the property's own path.
      String hrefValue = prop.isFeatureRef() ? readXlinkHrefAsValue(prop) : null;
      if (hrefValue != null) {
        context.pathTracker().track("id", segmentPathDepth + 1);
        context.setValue(hrefValue);
        context.setValueType(Type.STRING);
        downstream.onValue(context);
      }
      frames.push(Frame.objectProperty(prop, segment, segmentPathDepth));
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Skipping child <{}>: not handled by this decoder.", localName);
      }
      frames.push(Frame.unknown());
    }

    depth++;
    return false;
  }

  private void onEndElement() throws XMLStreamException, java.io.IOException {
    if (geometryDecoder.isWaitingForInput()) {
      // The geometry decoder paused on EVENT_INCOMPLETE while reading a coordinate element's text
      // (pos/posList/coordinates). Any CHARACTERS that arrived after the pause landed in this
      // decoder's `buffer` via the main loop. Hand them over so continueDecoding can append them
      // to the geometry decoder's coord frame before finalising — otherwise the trailing chunk of
      // the coordinate text is silently dropped (observed: a posList split mid-number produced a
      // truncated odd coord count, which the dimension heuristic then promoted to XYZ).
      String pending = isBuffering ? buffer.toString() : "";
      if (isBuffering) {
        isBuffering = false;
        buffer.setLength(0);
      }
      Optional<Geometry<?>> optGeometry =
          geometryDecoder.continueDecoding(
              parser, crs, srsDimension, parser.getLocalName(), pending);
      if (optGeometry.isPresent()) {
        emitGeometry(optGeometry.get());
      }
      return;
    }

    // A VALUE_WRAPPER about to be popped must not flush the character buffer: the inner text was
    // emitted as CHARACTERS inside this wrapper, and the enclosing VALUE_PROPERTY still needs to
    // read it on its own end. For every other frame, capture and clear the buffer as before.
    Frame about = frames.peek();
    boolean preserveBuffer = about != null && about.kind == FrameKind.VALUE_WRAPPER;
    String bufferedText;
    if (preserveBuffer) {
      bufferedText = "";
    } else {
      bufferedText = isBuffering ? buffer.toString() : "";
      if (isBuffering) {
        isBuffering = false;
        buffer.setLength(0);
      }
    }

    depth--;

    Frame frame = frames.poll();
    if (frame != null && frame.kind == FrameKind.VALUE_PROPERTY) {
      // Re-track the property's path in case any UNKNOWN descendants shifted the path tracker
      // (GML's alternation rule forbids element children inside a scalar property, but we still
      // tolerate them defensively).
      context.pathTracker().track(frame.segment, frame.pathDepth);
      if (frame.nilOnCurrent) {
        if (nullValue.isPresent()) {
          context.setValue(nullValue.get());
          context.setValueType(Type.STRING);
          downstream.onValue(context);
        }
      } else if (frame.pendingXlinkHrefValue != null) {
        context.setValue(frame.pendingXlinkHrefValue);
        context.setValueType(Type.STRING);
        downstream.onValue(context);
      } else if (!bufferedText.isEmpty()) {
        context.setValue(bufferedText);
        context.setValueType(Type.STRING);
        downstream.onValue(context);
      } else if (frame.pendingXlinkHrefFallback != null) {
        context.setValue(frame.pendingXlinkHrefFallback);
        context.setValueType(Type.STRING);
        downstream.onValue(context);
      }
    } else if (frame != null && frame.kind == FrameKind.OBJECT_PROPERTY) {
      // Re-track the OBJECT_PROPERTY's own path before emitting onObjectEnd — nested child
      // elements may have pushed deeper segments onto the path tracker. For array non-FEATURE_REF
      // OBJECT_PROPERTYs the OBJECT pair was emitted per peer OBJECT_ELEMENT, so the prop END
      // just pops without an additional onObjectEnd.
      context.pathTracker().track(frame.segment, frame.pathDepth);
      boolean arrayOfObjects = frame.prop.isArray() && !frame.prop.isFeatureRef();
      if (!arrayOfObjects) {
        downstream.onObjectEnd(context);
      }
    } else if (frame != null && frame.kind == FrameKind.OBJECT_ELEMENT) {
      // Close any array still open inside this OBJECT_ELEMENT before bookkeeping at the enclosing
      // OBJECT_PROPERTY level. The path tracker still points at the array property's path from
      // the last child emit, which is where ARRAY_END belongs.
      if (frame.openArrayChildPath != null) {
        downstream.onArrayEnd(context);
      }
      // For array non-FEATURE_REF OBJECT_PROPERTYs the per-peer OBJECT pair is closed here, at
      // the path of the enclosing OBJECT_PROPERTY (the OBJECT_ELEMENT itself contributes no path
      // segment). For non-array OBJECT_PROPERTYs the OBJECT_ELEMENT END is silent — onObjectEnd
      // fires at the enclosing OBJECT_PROPERTY's END above.
      Frame enclosing = frames.peek();
      if (enclosing != null
          && enclosing.kind == FrameKind.OBJECT_PROPERTY
          && enclosing.prop.isArray()
          && !enclosing.prop.isFeatureRef()) {
        context.pathTracker().track(enclosing.segment, enclosing.pathDepth);
        downstream.onObjectEnd(context);
      }
    }

    if (inFeature && depth == featureRootDepth) {
      // Close any array still open at the last array property (path tracker still points at
      // it). Drops the last array bracket so ARRAY/ARRAY_END remain balanced for every feature.
      if (currentArrayPath != null) {
        downstream.onArrayEnd(context);
        currentArrayPath = null;
      }
      inFeature = false;
      downstream.onFeatureEnd(context);
    }

    if (depth == 0) {
      downstream.onEnd(context);
    }
  }

  private void onFeatureStart() {
    inFeature = true;
    crs = defaultCrs;
    featureGeometryCrs = Optional.empty();

    context.metadata().numberReturned(OptionalLong.of(1L));
    context.metadata().numberMatched(OptionalLong.of(1L));
    context.metadata().isSingleFeature(true);

    downstream.onStart(context);

    String gmlId = readGmlId();

    // No path-tracker segment for the feature root itself: emitted paths start at the property
    // name of the first child (depth 0). Property-name paths carry no source-system prefix,
    // so they line up directly with any downstream's property-name lookups.
    downstream.onFeatureStart(context);

    if (gmlId != null) {
      Optional<FeatureSchema> idProperty =
          featureSchema.getProperties().stream()
              .filter(p -> p.getRole().filter(r -> r == SchemaBase.Role.ID).isPresent())
              .findFirst();
      if (idProperty.isPresent()) {
        context.pathTracker().track(idProperty.get().getName(), 0);
        context.setValue(gmlId);
        context.setValueType(Type.STRING);
        downstream.onValue(context);
      }
    }

    emitVariableNameDiscriminator(
        featureSchema, 0, parser.getNamespaceURI(), parser.getLocalName());

    emitXmlAttributesOnCurrent(featureSchema, 0);
  }

  /**
   * Reverse of the encoder's {@code variableObjectElementNames} option. When the wire element of a
   * GML object (feature root or a nested OBJECT_PROPERTY's inner element) matches a configured
   * qualified name for {@code owner.objectType}, the mapped source value is emitted at the
   * discriminator property's source path at depth {@code childPathDepth}. The OBJECT element itself
   * is accepted independently of this method — at the feature root by {@link #onStartElement}'s
   * feature-type / variable-name accept check, at nested OBJECT_PROPERTYs by GML's alternation rule
   * (the single inner element is the object regardless of its name).
   *
   * <p>The lookup is by schema property <em>name</em> regardless of {@code useAlias}, since {@link
   * VariableObjectName#getProperty()} carries the source-side property identifier.
   */
  private void emitVariableNameDiscriminator(
      FeatureSchema owner, int childPathDepth, String wireNamespaceUri, String wireLocalName) {
    Optional<DiscriminatorMatch> match =
        resolveVariableNameDiscriminator(owner, wireNamespaceUri, wireLocalName);
    if (match.isEmpty()) {
      return;
    }
    Optional<FeatureSchema> propOpt =
        owner.getProperties().stream()
            .filter(p -> p.getName().equals(match.get().property))
            .findFirst();
    if (propOpt.isEmpty()) {
      return;
    }
    FeatureSchema prop = propOpt.get();
    context.pathTracker().track(prop.getName(), childPathDepth);
    context.setValue(match.get().value);
    context.setValueType(Type.STRING);
    downstream.onValue(context);
  }

  /**
   * Returns the discriminator property name and source value for the wire element {@code
   * (namespaceUri, localName)}, if a configured {@code variableObjectElementNames} entry for {@code
   * owner.objectType} maps the wire's qualified name to a value. Returns empty otherwise.
   */
  private Optional<DiscriminatorMatch> resolveVariableNameDiscriminator(
      FeatureSchema owner, String namespaceUri, String localName) {
    Optional<String> ownerObjectType = owner.getObjectType();
    if (ownerObjectType.isEmpty()) {
      return Optional.empty();
    }
    VariableObjectName variable =
        inputProfile.getVariableObjectElementNames().get(ownerObjectType.get());
    if (variable == null) {
      return Optional.empty();
    }
    String qualifiedWire =
        namespaceNormalizer.getQualifiedName(namespaceUri == null ? "" : namespaceUri, localName);
    String value = variable.getMapping().get(qualifiedWire);
    if (value == null) {
      return Optional.empty();
    }
    return Optional.of(new DiscriminatorMatch(variable.getProperty(), value));
  }

  private static final class DiscriminatorMatch {
    final String property;
    final String value;

    DiscriminatorMatch(String property, String value) {
      this.property = property;
      this.value = value;
    }
  }

  /**
   * Reverse of the encoder's {@code xmlAttributes} option: when a child property's {@code
   * fullPathAsString} is listed in {@link FeatureTokenDecoderGmlInputProfile#getXmlAttributes()},
   * the encoder writes the value as an unqualified XML attribute on the parent object element
   * instead of as a child element. Here we scan the current START_ELEMENT's unqualified attributes
   * and emit each match at the property's source path. Qualified attributes ({@code gml:id}, {@code
   * xsi:*}, {@code xlink:*}, …) are not candidates and are handled by the dedicated readers.
   *
   * @param parent the OBJECT schema that owns the attributes on the current element (the feature
   *     schema for the feature root).
   * @param emitDepth path tracker depth at which the emitted values sit ({@code 1} for direct
   *     children of the feature root).
   */
  private void emitXmlAttributesOnCurrent(FeatureSchema parent, int emitDepth) {
    List<String> configured = inputProfile.getXmlAttributes();
    if (configured.isEmpty()) {
      return;
    }
    for (int i = 0; i < parser.getAttributeCount(); i++) {
      String ns = parser.getAttributeNamespace(i);
      if (ns != null && !ns.isEmpty()) {
        continue;
      }
      String localName = parser.getAttributeLocalName(i);
      Optional<FeatureSchema> propOpt = lookupChild(parent, localName);
      if (propOpt.isEmpty()) {
        continue;
      }
      FeatureSchema prop = propOpt.get();
      if (!configured.contains(prop.getFullPathAsString())) {
        continue;
      }
      context.pathTracker().track(prop.getName(), emitDepth);
      context.setValue(parser.getAttributeValue(i));
      context.setValueType(Type.STRING);
      downstream.onValue(context);
    }
  }

  /**
   * Reads the {@code gml:id} attribute on the current START_ELEMENT, applying {@link
   * FeatureTokenDecoderGmlInputProfile#getGmlIdPrefix()} if set. Returns {@code null} if no {@code
   * gml:id} attribute is present.
   */
  private String readGmlId() {
    for (int i = 0; i < parser.getAttributeCount(); i++) {
      String qn =
          namespaceNormalizer.getQualifiedName(
              parser.getAttributeNamespace(i), parser.getAttributeLocalName(i));
      if ("gml:id".equals(qn)) {
        String value = parser.getAttributeValue(i);
        String prefix = inputProfile.getGmlIdPrefix();
        if (value != null && !prefix.isEmpty() && value.startsWith(prefix)) {
          return value.substring(prefix.length());
        }
        return value;
      }
    }
    return null;
  }

  /**
   * Resolves the wire-form local name of a child element to a property of {@code parent}. With
   * {@code useAlias} on, each property's {@code alias} is matched first (falling back to the
   * property name when no alias is set); otherwise the property name is matched directly. The
   * property's name/alias may carry an explicit {@code prefix:} which is stripped before the
   * local-name comparison.
   *
   * <p>This overload is used for XML attribute matching ({@link #emitXmlAttributesOnCurrent}),
   * where the wire attribute is unqualified and no namespace check applies.
   */
  private Optional<FeatureSchema> lookupChild(FeatureSchema parent, String wireLocalName) {
    if (wireLocalName == null) {
      return Optional.empty();
    }
    boolean useAlias = inputProfile.getUseAlias();
    return parent.getProperties().stream()
        .filter(p -> wireLocalName.equals(stripPrefix(propertyKey(p, useAlias))))
        .findFirst();
  }

  /**
   * Element-path variant of {@link #lookupChild(FeatureSchema, String)} that additionally enforces
   * the namespace expected for the property given the input profile's {@code applicationNamespaces}
   * / {@code defaultNamespace} / {@code objectTypeNamespaces}. The wire element's local name must
   * equal the property's name/alias (after stripping any {@code prefix:}), and its namespace URI
   * must equal the property's expected URI (see {@link #expectedNamespaceUri}); when no expected
   * URI is configured the wire URI is not checked.
   */
  private Optional<FeatureSchema> lookupChild(
      FeatureSchema parent, String wireLocalName, String wireNamespaceUri) {
    if (wireLocalName == null) {
      return Optional.empty();
    }
    boolean useAlias = inputProfile.getUseAlias();
    String wireUri = wireNamespaceUri == null ? "" : wireNamespaceUri;
    for (FeatureSchema p : parent.getProperties()) {
      String key = propertyKey(p, useAlias);
      String base = stripPrefix(key);
      // The configured set holds property ids (technical full paths), not the on-the-wire
      // name/alias, so membership is tested against the property's full path; the wire element
      // base for the suffix match stays the name/alias the encoder emits.
      boolean suffixed =
          inputProfile.getObjectTypeSuffixedProperties().contains(p.getFullPathAsString());
      if (!(wireLocalName.equals(base) || (suffixed && wireLocalName.startsWith(base + "_")))) {
        continue;
      }
      String expectedUri = expectedNamespaceUri(p, parent, key);
      if (expectedUri == null || expectedUri.equals(wireUri)) {
        return Optional.of(p);
      }
    }
    return Optional.empty();
  }

  private static String propertyKey(FeatureSchema p, boolean useAlias) {
    return useAlias ? p.getAlias().orElse(p.getName()) : p.getName();
  }

  /**
   * Returns {@code true} when the namespace URI belongs to a hardcoded external schema whose object
   * elements directly carry text content (ISO 19115 {@code gmd}/{@code gco} for now). Children of a
   * scalar property element in these namespaces are decoded as value wrappers around the scalar
   * text — see the entry point in {@link #onStartElement}. Other external schemas with the same
   * convention need to be added here when the need arises.
   */
  private static boolean isExternalContentNamespace(String uri) {
    return GMD_NS.equals(uri) || GCO_NS.equals(uri);
  }

  private static String stripPrefix(String key) {
    int colon = key.indexOf(':');
    return colon >= 0 ? key.substring(colon + 1) : key;
  }

  /**
   * Resolves the XML namespace URI expected for {@code property}, whose schema name/alias is {@code
   * key}. Mirrors the encoder's qualification chain:
   *
   * <ol>
   *   <li>An explicit {@code prefix:name} in the schema name/alias takes precedence. The prefix is
   *       resolved against the constructor's namespace map (predefined + {@code
   *       applicationNamespaces}).
   *   <li>Otherwise the property's {@code originObjectType} — the object type from the fragment
   *       that originally listed this property — is looked up in {@code objectTypeNamespaces}; its
   *       prefix resolves to the URI. The property's own origin always wins over the parent's
   *       {@code objectType}; setting it deliberately falls through to the default namespace if the
   *       type isn't mapped, suppressing the parent walk.
   *   <li>Otherwise the parent's {@code objectType} is looked up in {@code objectTypeNamespaces} —
   *       but only when the parent is a NESTED OBJECT, not the feature root. The feature root's
   *       {@code objectType} pins the namespace of the feature element itself; it must not
   *       propagate down to property children that inherited from a different schema fragment.
   *       Nested OBJECTs (e.g. ISO 19115 {@code LI_Lineage}) do propagate, since their inline child
   *       properties belong to the nested object's own namespace.
   *   <li>Otherwise the input profile's {@code defaultNamespace} prefix resolves to the URI.
   * </ol>
   *
   * Returns {@code null} when no namespace expectation is configured for this property — the caller
   * then matches by local name only, preserving the decoder's behaviour when no namespace data is
   * provided in the input profile.
   */
  private String expectedNamespaceUri(FeatureSchema property, FeatureSchema parent, String key) {
    int colon = key.indexOf(':');
    if (colon > 0) {
      return namespaceNormalizer.getNamespaceURI(key.substring(0, colon));
    }
    Optional<String> originObjectType = property.getOriginObjectType();
    if (originObjectType.isPresent()) {
      String prefix = inputProfile.getObjectTypeNamespaces().get(originObjectType.get());
      if (prefix != null) {
        return namespaceNormalizer.getNamespaceURI(prefix);
      }
    } else if (parent != featureSchema) {
      Optional<String> parentObjectType = parent.getObjectType();
      if (parentObjectType.isPresent()) {
        String prefix = inputProfile.getObjectTypeNamespaces().get(parentObjectType.get());
        if (prefix != null) {
          return namespaceNormalizer.getNamespaceURI(prefix);
        }
      }
    }
    String defaultPrefix = inputProfile.getDefaultNamespace();
    if (defaultPrefix != null && !defaultPrefix.isEmpty()) {
      return namespaceNormalizer.getNamespaceURI(defaultPrefix);
    }
    return null;
  }

  /**
   * Reads {@code xlink:href} on the current START_ELEMENT and reduces it to a bare value via the
   * appropriate reverse template, if the property routes xlink:href as its value. Returns {@code
   * null} when there is no href, when the property is not a codelist or feature-ref, or when the
   * property routes hrefs but no matching template is configured (the unchanged href is then
   * emitted as-is; mismatching template inputs also fall through to the unchanged href).
   */
  private String readXlinkHrefAsValue(FeatureSchema prop) {
    if (!shouldRouteXlinkHrefAsValue(prop)) {
      return null;
    }
    String href = readRawXlinkHref();
    if (href == null) {
      return null;
    }
    return reverseXlinkHrefTemplate(href, prop);
  }

  /** Returns the raw {@code xlink:href} attribute on the current START_ELEMENT, or {@code null}. */
  private String readRawXlinkHref() {
    for (int i = 0; i < parser.getAttributeCount(); i++) {
      if (XLINK_NS.equals(parser.getAttributeNamespace(i))
          && "href".equals(parser.getAttributeLocalName(i))) {
        return parser.getAttributeValue(i);
      }
    }
    return null;
  }

  /**
   * Reduces an {@code xlink:href} to its bare value segment via the appropriate reverse template
   * from the input profile. For codelist properties the schema's codelist id is substituted into
   * {@code {{codelistId}}} first. If no template is configured, the href is returned unchanged
   * silently. If a template is configured but the href does not match, the href is returned
   * unchanged and a warning is logged — the raw URI will almost always overflow the storage column
   * or be wrong as a value, so the operator needs visibility to fix the config mismatch.
   */
  private String reverseXlinkHrefTemplate(String href, FeatureSchema prop) {
    String template;
    if (prop.isFeatureRef()) {
      template = inputProfile.getFeatureRefTemplate();
    } else {
      Optional<String> codelistId = prop.getConstraints().flatMap(SchemaConstraints::getCodelist);
      if (codelistId.isEmpty()) {
        return href;
      }
      String raw = inputProfile.getCodelistUriTemplate();
      template = raw == null ? null : raw.replace("{{codelistId}}", codelistId.get());
    }
    Optional<String> reduced = applyReverseTemplate(template, href);
    if (reduced.isEmpty() && template != null && !template.isEmpty() && LOGGER.isWarnEnabled()) {
      LOGGER.warn(
          "xlink:href '{}' on property '{}' does not match the configured template '{}'; "
              + "the unchanged href is passed through as the value",
          href,
          prop.getFullPathAsString(),
          template);
    }
    return reduced.orElse(href);
  }

  private static boolean shouldRouteXlinkHrefAsValue(FeatureSchema prop) {
    if (prop.isFeatureRef()) {
      return true;
    }
    return prop.getConstraints().flatMap(SchemaConstraints::getCodelist).isPresent();
  }

  private static Optional<String> applyReverseTemplate(String template, String href) {
    if (template == null || template.isEmpty()) {
      return Optional.empty();
    }
    int idx = template.indexOf("{{value}}");
    if (idx < 0) {
      return Optional.empty();
    }
    String prefix = template.substring(0, idx);
    String suffix = template.substring(idx + "{{value}}".length());
    String regex = Pattern.quote(prefix) + "(.+?)" + Pattern.quote(suffix);
    Matcher m = Pattern.compile(regex).matcher(href);
    return m.matches() ? Optional.of(m.group(1)) : Optional.empty();
  }

  /**
   * The {@code uom} attribute on a numeric property is consistency information against the schema's
   * declared {@code unit}, not part of the value carried into the feature representation.
   * Reverse-map the wire form via {@code uomMappings} (if configured) and warn when the result does
   * not match the schema's unit. The attribute itself is dropped — the canonical unit lives in the
   * schema. No-op when the property has no declared unit or no unqualified {@code uom} attribute is
   * present.
   */
  private void validateUom(FeatureSchema prop) {
    Optional<String> schemaUnit = prop.getUnit();
    if (schemaUnit.isEmpty()) {
      return;
    }
    String wireUom = null;
    for (int i = 0; i < parser.getAttributeCount(); i++) {
      String ns = parser.getAttributeNamespace(i);
      if ((ns == null || ns.isEmpty()) && "uom".equals(parser.getAttributeLocalName(i))) {
        wireUom = parser.getAttributeValue(i);
        break;
      }
    }
    if (wireUom == null) {
      return;
    }
    String mapped = inputProfile.getUomMappings().getOrDefault(wireUom, wireUom);
    if (!schemaUnit.get().equals(mapped) && LOGGER.isWarnEnabled()) {
      LOGGER.warn(
          "uom attribute '{}' on property '{}' does not match the schema unit '{}'",
          wireUom,
          prop.getFullPathAsString(),
          schemaUnit.get());
    }
  }

  /**
   * Returns {@code true} when the current START_ELEMENT carries {@code xsi:nil="true"}. Other
   * xsi-namespaced attributes are caught by {@link #rejectXsiType()} (for {@code xsi:type}) or
   * ignored. {@code nilReason} (in no namespace) is unused on input and silently dropped.
   */
  private boolean readXsiNil() {
    for (int i = 0; i < parser.getAttributeCount(); i++) {
      if (XSI_NS.equals(parser.getAttributeNamespace(i))
          && "nil".equals(parser.getAttributeLocalName(i))) {
        return "true".equalsIgnoreCase(parser.getAttributeValue(i));
      }
    }
    return false;
  }

  /**
   * Whether the current START_ELEMENT is part of a value-wrap chain: its parent frame is a {@link
   * FrameKind#VALUE_WRAPPER}, or a {@link FrameKind#VALUE_PROPERTY} whose path is listed in {@code
   * valueWrap} or whose child lives in an ISO 19115 content namespace ({@code gmd}/{@code gco}).
   * Such elements carry no schema meaning — they only wrap the property's scalar text — so they are
   * pushed as {@link Frame#valueWrapper()} and exempt from {@link #rejectXsiType()}: ISO 19139
   * requires typed values like {@code <gco:Record xsi:type="gml:doubleList">} inside {@code
   * DQ_QuantitativeResult}, where {@code xsi:type} declares the content type of an anyType element
   * rather than substituting a schema type.
   */
  private boolean isValueWrapChainElement() {
    Frame parent = frames.peek();
    return parent != null
        && (parent.kind == FrameKind.VALUE_WRAPPER
            || (parent.kind == FrameKind.VALUE_PROPERTY
                && (parent.valueWrapped || isExternalContentNamespace(parser.getNamespaceURI()))));
  }

  /**
   * {@code xsi:type} substitution is not supported by this decoder — schema lookup is by element
   * name only, so a substituted type carries no extra information into the token stream and is
   * almost certainly user error. Reject early with a clear message naming the element on which it
   * appeared. Elements inside a value-wrap chain (see {@link #isValueWrapChainElement()}) are
   * exempt: there {@code xsi:type} types the content of an anyType value element (ISO 19139 {@code
   * gco:Record}) and is dropped on input; the encoder regenerates it from the attributes declared
   * on the {@code valueWrap} chain entry.
   */
  private void rejectXsiType() {
    for (int i = 0; i < parser.getAttributeCount(); i++) {
      if (XSI_NS.equals(parser.getAttributeNamespace(i))
          && "type".equals(parser.getAttributeLocalName(i))) {
        throw new IllegalArgumentException(
            "xsi:type on element <"
                + parser.getLocalName()
                + "> is not supported by this decoder.");
      }
    }
  }

  private void emitGeometry(Geometry<?> geom) {
    checkMixedCrs(geom.getCrs());
    context.setGeometry(geom);
    getDownstream().onGeometry(context);
  }

  /**
   * Geometries within a single feature must share the same resolved CRS — a feature with two
   * geometries in different CRSes is malformed because the consumer has no basis to choose one. The
   * check compares resolved {@link EpsgCrs} values, so two {@code srsName} forms that map to the
   * same EPSG code (e.g. via {@code srsNameMappings}) are treated as equal.
   */
  private void checkMixedCrs(Optional<EpsgCrs> geometryCrs) {
    if (featureGeometryCrs.isEmpty()) {
      featureGeometryCrs = geometryCrs;
      return;
    }
    if (geometryCrs.isPresent() && !geometryCrs.equals(featureGeometryCrs)) {
      throw new IllegalArgumentException(
          "Geometries within a single feature must share the same CRS but found "
              + featureGeometryCrs.get()
              + " and "
              + geometryCrs.get()
              + ".");
    }
  }

  /**
   * Compares a wire element {@code (namespaceUri, localName)} against a configured qualified name
   * of the form {@code prefix:localName}. The configured prefix is resolved against the namespace
   * normaliser; both the resolved URI and the local name must match. A configured value without a
   * prefix is rejected as misconfiguration — wrapper element names are expected in the same
   * qualified form the encoder writes.
   */
  private boolean matchesQualifiedElement(
      String namespaceUri, String localName, String configuredQualifiedName) {
    int colon = configuredQualifiedName.indexOf(':');
    if (colon <= 0) {
      throw new IllegalArgumentException(
          "Wrapper element name '"
              + configuredQualifiedName
              + "' must be in 'prefix:localName' form.");
    }
    String expectedPrefix = configuredQualifiedName.substring(0, colon);
    String expectedLocal = configuredQualifiedName.substring(colon + 1);
    String expectedUri = namespaceNormalizer.getNamespaceURI(expectedPrefix);
    String wireUri = namespaceUri == null ? "" : namespaceUri;
    return expectedLocal.equals(localName) && expectedUri != null && expectedUri.equals(wireUri);
  }

  private boolean matchesFeatureType(final String namespace, final String localName) {
    return featureTypes.stream()
        .anyMatch(
            featureType ->
                featureType.getLocalPart().equals(localName)
                    && Objects.nonNull(namespace)
                    && featureType.getNamespaceURI().equals(namespace));
  }

  private boolean matchesFeatureType(final String localName) {
    return featureTypes.stream()
        .anyMatch(featureType -> featureType.getLocalPart().equals(localName));
  }
}
