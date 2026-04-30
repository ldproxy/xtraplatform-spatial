/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.strings.domain;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.commonmark.Extension;
import org.commonmark.ext.gfm.tables.TablesExtension;
import org.commonmark.node.Link;
import org.commonmark.node.Node;
import org.commonmark.node.Paragraph;
import org.commonmark.parser.Parser;
import org.commonmark.renderer.html.CoreHtmlNodeRenderer;
import org.commonmark.renderer.html.HtmlRenderer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zahnen
 */
@SuppressWarnings({
  "PMD.NcssCount",
  "PMD.CognitiveComplexity",
  "PMD.CyclomaticComplexity",
  "PMD.NPathComplexity"
})
public final class StringTemplateFilters {

  private static final Logger LOGGER = LoggerFactory.getLogger(StringTemplateFilters.class);

  private static final Set<Extension> EXTENSIONS = Collections.singleton(TablesExtension.create());
  private static final Parser PARSER = Parser.builder().extensions(EXTENSIONS).build();

  private static final HtmlRenderer RENDERER =
      HtmlRenderer.builder()
          .extensions(EXTENSIONS)
          .nodeRendererFactory(
              context ->
                  new CoreHtmlNodeRenderer(context) {
                    @Override
                    public void visit(Paragraph paragraph) {
                      this.visitChildren(paragraph);
                    }
                  })
          .attributeProviderFactory(
              context ->
                  (node, tagName, attributes) -> {
                    if (node instanceof Link) {
                      attributes.put("target", "_blank");
                    }
                  })
          .build();

  private static final Pattern VALUE_PATTERN =
      Pattern.compile("\\{\\{([\\w.]+)( ?\\| ?[\\w]+(:'[^']*')*)*\\}\\}");
  private static final Pattern VALUE_PATTERN_SINGLE =
      Pattern.compile("\\{([\\w.]+)( ?\\| ?[\\w]+(:'[^']*')*)*\\}");
  private static final Pattern FILTER_PATTERN = Pattern.compile(" ?\\| ?([\\w]+)((?::'[^']*')*)");

  private StringTemplateFilters() {}

  public static String applyFilterMarkdown(String value) {

    Node document = PARSER.parse(value);
    return RENDERER.render(document);
  }

  public static String applyTemplate(String template, String value) {
    return applyTemplate(template, value, isHtml -> {});
  }

  public static String applyTemplate(String template, String value, Consumer<Boolean> isHtml) {
    return applyTemplate(template, value, isHtml, "value");
  }

  public static String applyTemplate(
      String template, String value, Consumer<Boolean> isHtml, String valueSubst) {
    if (Objects.isNull(value) || value.isEmpty()) {
      return "";
    }

    if (Objects.isNull(template) || template.isEmpty()) {
      return value;
    }

    return applyTemplate(
        template, isHtml, key -> Objects.equals(key, valueSubst) ? value : null, Map.of(), false);
  }

  public static String applyTemplate(String template, Function<String, String> valueLookup) {
    return applyTemplate(template, isHtml -> {}, valueLookup, Map.of(), false);
  }

  public static String applyTemplate(
      String template,
      Function<String, String> valueLookup,
      Map<String, Function<String, String>> customFilters) {
    return applyTemplate(template, isHtml -> {}, valueLookup, customFilters, false);
  }

  public static String applyTemplate(
      String template, Function<String, String> valueLookup, boolean allowSingleCurlyBraces) {
    return applyTemplate(template, isHtml -> {}, valueLookup, Map.of(), allowSingleCurlyBraces);
  }

  public static String applyTemplate(
      String template,
      Consumer<Boolean> isHtml,
      Function<String, String> valueLookup,
      Map<String, Function<String, String>> customFilters,
      boolean allowSingleCurlyBraces) {

    if (Objects.isNull(template) || template.isEmpty()) {
      return "";
    }

    StringBuilder formattedValueBuilder = new StringBuilder();
    Matcher matcher =
        !allowSingleCurlyBraces || template.contains("{{")
            ? VALUE_PATTERN.matcher(template)
            : VALUE_PATTERN_SINGLE.matcher(template);
    boolean hasAppliedMarkdown = false;
    Map<String, String> assigns = new HashMap<>();

    int lastMatch = 0;
    while (matcher.find()) {
      String key = matcher.group(1);
      String filteredValue = valueLookup.apply(key);
      Matcher matcher2 = FILTER_PATTERN.matcher(template.substring(matcher.start(), matcher.end()));
      while (matcher2.find()) {
        String filter = matcher2.group(1);
        String params = matcher2.group(2);
        List<String> parameters =
            matcher2.groupCount() < 2
                ? ImmutableList.of()
                : Splitter.onPattern("(?<=^|'):(?='|$)")
                    .omitEmptyStrings()
                    .splitToList(params)
                    .stream()
                    .map(s -> s.substring(1, s.length() - 1))
                    .collect(Collectors.toList());

        if (Objects.isNull(filteredValue)) {
          if ("orElse".equals(filter) && !parameters.isEmpty()) {
            filteredValue =
                applyTemplate(
                    parameters.get(0).replace("\"", "'"),
                    isHtml,
                    valueLookup,
                    customFilters,
                    false);
          }
        } else {
          if ("markdown".equals(filter)) {
            filteredValue = applyFilterMarkdown(filteredValue);
            hasAppliedMarkdown = true;
          } else if ("replace".equals(filter) && parameters.size() >= 2) {
            filteredValue = filteredValue.replaceAll(parameters.get(0), parameters.get(1));
          } else if ("prepend".equals(filter) && !parameters.isEmpty()) {
            filteredValue = parameters.get(0).concat(filteredValue);
          } else if ("append".equals(filter) && !parameters.isEmpty()) {
            filteredValue = filteredValue.concat(parameters.get(0));
          } else if ("urlEncode".equals(filter) || "urlencode".equals(filter)) {
            try {
              filteredValue = URLEncoder.encode(filteredValue, Charsets.UTF_8.toString());
            } catch (UnsupportedEncodingException e) {
              // ignore
            }
          } else if ("toLower".equals(filter)) {
            filteredValue = filteredValue.toLowerCase(Locale.ROOT);
          } else if ("toUpper".equals(filter)) {
            filteredValue = filteredValue.toUpperCase(Locale.ROOT);
          } else if ("assignTo".equals(filter) && !parameters.isEmpty()) {
            assigns.put(parameters.get(0), filteredValue);
          } else if ("unHtml".equals(filter)) {
            filteredValue = filteredValue.replaceAll("<.*?>", "");
          } else if (customFilters.containsKey(filter)) {
            filteredValue = customFilters.get(filter).apply(filteredValue);
          } else if (!"orElse".equals(filter)) {
            LOGGER.warn("Template filter '{}' not supported", filter);
          }
        }
      }
      formattedValueBuilder
          .append(template, lastMatch, matcher.start())
          .append(Objects.requireNonNullElse(filteredValue, ""));
      lastMatch = matcher.end();
    }
    formattedValueBuilder.append(template.substring(lastMatch));
    String formattedValue = formattedValueBuilder.toString();
    for (Map.Entry<String, String> entry : assigns.entrySet()) {
      String valueSubst2 = entry.getKey();
      String value2 = entry.getValue();
      formattedValue = formattedValue.replaceAll("\\{\\{" + valueSubst2 + "}}", value2);
    }

    Matcher recurseMatcher =
        !allowSingleCurlyBraces || formattedValue.contains("{{")
            ? VALUE_PATTERN.matcher(formattedValue)
            : VALUE_PATTERN_SINGLE.matcher(formattedValue);

    if (recurseMatcher.find()) {
      final boolean currentHasAppliedMarkdown = hasAppliedMarkdown;
      return applyTemplate(
          formattedValue,
          nextHasAppliedMarkdown ->
              isHtml.accept(currentHasAppliedMarkdown || nextHasAppliedMarkdown),
          valueLookup,
          customFilters,
          allowSingleCurlyBraces);
    }

    isHtml.accept(hasAppliedMarkdown);

    return formattedValue;
  }
}
