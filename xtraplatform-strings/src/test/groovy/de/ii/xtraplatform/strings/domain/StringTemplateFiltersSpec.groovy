/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.strings.domain

import spock.lang.Specification

import java.util.function.Function

class StringTemplateFiltersSpec extends Specification {

    def 'All arguments are null'() {
        when:
        String result = StringTemplateFilters.applyTemplate(null, null, null, null)
        then:
        result.isEmpty()
    }

    def 'Template is null, value is present'() {
        given:
        String template = null
        String value = "foobar"
        when:
        String result = StringTemplateFilters.applyTemplate(template, value)
        then:
        result == value
    }

    def 'Value is null, template is present'() {
        given:
        String template = "{{value | replace:'ABC':' '}}"
        String value = null
        when:
        String result = StringTemplateFilters.applyTemplate(template, (String) value)
        then:
        result.isEmpty()
    }

    def 'Template is empty, value is present'() {
        given:
        String template = ""
        String value = "foobar"
        when:
        String result = StringTemplateFilters.applyTemplate(template, value)
        then:
        result == value
    }

    def 'Value is empty, template is present'() {
        given:
        String template = "{{value | replace:'ABC':' '}}"
        String value = ""
        when:
        String result = StringTemplateFilters.applyTemplate(template, value)
        then:
        result.isEmpty()
    }

    def 'Malformed template'() {
        given:
        String template = "{replace:'ABC':' '}}"
        String value = "foobar"
        when:
        String result = StringTemplateFilters.applyTemplate(template, value)
        then:
        result == template
    }

    def 'Template does not follow the pattern'() {
        given:
        String template = "foo"
        String value = "bar"
        when:
        String result = StringTemplateFilters.applyTemplate(template, value)
        then:
        result == template
    }

    def 'replace test'() {
        given:
        String template = "{{value | replace:'ABC':' '}}"
        String value = "foobarABCtest"
        when:
        String result = StringTemplateFilters.applyTemplate(template, value)
        then:
        result == "foobar test"
    }

    def 'append test'() {
        given:
        String template = "{{value | append:'bar'}}"
        String value = "foo"
        when:
        String result = StringTemplateFilters.applyTemplate(template, value)
        then:
        result == "foobar"
    }

    def 'prepend test'() {
        given:
        String template = "{{value | prepend:'bar'}}"
        String value = "foo"
        when:
        String result = StringTemplateFilters.applyTemplate(template, value)
        then:
        result == "barfoo"
    }

    def 'toLower test'() {
        given:
        String template = "{{value | toLower}}"
        String value = "fooBAR"
        when:
        String result = StringTemplateFilters.applyTemplate(template, value)
        then:
        result == "foobar"
    }

    def 'toUpper test'() {
        given:
        String template = "{{value | toUpper}}"
        String value = "fooBAR"
        when:
        String result = StringTemplateFilters.applyTemplate(template, value)
        then:
        result == "FOOBAR"
    }

    def 'urlEncode test'() {
        given:
        String template = "{{value | urlEncode}}"
        String value = "query=foo&bar test"
        when:
        String result = StringTemplateFilters.applyTemplate(template, value)
        then:
        result == "query%3Dfoo%26bar+test"
    }

    def 'urlencode test'() {
        given:
        String template = "{{value | urlencode}}"
        String value = "query=foo&bar test"
        when:
        String result = StringTemplateFilters.applyTemplate(template, value)
        then:
        result == "query%3Dfoo%26bar+test"
    }

    def 'markdown test'() {
        given:
        String template = "{{value | markdown}}"
        String value = "foo*bar*"
        when:
        String result = StringTemplateFilters.applyTemplate(template, value)
        then:
        result == "foo<em>bar</em>"
    }

    def 'markdown with newline'() {
        given:
        String template = "{{value | markdown}}"
        String value = "foo\\\n*bar*"
        when:
        String result = StringTemplateFilters.applyTemplate(template, value)
        then:
        result == "foo<br />\n<em>bar</em>"
    }

    def 'markdown test with link'() {
        given:

        String value = "[example](http://www.example.com)"
        String template = "{{value | markdown}}"

        when:

        String result = StringTemplateFilters.applyTemplate(template, value)
        then:

        result == "<a href=\"http://www.example.com\" target=\"_blank\">example</a>"
    }

    def 'assignTo test'() {
        given:
        String template = "{{value | append:'bar' | assignTo:'foobar' | toUpper | append:' {{foobar}}'}}"
        String value = "foo"
        when:
        String result = StringTemplateFilters.applyTemplate(template, value)
        then:
        result == "FOOBAR foobar"
    }

    def 'Multiple filters'() {
        given:
        String template = "{{value | prepend:'query=' | append:' test' | urlencode}}"
        String value = "foo&bar"
        when:
        String result = StringTemplateFilters.applyTemplate(template, value)
        then:
        result == "query%3Dfoo%26bar+test"
    }

    def 'Template is empty'() {
        given:
        Function<String, String> function = s -> null
        when:
        String result = StringTemplateFilters.applyTemplate("",
                isHtml -> { },
                function, Map.of(), false)
        then:
        result.isEmpty()
    }

    def 'Template is null'() {
        given:
        Function<String, String> function = s -> null
        when:
        String result = StringTemplateFilters.applyTemplate(null,
                isHtml -> { }, function, Map.of(), false,
        )
        then:
        result.isEmpty()
    }

    def 'Test key does not equal valueSubst'() {
        given:
        String template = "{{value | replace:'ABC':' '}}"
        String value = "testABCtest"
        when:
        String result = StringTemplateFilters.applyTemplate(template, value,
                isHtml -> { },
                "")

        then:
        result.isEmpty()
    }

    def 'Test template and valueLookup'() {
        given:
        String template = "{{value | replace:'ABC':''}}"
        String value = "TestABC"
        Function<String, String> function = s -> value
        when:
        String result = StringTemplateFilters.applyTemplate(template, function)
        then:
        result == "Test"
    }

    def 'Test unHtml'() {
        given:
        String template = "{{value | unHtml}}"
        String value = "<test>test"
        Function<String, String> function = s -> value

        when:
        String result = StringTemplateFilters.applyTemplate(template,
                isHtml -> { },
                function, Map.of(), false)

        then:
        result == "test"

    }

    def 'Test custom template filter'() {
        given:
        String template = "{{value | test}}"
        String value = "<test>test"
        Function<String, String> function = s -> value
        Map<String, Function<String, String>> customFilters = Map.of("test", (Function<String, String>) (s) -> s.replace("<test>", ""))

        when:
        String result = StringTemplateFilters.applyTemplate(template,
                isHtml -> { },
                function, customFilters, false)

        then:

        result == "test"

    }

    def 'Test not supported template filter'() {
        given:
        String template = "{{value | test}}"
        String value = "<test>test"
        Function function = s -> value

        when:
        String result = StringTemplateFilters.applyTemplate(template,
                isHtml -> { },
                function, Map.of(), false)

        then:

        result == "<test>test"

    }

    def 'Test orElse where the value is present'() {
        given:
        String template = "{{title | orElse:'{{id | prepend:\"#\"}}'}}"
        Map<String, String> lookup = new HashMap<>();
        lookup.put("title", "test")
        lookup.put("id", "1")
        when:
        String result = StringTemplateFilters.applyTemplate(template, lookup::get)

        then:
        result == "test"
    }

    def 'Test orElse where the value is not present'() {
        given:
        String template = "{{title | orElse:'{{id | prepend:\"#\"}}'}}"
        Map<String, String> lookup = new HashMap<>();
        lookup.put("id", "1")
        when:
        String result = StringTemplateFilters.applyTemplate(template, lookup::get)

        then:
        result == "#1"
    }

}
