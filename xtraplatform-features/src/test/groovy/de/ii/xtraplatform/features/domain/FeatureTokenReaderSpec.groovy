/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain


import de.ii.xtraplatform.geometries.domain.GeometryType
import de.ii.xtraplatform.geometries.domain.Axes
import de.ii.xtraplatform.geometries.domain.MultiPoint
import de.ii.xtraplatform.geometries.domain.Position
import spock.lang.Specification

/**
 * @author zahnen
 */
class FeatureTokenReaderSpec extends Specification {

    FeatureEventHandlerGeneric eventHandler
    FeatureTokenReader tokenReader

    def setup() {
        eventHandler = Mock()

        FeatureEventHandler.ModifiableContext context = ModifiableGenericContext.create();
        SchemaMapping mapping = Mock()
        mapping.getPathSeparator() >> Optional.empty()
        context.setMappings([ft: mapping])
        context.setType('ft')
        context.setQuery(ImmutableFeatureQuery.builder().type('ft').build())
        tokenReader = new FeatureTokenReader(eventHandler, context)
    }

    def 'single feature'() {

        given:

        when:

        FeatureTokenFixtures.SINGLE_FEATURE.forEach(token -> tokenReader.onToken(token))

        then:
        1 * eventHandler.onStart({ FeatureEventHandler.ModifiableContext context ->
            context.metadata().isSingleFeature()
        })

        then:
        1 * eventHandler.onFeatureStart(_)

        then:
        1 * eventHandler.onValue({ FeatureEventHandler.ModifiableContext context ->
            context.path() == ["id"]
            context.value() == "24"
            context.valueType() == SchemaBase.Type.STRING
        })

        then:
        1 * eventHandler.onValue({ FeatureEventHandler.ModifiableContext context ->
            context.path() == ["kennung"]
            context.value() == "611320001-1"
            context.valueType() == SchemaBase.Type.STRING
        })

        then:
        1 * eventHandler.onFeatureEnd(_)

        then:
        1 * eventHandler.onEnd(_)

    }

    def 'collection with 3 features'() {

        given:

        when:

        FeatureTokenFixtures.COLLECTION.forEach(token -> tokenReader.onToken(token))

        then:
        1 * eventHandler.onStart({ FeatureEventHandler.ModifiableContext context ->
            context.metadata().getNumberReturned() == OptionalLong.of(3)
            context.metadata().getNumberMatched() == OptionalLong.of(12)
        })

        then:
        1 * eventHandler.onFeatureStart(_)

        then:
        1 * eventHandler.onValue({ FeatureEventHandler.ModifiableContext context ->
            context.path() == ["id"]
            context.value() == "19"
            context.valueType() == SchemaBase.Type.STRING
            (!context.inObject())
        })

        then:
        1 * eventHandler.onGeometry({ FeatureEventHandler.ModifiableContext context ->
            context.path() == ["geometry"]
            context.geometry().getValue() ==  Position.ofXY(6.295202392345018, 50.11336914792363)
            context.geometry().getType() == GeometryType.POINT
            context.geometry().getAxes() == Axes.XY
        })

        then:
        1 * eventHandler.onValue({ FeatureEventHandler.ModifiableContext context ->
            context.path() == ["kennung"]
            context.value() == "580340001-1"
            context.valueType() == SchemaBase.Type.STRING
            (!context.inObject())
            (!context.inArray())
        })

        then:
        1 * eventHandler.onFeatureEnd(_)

        then:
        1 * eventHandler.onFeatureStart(_)

        then:
        1 * eventHandler.onValue({ FeatureEventHandler.ModifiableContext context ->
            context.path() == ["id"]
            context.value() == "20"
            context.valueType() == SchemaBase.Type.STRING
        })

        then:
        1 * eventHandler.onValue({ FeatureEventHandler.ModifiableContext context ->
            context.path() == ["kennung"]
            context.value() == "580410003-1"
            context.valueType() == SchemaBase.Type.STRING
        })

        then:
        1 * eventHandler.onFeatureEnd(_)

        then:
        1 * eventHandler.onFeatureStart(_)

        then:
        1 * eventHandler.onValue({ FeatureEventHandler.ModifiableContext context ->
            context.path() == ["id"]
            context.value() == "21"
            context.valueType() == SchemaBase.Type.STRING
        })

        then:
        1 * eventHandler.onGeometry({ FeatureEventHandler.ModifiableContext context ->
            context.path() == ["geometry"]
            ((MultiPoint) context.geometry()).getValue().get(0).getValue() == Position.ofXY(6.406233970262905, 50.1501333536934)
            context.geometry().getType() == GeometryType.MULTI_POINT
            context.geometry().getAxes() == Axes.XY
        })

        then:
        1 * eventHandler.onValue({ FeatureEventHandler.ModifiableContext context ->
            context.path() == ["kennung"]
            context.value() == "631510001-1"
            context.valueType() == SchemaBase.Type.STRING
        })

        then:
        1 * eventHandler.onFeatureEnd(_)

        then:
        1 * eventHandler.onEnd(_)

    }
}
