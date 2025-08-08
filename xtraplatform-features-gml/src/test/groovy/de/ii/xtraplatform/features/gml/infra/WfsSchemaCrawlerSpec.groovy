/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra

import com.fasterxml.aalto.stax.InputFactoryImpl
import com.google.common.collect.ImmutableMap
import de.ii.xtraplatform.features.gml.infra.req.GetCapabilities
import de.ii.xtraplatform.features.gml.infra.req.WfsOperation
import de.ii.xtraplatform.features.gml.app.WfsRequestEncoder
import de.ii.xtraplatform.features.gml.domain.ConnectionInfoWfsHttp
import de.ii.xtraplatform.features.gml.domain.ImmutableConnectionInfoWfsHttp
import de.ii.xtraplatform.features.gml.domain.WfsConnector
import de.ii.xtraplatform.features.domain.Metadata
import de.ii.xtraplatform.features.domain.SchemaBase
import de.ii.xtraplatform.geometries.domain.GeometryType
import de.ii.xtraplatform.features.gml.infra.req.WFS
import org.codehaus.staxmate.SMInputFactory
import spock.lang.Ignore
import spock.lang.Specification

//TODO: use file based MockWfsConnectorHttp
class WfsSchemaCrawlerSpec extends Specification {

    @Ignore
    def 'parse schema'() {

        given:
        def connectionInfo = new ImmutableConnectionInfoWfsHttp.Builder()
                .version("2.0.0")
                .gmlVersion("3.2.1")
                .uri(URI.create("https://www.wfs.nrw.de/geobasis/wfs_nw_alkis_vereinfacht"))
                .build()

        WfsConnector connector = new MockWfsConnectorHttp(connectionInfo)
        def wfsSchemaCrawler = new WfsSchemaCrawler(connector, connectionInfo)

        when:
        def featureTypeList = wfsSchemaCrawler.parseSchema()

        then:

        featureTypeList.size() == 7
        featureTypeList.get(0).getName() == "flurstueck"
        featureTypeList.get(0).getSourcePath().get() == "/ave:Flurstueck"
        featureTypeList.get(0).getPropertyMap().containsKey("id")
        featureTypeList.get(0).getPropertyMap().get("id").role.get() == SchemaBase.Role.ID
        featureTypeList.get(0).getPropertyMap().get("id").type == SchemaBase.Type.STRING
        featureTypeList.get(0).getPropertyMap().get("id").sourcePath.get() == "gml:@id"
        featureTypeList.get(0).getPropertyMap().containsKey("kreis")
        featureTypeList.get(0).getPropertyMap().get("kreis").additionalInfo.get("multiple") == "false"
        featureTypeList.get(0).getPropertyMap().get("kreis").type == SchemaBase.Type.STRING
        featureTypeList.get(0).getPropertyMap().get("kreis").sourcePath.get() == "ave:kreis"
        featureTypeList.get(0).getPropertyMap().containsKey("geometrie")
        featureTypeList.get(0).getPropertyMap().get("geometrie").type == SchemaBase.Type.GEOMETRY
        featureTypeList.get(0).getPropertyMap().get("geometrie").geometryType.get() == GeometryType.MULTI_POLYGON
        featureTypeList.get(0).getPropertyMap().get("geometrie").sourcePath.get() == "ave:geometrie"
        featureTypeList.get(0).getPropertyMap().get("geometrie").additionalInfo.get("crs") == "25832"
        featureTypeList.get(0).getPropertyMap().get("geometrie").additionalInfo.get("multiple") == "false"
        featureTypeList.get(0).getPropertyMap().get("flaeche").type == SchemaBase.Type.FLOAT
        featureTypeList.get(0).getPropertyMap().get("aktualit").type == SchemaBase.Type.DATETIME

    }

    @Ignore
    def 'test complex schema'() {
        given:
        def conn = new ImmutableConnectionInfoWfsHttp.Builder()
                .version("2.0.0")
                .gmlVersion("3.2.1")
                .uri(URI.create("https://www.wfs.nrw.de/geobasis/wfs_nw_inspire-adressen_gebref?SERVICE=WFS&REQUEST=GetCapabilities"))
                .build()

        WfsConnector connector = new MockWfsConnectorHttp(conn)
        def wfsSchemaCrawler = new WfsSchemaCrawler(connector, conn)

        when:
        def featureTypeList = wfsSchemaCrawler.parseSchema()

        then:
        featureTypeList.size() == 4
        featureTypeList.get(0).getPropertyMap().containsKey("id")
        featureTypeList.get(0).getPropertyMap().get("id").getRole().get() == SchemaBase.Role.ID
        featureTypeList.get(0).getPropertyMap().get("id").getType() == SchemaBase.Type.STRING
        featureTypeList.get(0).getPropertyMap().get("id").getSourcePath().get() == "gml:@id"
        featureTypeList.get(0).getPropertyMap().containsKey("inspireId")
        featureTypeList.get(0).getPropertyMap().get("inspireId").getType() == SchemaBase.Type.OBJECT
        featureTypeList.get(0).getPropertyMap().get("inspireId").getProperties().size() == 3
        featureTypeList.get(0).getPropertyMap().containsKey("validFrom")
        featureTypeList.get(0).getPropertyMap().get("validFrom").getType() == SchemaBase.Type.DATETIME
        featureTypeList.get(0).getPropertyMap().get("validFrom").getSourcePath().get() == "validFrom"
        featureTypeList.get(0).getPropertyMap().get("position").getType() == SchemaBase.Type.OBJECT_ARRAY
        featureTypeList.get(0).getPropertyMap().get("position").getPropertyMap().get("geometry").getType() == SchemaBase.Type.GEOMETRY
        featureTypeList.get(0).getPropertyMap().get("position").getPropertyMap().get("geometry").getGeometryType().get() == GeometryType.POINT
        featureTypeList.get(0).getPropertyMap().get("pronunciation").getType() == SchemaBase.Type.OBJECT_ARRAY
        featureTypeList.get(0).getPropertyMap().get("pronunciation").getPropertyMap().get("pronunciationIPA").getType() == SchemaBase.Type.VALUE_ARRAY
        featureTypeList.get(0).getPropertyMap().get("pronunciation").getPropertyMap().get("pronunciationIPA").getValueType().get() == SchemaBase.Type.STRING
        featureTypeList.get(0).getPropertyMap().get("pronunciation").getPropertyMap().get("pronunciationIPA").getSourcePath().get() ==
                "locator/name/name/pronunciation/pronunciationIPA"

    }

    class MockWfsConnectorHttp extends WfsConnectorHttp {

        private final WfsRequestEncoder wfsRequestEncoder
        private static SMInputFactory staxFactory = new SMInputFactory(new InputFactoryImpl())
        ConnectionInfoWfsHttp connectionInfo;


        MockWfsConnectorHttp(ConnectionInfoWfsHttp connectionInfo) {
            Map<String, Map<WFS.METHOD, URI>> urls = ImmutableMap.of("default", ImmutableMap.of(WFS.METHOD.GET, parseAndCleanWfsUrl(connectionInfo.getUri()), WFS.METHOD.POST, parseAndCleanWfsUrl(connectionInfo.getUri())));
            this.connectionInfo = connectionInfo
            this.wfsRequestEncoder = new WfsRequestEncoder(connectionInfo.getVersion(), connectionInfo.getGmlVersion(), connectionInfo.getNamespaces(), urls)
        }

        @Override
        InputStream runWfsOperation(WfsOperation operation) {
            return new URL(wfsRequestEncoder.getAsUrl(operation)).openConnection().getInputStream()
        }

        @Override
        Optional<Metadata> getMetadata() {
            try {

                InputStream inputStream = runWfsOperation(new GetCapabilities())
                WfsCapabilitiesAnalyzer metadataConsumer = new WfsCapabilitiesAnalyzer()
                WFSCapabilitiesParser gmlSchemaParser = new WFSCapabilitiesParser(metadataConsumer, staxFactory)
                gmlSchemaParser.parse(inputStream)
                return Optional.of(metadataConsumer.getMetadata())

            } catch (Throwable e) {
                e.printStackTrace()
            }

            return Optional.empty();
        }

        @Override
        String getProviderId() {
            return null
        }
    }
}
