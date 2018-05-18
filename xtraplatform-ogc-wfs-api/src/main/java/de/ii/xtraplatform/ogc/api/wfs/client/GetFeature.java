package de.ii.xtraplatform.ogc.api.wfs.client;

import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.ogc.api.GML;
import de.ii.xtraplatform.ogc.api.Versions;
import de.ii.xtraplatform.ogc.api.WFS;
import de.ii.xtraplatform.util.xml.XMLDocument;
import de.ii.xtraplatform.util.xml.XMLDocumentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author zahnen
 */
public class GetFeature implements WfsOperation {
    enum RESULT_TYPE {
        RESULT,
        HITS
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(WFSOperation.class);

    private final List<WFSQuery2> query;
    private final Integer count;
    private final Integer startIndex;
    private final RESULT_TYPE resultType;

    public GetFeature(List<WFSQuery2> query, Integer count, Integer startIndex, RESULT_TYPE resultType) {
        this.query = query;
        this.count = count;
        this.startIndex = startIndex;
        this.resultType = resultType;
    }

    @Override
    public WFS.OPERATION getOperation() {
        return WFS.OPERATION.GET_FEATURE;
    }


    @Override
    public XMLDocument asXml(XMLDocumentFactory documentFactory, Versions versions) throws TransformerException, IOException, SAXException {
        final XMLDocument doc = documentFactory.newDocument();
        doc.addNamespace(WFS.getNS(versions.getWfsVersion()), WFS.getPR(versions.getWfsVersion()));

        Element operation = doc.createElementNS(WFS.getNS(versions.getWfsVersion()), getOperationName(versions.getWfsVersion()));
        operation.setAttribute("service", "WFS");
        doc.appendChild(operation);

        if (this.count != null) {
            operation.setAttribute(WFS.getWord(versions.getWfsVersion(), WFS.VOCABULARY.COUNT), String.valueOf(count));
        }

        if (this.startIndex != null && versions.getWfsVersion().isGreaterOrEqual(WFS.VERSION._2_0_0)) {
            operation.setAttribute(WFS.getWord(versions.getWfsVersion(), WFS.VOCABULARY.STARTINDEX), String.valueOf(startIndex));
        }

        if (this.resultType == RESULT_TYPE.HITS) {
            operation.setAttribute(WFS.getWord(versions.getWfsVersion(), WFS.VOCABULARY.RESULT_TYPE), String.valueOf(RESULT_TYPE.HITS));
        }

        if (versions.getGmlVersion() != null && versions.getWfsVersion() != null) {
            operation.setAttribute(GML.getWord(versions.getWfsVersion(), WFS.VOCABULARY.OUTPUT_FORMAT), GML.getWord(versions.getGmlVersion(), GML.VOCABULARY.OUTPUTFORMAT_VALUE));
            operation.setAttribute(WFS.getWord(versions.getWfsVersion(), WFS.VOCABULARY.VERSION), versions.getWfsVersion().toString());
        }

        for (WFSQuery2 q : query) {
            operation.appendChild(q.asXml(doc, versions));
        }

        doc.done();

        return doc;
    }

    @Override
    public Map<String,String> asKvp(XMLDocumentFactory documentFactory, Versions versions) throws TransformerException, IOException, SAXException {
        final XMLDocument doc = documentFactory.newDocument();
        final ImmutableMap.Builder<String,String> builder = ImmutableMap.builder();

        builder.put("SERVICE", "WFS");
        builder.put("REQUEST", getOperationName(versions.getWfsVersion()));

        builder.put(GML.getWord(versions.getWfsVersion(), WFS.VOCABULARY.OUTPUT_FORMAT).toUpperCase(), GML.getWord(versions.getGmlVersion(), GML.VOCABULARY.OUTPUTFORMAT_VALUE));
        builder.put(WFS.getWord(versions.getWfsVersion(), WFS.VOCABULARY.VERSION).toUpperCase(), versions.getWfsVersion().toString());

        if (this.count != null) {
            builder.put(WFS.getWord(versions.getWfsVersion(), WFS.VOCABULARY.COUNT).toUpperCase(), String.valueOf(count));
        }

        if (this.startIndex != null && versions.getWfsVersion().isGreaterOrEqual(WFS.VERSION._2_0_0)) {
            builder.put(WFS.getWord(versions.getWfsVersion(), WFS.VOCABULARY.STARTINDEX).toUpperCase(), String.valueOf(startIndex));
        }

        if (this.resultType == RESULT_TYPE.HITS) {
            builder.put(WFS.getWord(versions.getWfsVersion(), WFS.VOCABULARY.RESULT_TYPE).toUpperCase(), String.valueOf(RESULT_TYPE.HITS).toLowerCase());
        }

        if (!query.isEmpty()) {
            builder.putAll(query.get(0).asKvp(doc, versions));
        }

        final String namespaces = doc.getNamespaceNormalizer()
                            .getNamespaces()
                            .keySet()
                            .stream()
                            .map(prefix -> "xmlns(" + prefix + "," + doc.getNamespaceNormalizer().getNamespaceURI(prefix) + ")")
                            .collect(Collectors.joining(","));

        builder.put("NAMESPACES", namespaces);

        return builder.build();
    }
}
