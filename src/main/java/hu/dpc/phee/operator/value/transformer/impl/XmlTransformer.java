package hu.dpc.phee.operator.value.transformer.impl;

import hu.dpc.phee.operator.config.model.Transformer;
import hu.dpc.phee.operator.value.transformer.TransformType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.StringReader;

@Slf4j
public class XmlTransformer extends SimpleTransformer {

    private static final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    private static final XPathFactory xPathFactory = XPathFactory.newInstance();

    public XmlTransformer(Transformer transformer) {
        super(transformer);
    }

    @Override
    public TransformType getTransformType() {
        return TransformType.XML_PATH;
    }

    @Override
    public void apply(Object entity, Object value) {
        log.debug("applying xpath for variable {}", getTransformer().getVariableName());
        String result;
        try {
            Document document = documentBuilderFactory.newDocumentBuilder().parse(new InputSource(new StringReader(String.valueOf(value))));
            result = xPathFactory.newXPath().compile(getTransformer().getXpath()).evaluate(document);
        } catch (SAXException | IOException | ParserConfigurationException | XPathExpressionException e) {
            log.error("failed to get xml value for field {} with xml path {}", getTransformer().getField(), getTransformer().getXpath(), e);
            return;
        }
        if (StringUtils.isNotBlank(result)) {
            setPropertyValue(entity, result);
        } else {
            log.warn("null result when setting field {} from variable {} using xml path {} of value {}", getTransformer().getField(), getTransformer().getVariableName(), getTransformer().getXpath(), value);
        }
    }
}