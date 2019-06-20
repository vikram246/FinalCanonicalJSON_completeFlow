package demo;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.json.JSONObject;
import org.json.XML;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

public class XMLCreator {

	public static DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

	public void createXML(String JSONString) {

		try {
			DocumentBuilder dBuilder = dbf.newDocumentBuilder();
			DOMImplementation impl = dBuilder.getDOMImplementation();
			Document doc = impl.createDocument("http://schemas.xmlsoap.org/soap/envelope/", "SOAP-ENV:Envelope", null);
			Element root = doc.getDocumentElement();
			root.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:ws", "http://www.informatica.com/dis/ws/");

			// header
			Element header = doc.createElement("SOAP-ENV:Header");
			root.appendChild(header);

			// body
			Element body = doc.createElement("SOAP-ENV:Body");
			root.appendChild(body);

			String xml = "";

			JSONObject jsoObject = new JSONObject(JSONString);
			xml = xml + XML.toString(jsoObject);
			int Account_ID = jsoObject.getInt("Account_ID");

			xml = (xml + "").replace(" :>", ">").replace("<", "<ws:").replace("<ws:/", "</ws:");
			xml = "<ws:IDQ_Publish>" + xml + "</ws:IDQ_Publish>";
			String tempFile = "Account_id_temp" + Account_ID + ".xml";
			Files.write(Paths.get(tempFile), xml.getBytes(), StandardOpenOption.CREATE);

			InputStream is = new FileInputStream(tempFile);
			Document oldDoc = dBuilder.parse(is);
			Node oldRoot = oldDoc.getDocumentElement();
			body.appendChild(doc.importNode(oldRoot, true));

			String fileName = "Account_id_" + Account_ID + ".xml";
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			DOMSource domSource = new DOMSource(doc);
			StreamResult streamResult = new StreamResult(new File(fileName));
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.transform(domSource, streamResult);
			Files.delete(Paths.get(tempFile));
			System.out.println("Print Done");
		} catch (Exception e) {
			System.out.println(e);
		}
	}

}
