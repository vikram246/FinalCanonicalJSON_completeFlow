package Maven_test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.sql.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.json.simple.JSONArray;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

/**
 * @author Vikram
 *
 */

public class AttributeMapper extends KafkaProduer {

	static KafkaProduer client;

	static long start = System.currentTimeMillis();

	public void PrepareCanonicalJSON(String sourceSystemName, Object accId) {
		// Loading from XML
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = null;

		org.w3c.dom.Document dom = null;
		try {
			db = dbf.newDocumentBuilder();

			// dom = db.parse(this.getClass().getResourceAsStream("config.xml"));
			dom = db.parse(new FileInputStream("config.xml"));
		} catch (Exception ex) {
			String error = ex.getMessage();
			System.out.println("The error is " + error);
		}

		try {

			// YAML Properties:
			ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
			Configuration user = mapper.readValue(new File("application.yaml"), Configuration.class);
			String passwd = user.getPassword();
			String username = user.getUser();
			String url = user.getUrl();
			String driver = user.getDriverclass();
			String topicName = user.getTopicName();
			String key = user.getKey();
			// String value = "";
			String bootstarp = user.getBootstrap();
			String host = user.getHost();
			String keyserial = user.getKeySerial();
			String valueserial = user.getValueSerial();
			String keyserializer = user.getKeyserializer();
			String valueserializer = user.getValueserializer();

			String acc_query = "";
			String con_query = "";
			String add_query = "";
			String acc_WhereClause = "";
			String con_WhereClause = "";
			String add_WhereClause = "";

			Map<String, String> acc_fields = new HashMap<String, String>();
			Map<String, String> con_fields = new HashMap<String, String>();
			Map<String, String> add_fields = new HashMap<String, String>();

			Element docEle = dom.getDocumentElement();

			// get a nodelist of elements
			NodeList sourceSystems = docEle.getElementsByTagName("SourceSystem");
			int i;

			for (i = 0; i < sourceSystems.getLength(); i++) {
				Element sourceSystem = (Element) sourceSystems.item(i);
				String systemName = sourceSystem.getAttribute("Name");
				if (systemName.equals(sourceSystemName)) {
					NodeList acc_Entities = sourceSystem.getElementsByTagName("Account");
					Element acc_Entity = null;

					for (int l = 0; l < acc_Entities.getLength(); l++) {
						acc_Entity = (Element) acc_Entities.item(l);
						acc_query = acc_Entity.getAttribute("SQL");
						acc_WhereClause = acc_Entity.getAttribute("AccountIdWhereClause");

						NodeList attributes = acc_Entity.getChildNodes();
						for (int k = 0; k < attributes.getLength(); k++) {

							if (attributes.item(k).getNodeType() != Node.ELEMENT_NODE) {
								continue;
							}
							Element attribute = (Element) attributes.item(k);
							if (false == attribute.getNodeName().equals("Attribute")) {
								break;
							}
							String sourceName = attribute.getAttribute("SourceName");
							String targetName = attribute.getAttribute("TargetName");
							acc_fields.put(sourceName, targetName);

						}

						// For Contact
						NodeList con_Entities = acc_Entity.getElementsByTagName("ChildEntity_Contact");
						Element con_Entity = null;

						for (int j = 0; j < con_Entities.getLength(); j++) {
							con_Entity = (Element) con_Entities.item(j);
							con_query = con_Entity.getAttribute("SQL");
							con_WhereClause = con_Entity.getAttribute("ContactIdWhereClause");

							attributes = con_Entity.getElementsByTagName("Attribute");
							for (int k = 0; k < attributes.getLength(); k++) {
								if (attributes.item(k).getNodeType() != Node.ELEMENT_NODE) {
									continue;
								}
								Element attribute = (Element) attributes.item(k);
								if (false == attribute.getNodeName().equals("Attribute")) {
									break;
								}
								String sourceName = attribute.getAttribute("SourceName");
								String targetName = attribute.getAttribute("TargetName");
								con_fields.put(sourceName, targetName);
							}
						}

						// for Address
						NodeList add_Entities = acc_Entity.getElementsByTagName("ChildEntity_Address");
						Element add_Entity = null;

						for (int j = 0; j < add_Entities.getLength(); j++) {
							add_Entity = (Element) add_Entities.item(j);
							add_query = add_Entity.getAttribute("SQL");
							add_WhereClause = add_Entity.getAttribute("AddressIdWhereClause");

							attributes = add_Entity.getElementsByTagName("Attribute");
							for (int k = 0; k < attributes.getLength(); k++) {
								if (attributes.item(k).getNodeType() != Node.ELEMENT_NODE) {
									continue;
								}
								Element attribute = (Element) attributes.item(k);
								if (false == attribute.getNodeName().equals("Attribute")) {
									break;
								}
								String sourceName = attribute.getAttribute("SourceName");
								String targetName = attribute.getAttribute("TargetName");
								add_fields.put(sourceName, targetName);
							}

						} // for j add_Entities

						// System.out.println("test");
					} // for j acc_Entities
				} // if sysname=MySQL

			} // for i sourceSystems
			Class.forName(driver);
			Connection con = DriverManager.getConnection(url, username, passwd);

			Statement st1 = con.createStatement();

			ResultSet rs1 = st1.executeQuery(acc_query + acc_WhereClause + accId);
			System.out.println("AM Accun Query :" + acc_query + acc_WhereClause + accId);
			JSONObject json_account = new JSONObject();
			ResultSetMetaData rsmd = rs1.getMetaData();
			JSONArray array_contact = new JSONArray();
			JSONArray array_address = new JSONArray();

			// for Account
			while (rs1.next()) {
				int numColumns = rsmd.getColumnCount();
				for (int z = 1; z <= numColumns; z++) {
					String column_name = rsmd.getColumnName(z);
					json_account.put(acc_fields.get(column_name), rs1.getObject(column_name));

				}

				Statement st2 = con.createStatement();
				ResultSet rs2 = st2.executeQuery(con_query + con_WhereClause + rs1.getInt("accountid"));
				Statement st3 = con.createStatement();

				ResultSetMetaData rsmd1 = rs2.getMetaData();
				array_contact = new JSONArray();
				// for contact
				while (rs2.next()) {

					numColumns = rsmd1.getColumnCount();
					JSONObject json_contact = new JSONObject();
					for (int z = 1; z <= numColumns; z++) {
						String column_name = rsmd1.getColumnName(z);
						json_contact.put(con_fields.get(column_name), rs2.getObject(column_name));

					}
					array_contact.add(json_contact);

					// for address
					ResultSet rs3 = st3.executeQuery(add_query + add_WhereClause + rs2.getInt("contactid"));
					ResultSetMetaData rsmd2 = rs3.getMetaData();
					array_address = new JSONArray();
					while (rs3.next()) {
						int numColumns1 = rsmd2.getColumnCount();
						JSONObject json_address = new JSONObject();
						for (int a = 1; a <= numColumns1; a++) {
							String colum_name = rsmd2.getColumnName(a);
							json_address.put(add_fields.get(colum_name), rs3.getObject(colum_name));
						}
						array_address.add(json_address);
					}
					json_contact.put("Address :", array_address);
				}

				json_account.put("Contact :", array_contact);
				rs2.close();

			} // while rs1
			rs1.close();
			con.close();
			System.out.println(json_account.toString());

			String value = json_account.toString();
			FileWriter fw = new FileWriter("JsonOutput.txt");
			fw.write(value);
			fw.close();

			// Kafka Producer:
			Properties props = new Properties();
			props.put(bootstarp, host);
			props.put(keyserial, keyserializer);
			props.put(valueserial, valueserializer);

			Producer<String, String> producer = new KafkaProducer<String, String>(props);
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);
			producer.send(record);
			producer.close();
			System.out.println("############### Canonical JSON pushed to topic successfully ################");

			// create XML

			//XMLCreator createXML = new XMLCreator();
			//createXML.createXML(value, accId);

		}

		catch (Exception e) {
			e.printStackTrace();

		}

	}

}
