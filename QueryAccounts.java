package Maven_test;

import java.io.File;
import java.io.FileInputStream;
import java.sql.*;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.util.ArrayList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

/**
 *
 * @author Vikram
 */
public class QueryAccounts extends Thread {

	String p_sourceSystemName;
	String p_lastExeTime;
	String p_entityName;
	ArrayList<String> p_accountResultList;

	ArrayList<String> GetAccountResultList() {
		return p_accountResultList;
	}

	QueryAccounts(String sourceSystemName, String lastExeTime, String entityName) {
		p_sourceSystemName = sourceSystemName;
		p_lastExeTime = lastExeTime;
		p_entityName = entityName;
	}

	@SuppressWarnings("unused")
	@Override
	public void run() {

		try {

			// YAML Properties:
			ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
			Configuration user = mapper.readValue(new File("application.yaml"), Configuration.class);
			String passwd = user.getPassword();
			String username = user.getUser();
			String url = user.getUrl();
			String driver = user.getDriverclass();

			// JDBC connection:
			Class.forName(driver);
			Connection con = DriverManager.getConnection(url, username, passwd);

			// XML and Mapper Properties:
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = null;
			org.w3c.dom.Document dom = null;
			Object obj = new Object();
			db = dbf.newDocumentBuilder();
			// dom = db.parse(obj.getClass().getResourceAsStream("config.xml"));
			dom = db.parse(new FileInputStream("config.xml"));
			Element docEle = dom.getDocumentElement();
			NodeList sourceSystems_Account = docEle.getElementsByTagName("Account");

			int i;
			for (i = 0; i < sourceSystems_Account.getLength(); i++) {
				Element sourceSystem_acc = (Element) sourceSystems_Account.item(i);

				// SQL_Queries
				String Acc_Query = sourceSystem_acc.getAttribute("SQL_Account");
				String Acc_TimeStamp = sourceSystem_acc.getAttribute("TimeStamp_Account");

				String LastModified = p_lastExeTime;
				Acc_TimeStamp = Acc_TimeStamp.replace("%1", LastModified);

				//Resultset Opertions starts:
				String[] arrEntity = { "Account", "Contact", "Address" };

				for (int k = 0; k < arrEntity.length; k++) {
					int count = 0;

					if (arrEntity[k] == p_entityName) {
						count++;
						// for child entity address
						if (p_entityName == "Address") {
							ResultSet addressSet = null;
							String childContact_query = null;
							String AddressClause = null;
							p_accountResultList = new ArrayList<String>();
							Statement addStat = con.createStatement();
							NodeList add_Entities = docEle.getElementsByTagName("ChildEntity_Address");
							NodeList con_Entities = docEle.getElementsByTagName("ChildEntity_Contact");
							for (int g = 0; g < con_Entities.getLength(); g++) {
								Element childEntity_Contact = (Element) con_Entities.item(g);
								childContact_query = childEntity_Contact.getAttribute("SQL_Contact");
								// System.out.println("Child Contact Query:"+childContact_query);
								break;
							}

							for (int f = 0; f < add_Entities.getLength(); f++) {
								Element childEntity_Address = (Element) add_Entities.item(f);
								String childAddress_query = childEntity_Address.getAttribute("SQL_Address");
								AddressClause = childEntity_Address.getAttribute("AddressIdWhereClause");
								// System.out.println("-----------------------------------");
								// System.out.println("Child Query to get Contact_ID:"+childAddress_query + " "
								// + p_entityName + " " + Acc_TimeStamp);
								// System.out.println("------------------------------------");
								addressSet = addStat
										.executeQuery(childAddress_query + " " + p_entityName + " " + Acc_TimeStamp);
								System.out.println("address set status:" + addressSet.isClosed());
								System.out.println(
										"Query is:" + childAddress_query + " " + p_entityName + " " + Acc_TimeStamp);
								ResultSetMetaData rsmd = addressSet.getMetaData();
								System.out.println("address set status again:" + addressSet.isClosed());
								while (addressSet.isClosed() == false && addressSet.next()) {

									String getAccId = childContact_query + " contact " + AddressClause
											+ addressSet.getInt(1);
									// System.out.println("Query get Account ID: " + getAccId);
									ResultSet ContactAccId = addStat.executeQuery(getAccId);
									while (ContactAccId.next()) {
										p_accountResultList.add(ContactAccId.getString(1));
										// System.out.println("Final AccountID's that got changed from Address: " +
										// p_accountResultList );
									}
									ContactAccId.close();
									// ResultSetMetaData accIdMetaData = ContactAccId.getMetaData();
								}

							}
							addStat.close();
							addressSet.close();

						} else {

							Statement st1 = con.createStatement();
							ResultSet rs1 = st1.executeQuery(Acc_Query + " " + p_entityName + " " + Acc_TimeStamp);
							System.out.println(p_entityName + " set status:" + rs1.isClosed());
							System.out.println("Query is:" + Acc_Query + " " + p_entityName + " " + Acc_TimeStamp);
							ResultSetMetaData rsmd = rs1.getMetaData();
							int columnCount = rsmd.getColumnCount();
							p_accountResultList = new ArrayList<String>(columnCount);
							System.out.println(p_entityName + " set status again:" + rs1.isClosed());
							while (rs1.next()) {
								int j = 1;
								// Understanding the loop
								while (j <= columnCount) {
									p_accountResultList.add(rs1.getString(j++));
								}

							}
							rs1.close();
							con.close();
						}

					}

				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
