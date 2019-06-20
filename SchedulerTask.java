package Maven_test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.stream.Stream;
import Maven_test.AttributeMapper;

/**
 * @author Vikram
 *
 */

public class SchedulerTask {

	AttributeMapper MQuery; // Object created for class Multiple Query
	String p_sourceSystemName;

	public SchedulerTask(String sourceSystemName) {
		p_sourceSystemName = sourceSystemName;
	}

	public void run() throws InterruptedException, IOException {

		MQuery = new AttributeMapper(); // Created a object of Multiple Query class so

		File fh = new File("MyLog.txt");
		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.ms");
		String lastExeTime = sdf.format(fh.lastModified());
		System.out.println("LastModified " + lastExeTime);

		Date date = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.ms");
		String currentExec = formatter.format(date);
		System.out.println("Date Format with yyyy-MM-dd HH:mm:ss.ms : " + currentExec);

		FileWriter fileWriter = new FileWriter("MyLog.txt");
		fileWriter.write(currentExec);
		fileWriter.close();

		QueryAccounts acc_QueryAccount = new QueryAccounts(p_sourceSystemName, lastExeTime, "Account");
		QueryAccounts con_QueryAccount = new QueryAccounts(p_sourceSystemName, lastExeTime, "Contact");
		QueryAccounts add_QueryAccount = new QueryAccounts(p_sourceSystemName, lastExeTime, "Address");

		acc_QueryAccount.run();
		con_QueryAccount.run();
		add_QueryAccount.run();

		acc_QueryAccount.join();
		con_QueryAccount.join();
		add_QueryAccount.join();

		ArrayList<String> IDsFromAccount = acc_QueryAccount.GetAccountResultList();
		ArrayList<String> IDsFromContact = con_QueryAccount.GetAccountResultList();
		ArrayList<String> IDsFromAddress = add_QueryAccount.GetAccountResultList();

		// System.out.println("addressList : " + addressResultList);
		ArrayList<String> finalResultList = new ArrayList<String>();
		finalResultList.addAll(IDsFromAccount);
		finalResultList.addAll(IDsFromContact);
		finalResultList.addAll(IDsFromAddress);
		System.out.println("Final List" + finalResultList);

		// System.out.println("FinalList : " + FinalResultList);
		HashSet<String> set = new HashSet<String>(finalResultList);

		System.out.println("Final Set" + set);

		AttributeMapper obj = new AttributeMapper();

		Stream<String> stream = set.parallelStream();
		stream.forEach((accId) -> {
			obj.PrepareCanonicalJSON(p_sourceSystemName, accId);
		});

	}
}
