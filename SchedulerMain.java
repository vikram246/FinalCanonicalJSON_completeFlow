package Maven_test;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class SchedulerMain {
	public static void main(String args[]) throws InterruptedException, IOException {
		// String Source_System;
		@SuppressWarnings("resource")

		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		Configuration user = mapper.readValue(new File("application.yaml"), Configuration.class);

		String Source_System = user.getSourcesystem_name();
		run(Source_System);

		return;
	}

	public static void run(String Source_System) throws InterruptedException, IOException {

		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		Configuration user = mapper.readValue(new File("application.yaml"), Configuration.class);

		int timer = user.getScheduler_Timer();

		for (;;) {
			System.out.println("Execution in Main Thread....");
			Thread.sleep((timer));

			SchedulerTask st = new SchedulerTask(Source_System); // Instantiate ScheduledTask class
			st.run();

		}
	}
}
