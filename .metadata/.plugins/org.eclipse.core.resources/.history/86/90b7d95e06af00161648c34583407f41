import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;


public class CheckPcsThread extends Thread{


	public String pcName;
	private boolean activePc = false;
	static String userName = "mbenseddik@";
	
	public CheckPcsThread(String pcName)
	{
		this.pcName = pcName;
	}

	public void checkPcIsActif() throws IOException
	{
		Vector<String> commands = new Vector<String>();
		commands.add("bash");
		commands.add("-c");
		commands.add("ssh "+ userName + pcName + " \"echo $((1))\"");

		ProcessBuilder p = new ProcessBuilder(commands);
		Process p2 = p.start();

		BufferedReader reader = new BufferedReader(new InputStreamReader(p2.getInputStream()));
		StringBuilder builder = new StringBuilder();
		String line1 = null;
		while ( (line1 = reader.readLine()) != null) {
			builder.append(line1);
		}
		String result = builder.toString();

		if(result.equalsIgnoreCase("1"))
		{
			activePc = true;
		}
	}

	public boolean isActivePc() {
		return activePc;
	}

	public String getPcName() {
		return pcName;
	}


	@Override
	public void run() {
		try {
			checkPcIsActif();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
