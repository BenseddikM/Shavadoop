import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Vector;


public class checkPcsThread extends Thread{


	public String pcName;
	private boolean activePc = false;
	static String userName = "mbenseddik@";
	private java.util.List<TaskListener> listeners = Collections.synchronizedList(new ArrayList<TaskListener>());
	
	public checkPcsThread(String pcName)
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

	public void addListener(TaskListener listener)
	{
		listeners.add(listener);
	}

	public void removeListener(TaskListener listener)
	{
		listeners.remove(listener);
	}

	private final void notifyListeners()
	{
		synchronized (listeners) {
			for(TaskListener listener : listeners){
				listener.threadComplete(this);
			}
		}
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			checkPcIsActif();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		notifyListeners();
		for(int i = 0; i< listeners.size(); i++)
		{
			System.out.println(listeners.get(i));
		}
	}

}
