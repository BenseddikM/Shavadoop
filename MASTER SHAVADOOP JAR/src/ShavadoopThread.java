import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Vector;

public class ShavadoopThread extends Thread implements Runnable{
	String directoryPath = "/cal/homes/bargaoui/shavadoopFiles/";
	String pathSlaveJar = directoryPath + "SLAVESHAVADOOP.jar";
	String userSession = "ssh bargaoui@";
	String namePc;
	String pathSx;
	String pathsUmx = "";
	String pathSmx;
	String mode;
	String wordToReduce;
	ArrayList<String> SlaveOutputStream;

	public ArrayList<String> getSlaveOutputStream() {
		return SlaveOutputStream;
	}

	// First constructor for Sx Mode
	public ShavadoopThread(String namePc, String Sx,String mode){
		this.namePc = namePc;
		SlaveOutputStream = new ArrayList<String>();
		pathSx = " " + directoryPath + Sx;
		this.mode = mode;
	}

	// Second constructor for Umx Mode
	public ShavadoopThread(String namePc, String wordToReduce, HashSet<String> listOfUmx , String pathSmx,String mode){
		this.namePc = namePc;
		this.wordToReduce = wordToReduce;
		this.pathSmx = pathSmx;
		SlaveOutputStream = new ArrayList<String>();
		for(String umxs : listOfUmx)
		{
			pathsUmx += " " + directoryPath + umxs;
		}
		this.mode = mode;
	}

	public String getOutputStreamThread(String command)
	{
		Vector<String> commandBash = new Vector<String>();
		commandBash.add("bash");
		commandBash.add("-c");
		commandBash.add(command);

		ProcessBuilder processbuilder = new ProcessBuilder(commandBash);
		Process process = null;

		try {
			process = processbuilder.start();
		} catch (IOException e) {
			e.printStackTrace();
		}

		BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

		StringBuilder builder = new StringBuilder();
		String line = null;
		try {
			while ( (line = reader.readLine()) != null) {
				builder.append(line + "\n");
				SlaveOutputStream.add(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		String contentFile = builder.toString();
		return contentFile;
	}
	
	// doc
	public String buildCommandForThread()
	{
		String command = "";
		if(mode.equals("SxUMx"))
		{
			command = userSession + namePc + " \"java -jar " + pathSlaveJar + " " + mode +  pathSx + "\"";
		}
		if(mode.equals("UMxSMx"))
		{
			command = userSession + namePc + " \"java -jar " 
					+ pathSlaveJar + " " + mode + " " + wordToReduce + " " + pathSmx + " " +  pathsUmx + "\"";
		}
		
		return command;
	}

	// The run function for the thread
	public void run(){
		String command = buildCommandForThread();
		String contentFile = getOutputStreamThread(command);
		System.out.println(contentFile);
	}
}
