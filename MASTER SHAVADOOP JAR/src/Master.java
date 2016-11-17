import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Vector;


public class Master {
	static String finalFile = "";
	static String directory = "/cal/homes/mbenseddik/shavadoopFiles/";
	static String pcsFilePath = "listOfPcs.txt";
	static String pathJar = directory + "/SLAVESHAVADOOP.jar";
	static String userName = "mbenseddik@";

	public static void deleteOldFiles()
	{
		File file = new File(directory);      
		String[] myFiles;    
		if(file.isDirectory()){
			myFiles = file.list();
			for (int i=0; i<myFiles.length; i++) {
				File myFile = new File(file, myFiles[i]);
				if(!myFiles[i].equals("SLAVESHAVADOOP.jar") && !myFiles[i].equals("motsIgnores.txt"))
					myFile.delete();
			}
		}
	}

	public static File getFileInput(String[] args)
	{
		System.out.println("Loding Input File...");
		displayStars();
		File fileInput = null;
		if (args.length == 0) {
			System.out.println("No arguments were given! Give a file input.txt in arguments.");
			System.exit(0);
		}
		else
		{
			fileInput = new File(args[0]);
		}
		return fileInput;
	}

	public static ArrayList<String> splitSx(String[] args)
	{
		ArrayList<String> listOfSx = new ArrayList<String>();

		File fileInput = getFileInput(args);
		if(fileInput.exists()){
			System.out.println("Load input file : Success!");
			displayStars();
			try (BufferedReader br = new BufferedReader(new FileReader(fileInput))) {
				int index = 1;
				System.out.println("Spliting to Sx files...");
				displayStars();
				String line = br.readLine();
				while (line != null) {
					if(!line.isEmpty())
					{
						System.out.println(line);
						try (Writer writer = new BufferedWriter(new OutputStreamWriter(
								new FileOutputStream(directory + "S"+ index + ".txt"), "utf-8"))) {
							writer.write(line);
							listOfSx.add("S"+index+".txt");
						}
						index++;
					}
					line = br.readLine();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else
		{
			System.out.println("Load input file : Failed!");
		}

		System.out.println("\nList of Sx files :");
		displayStars();
		for(int i = 0; i< listOfSx.size(); i++)
		{
			System.out.println(listOfSx.get(i));
		}

		return listOfSx;
	}

	public static ArrayList<String> getListOfActivePcs(String listOfPcsFile) throws IOException
	{
		Vector<String> allPcs = new Vector<>();
		ArrayList<String> listOfActivePcs = new ArrayList<String>();

		FileReader fileReader = new FileReader(listOfPcsFile);
		BufferedReader br = new BufferedReader(fileReader);
		String line;
		while( (line = br.readLine()) != null)
		{
			allPcs.add(line);
		}

		br.close();

		System.out.println("\nChecking The connexion on all the pcs...");
		displayStars();

		System.out.print("[");
		for(int i=0;i < allPcs.size(); i++)
		{
			Vector<String> commands = new Vector<String>();
			commands.add("bash");
			commands.add("-c");
			commands.add("ssh "+ userName + allPcs.get(i) + " \"echo $((1))\"");
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
				listOfActivePcs.add(allPcs.get(i));
			}
			System.out.print( "==");
		}

		System.out.print( "]");
		System.out.println("\nChecking the pcs Done! The list of pcs :");
		displayStars();
		for(String pc : listOfActivePcs)
		{
			System.out.println(pc + " : OK !");
		}

		return listOfActivePcs;
	}

	public static HashMap<String,ArrayList<String>> splitOnUmx(ArrayList<String> listOfActivePcs,ArrayList<String> SxDictionnary) throws InterruptedException
	{
		ArrayList<ShavadoopThread> threads = new ArrayList<ShavadoopThread>();
		HashMap<String,ArrayList<String>> UmxDictionnary = new HashMap<String, ArrayList<String>>();
		int i;
		int index = 0;

		System.out.println("\nSpliting on Umx files...");
		displayStars();

		for (i = 0; i< SxDictionnary.size(); i++)
		{
			String pcToRunOn = listOfActivePcs.get(index);
			String SxToRun = SxDictionnary.get(i);
			System.out.println("On lance Slave sur le pc : " + pcToRunOn + " pour split le fichier : " + SxToRun);
			threads.add(new ShavadoopThread(pcToRunOn,SxToRun,"SxUMx"));
			threads.get(i).start();

			index = (index +1) % listOfActivePcs.size();
		}

		index = 0;
		for (i = 0; i< SxDictionnary.size(); i++)
		{
			int a = i+1;
			String pcToRunOn = listOfActivePcs.get(index);
			String SxToRun = SxDictionnary.get(i);
			threads.get(i).join();
			UmxDictionnary.put("Um"+ a +".txt", threads.get(i).getSlaveOutputStream());
			System.out.println("Traitement sur le pc : " + pcToRunOn + " pour le fichier : " + SxToRun + " Fini !");
			index = (index +1) % listOfActivePcs.size();
		}

		System.out.println("\nSpliting on Umx Done!");
		displayStars();

		return UmxDictionnary;
	}

	public static HashMap<String,String> splitOnSmXAndRmx(HashMap<String, HashSet<String>> wordsDictionnary, ArrayList<String> listOfActivePcs) throws InterruptedException
	{
		HashMap<String, String> RmxDictionnary = new HashMap<String, String>();
		ArrayList<ShavadoopThread> threads = new ArrayList<ShavadoopThread>();
		int i = 0;
		int index = 0;

		System.out.println("\nSpliting on Smx And doing the Rmx part...");
		displayStars();
		for (Entry<String, HashSet<String>> entry : wordsDictionnary.entrySet())
		{
			String pcToRunOn = listOfActivePcs.get(index);
			String word = entry.getKey();
			HashSet<String> listUmx = entry.getValue();
			int a = i + 1;
			String pathSmx = directory + "Sm" + a + ".txt";

			System.out.println("On lance Slave sur le pc : " + pcToRunOn + " pour avoir le reducedFile : " + pathSmx);
			threads.add(new ShavadoopThread(pcToRunOn,word,listUmx,pathSmx,"UMxSMx"));
			threads.get(i).start();
			i++;
			index = (index +1) % listOfActivePcs.size();
		}

		index = 0;
		for (i = 0;i< threads.size(); i++)
		{
			int a = i+1;
			String pcToRunOn = listOfActivePcs.get(index);
			threads.get(i).join();
			if(!threads.get(i).getSlaveOutputStream().isEmpty())
				finalFile += threads.get(i).getSlaveOutputStream().get(0) + "\n";
			RmxDictionnary.put("Rm" + a + ".txt", pcToRunOn);
			index = (index +1) % listOfActivePcs.size();
		}

		System.out.println("\nSmx and Rmx parts Done!");
		displayStars();
		System.out.println("\nAll Done!");

		return RmxDictionnary;
	}


	public static HashMap<String,HashSet<String>> runOnSlaves(ArrayList<String> SxDictionnary) throws IOException, InterruptedException
	{
		HashMap<String,ArrayList<String>> UmxDictionnary = new HashMap<String,ArrayList<String>>();

		ArrayList<String> listOfActivePcs = getListOfActivePcs(pcsFilePath);
		UmxDictionnary = splitOnUmx(listOfActivePcs,SxDictionnary);
		HashMap<String,HashSet<String>> wordsDictionnary = convertUmxDictToWordsDict(UmxDictionnary);
		System.out.println("Words-Umx Dictionnary :");
		System.out.println(Arrays.deepToString(wordsDictionnary.entrySet().toArray()));
		splitOnSmXAndRmx(wordsDictionnary, listOfActivePcs);

		return wordsDictionnary;

	}



	public static HashMap<String,HashSet<String>> convertUmxDictToWordsDict(HashMap<String,ArrayList<String>> UmxDictionnary)
	{
		HashMap<String,HashSet<String>> wordsDictionnary = new HashMap<String, HashSet<String>>();
		for (String key: UmxDictionnary.keySet()) {
			ArrayList<String> wordsOfUm = UmxDictionnary.get(key);
			for (int i = 0; i< wordsOfUm.size(); i++)
			{
				String word = wordsOfUm.get(i);
				if (!wordsDictionnary.containsKey(word))
				{   
					HashSet<String> tmp = new HashSet<String>();
					tmp.add(key);
					wordsDictionnary.put(word, tmp);
				}
				else
				{
					wordsDictionnary.get(word).add(key);
				}
			}
		}

		return wordsDictionnary;
	}

	public static void saveFinalFile() throws UnsupportedEncodingException, FileNotFoundException, IOException
	{
		try (Writer writer = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(directory + "reducedFile.txt"), "utf-8"))) {
			writer.write(finalFile);
		}
	}

	public static void displayStars()
	{
		System.out.println("**********************************************************************");
		System.out.println("**********************************************************************\n\n");
	}

	public static void welcome()
	{
		System.out.println("********************************************************************");
		System.out.println("*        𝕎𝕖𝕝𝕔𝕠𝕞𝕖 𝕥𝕠 𝕊𝕙𝕒𝕧𝕒𝕕𝕠𝕠𝕡 - 𝕎𝕠𝕣𝕕ℂ𝕠𝕦𝕟𝕥 𝕄𝕒𝕡ℝ𝕖𝕕𝕦𝕔𝕖                                       *");
		System.out.println("* 		 ᴡᴇʟᴄᴏᴍᴇ ᴛᴏ ꜱʜᴀᴠᴀᴅᴏᴏᴘ - ᴡᴏʀᴅᴄᴏᴜɴᴛ ᴍᴀᴘʀᴇᴅᴜᴄᴇ		      *");
		System.out.println("*        Wₑₗcₒₘₑ ₜₒ ₛₕₐᵥₐdₒₒₚ ₋ WₒᵣdCₒᵤₙₜ ₘₐₚᵣₑdᵤcₑ		   *");
		System.out.println("*Ⓦⓔⓛⓒⓞⓜⓔ ⓣⓞ Ⓢⓗⓐⓥⓐⓓⓞⓞⓟ     -⃝ ⓌⓞⓡⓓⒸⓞⓤⓝⓣ ⓂⓐⓟⓇⓔⓓⓤⓒⓔ*");
		System.out.println("*        Wêl¢ðmê †ð §håvåÐððþ - WðrÐÇðµñ† MåþRêÐµ¢ê                *");
		System.out.println("********************************************************************");
	}

	public static void main(String[] args) throws IOException, InterruptedException {

		welcome();
		deleteOldFiles();
		displayStars();
		displayStars();

		long startTime = System.nanoTime();
		ArrayList<String> SxDictionnary = splitSx(args);
		runOnSlaves(SxDictionnary);
		saveFinalFile();
		long endTime = System.nanoTime();
		long duration = (endTime - startTime) / 1000000000;
		System.out.println("Duration : " + duration + "seconds !");

	}
}
