import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Vector;


/**
 * @author Benseddik Mohammed
 * @author Sami Bergaoui
 * @version 1.0.1
 * 
 */
public class Master {
	static String finalFile = "";
	static String directory = "/cal/homes/mbenseddik/shavadoopFiles/";
	static String pcsFilePath = "listOfPcs2.txt";
	static String extensionFiles = ".txt";
	static String finalFileName = "reducedFile.txt";
	static long timeout = 2000L;
	static long millisecondsUnit = 1000000000;
	static int splitSxPortion = 100;


	/**
	 * This method checks the ssh connexion to all the pcs whitin the Telecom ParisTech's computer park.
	 * from the file named *pcsFilePath*, we send an ssh request "echo $((1))" for each computer in the
	 * room, and we update our Dictionary of active pcs for each computer that sent an answer.
	 * As we can see for the latest version, we use also threads to have a better execution timer for
	 * this part.
	 * @param listOfPcsFile is the name of file which contains the list of pcs to consider as slaves
	 * @return listOfActivePcs is the Dictionary of active pcs within the room
	 * @throws IOException  if we can't reach or open the listOfPcsFile
	 * @throws InterruptedException as for the interrupted threads that search for active pcs
	 */
	public static ArrayList<String> 
	getListOfActivePcs(String listOfPcsFile) throws IOException, InterruptedException
	{
		// allPcs is the vector of all pcs in the room, active or down
		ArrayList<CheckPcsThread> pcThreads = new ArrayList<CheckPcsThread>();
		// We initialize here our threads Dictionary for each pc 
		Vector<String> allPcs = new Vector<>();
		// Our Dictionary of active pcs only
		ArrayList<String> listOfActivePcs = new ArrayList<String>();

		// **This part of code is here just to get the execution time
		long startTime = System.nanoTime();

		// We initialize here a FileReader and a BufferedReader so that we can browse the pcsFile line
		// by line, and add each line of the file (as an actual pc) to the allPcs Dictionary
		FileReader fileReader = new FileReader(listOfPcsFile);
		BufferedReader br = new BufferedReader(fileReader);
		String line;
		while( (line = br.readLine()) != null)
		{
			allPcs.add(line);
		}

		br.close();

		System.out.println("\nChecking The connexion on all the pcs...");
		ShavadoopUtils.displayStars();
		System.out.print("[");

		// We check here the true pcs that are on (or connected to the network). We initialize a
		// Dictionary of CheckPcsThread, each thread checks if the pc sends an answer to the ssh request
		// or not
		for(int i=0;i < allPcs.size(); i++)
		{
			CheckPcsThread threadOfWord = new CheckPcsThread(allPcs.get(i));
			pcThreads.add(threadOfWord);
			pcThreads.get(i).start();
			System.out.print( "==");
		}

		// Joining the threads here and adding the actual active pc to the listOfActivePcs Dictionary
		for(int i=0;i < pcThreads.size(); i++)
		{
			pcThreads.get(i).join();
			if(pcThreads.get(i).isActivePc() == true)
			{
				listOfActivePcs.add(pcThreads.get(i).getPcName());
			}
		}

		System.out.print( "]");
		System.out.println("\nChecking the pcs Done! The list of pcs :");
		for(String pc : listOfActivePcs)
		{
			System.out.println(pc + " : OK !");
		}

		// **This part of code is here just to get the execution time
		long endTime = System.nanoTime();
		long duration = (endTime - startTime) / millisecondsUnit;
		System.out.println("Duration of SEARCH FOR ACTVE PCS : " + duration + "seconds !");

		return listOfActivePcs;
	}

	/**
	 * This method splits all the Sx files to UMx files using threads on slaves. Sx files are created
	 * by the master directly by spliting the original file by lines or block of lines (see the splitSx
	 * an the splitSxBlock methods in the ShavadoopUtils class for more details).
	 * The purpose here to get the "Map" part of the MapReduce pattern. For each word in the Sxfiles,
	 * we put an occurence of "1" in the corresponding Umx File. The reducing part is done later on
	 * the UmxtoSmx part.
	 * @param listOfActivePcs is the dictionnay of active pcs on the room, we get if from 
	 * the getListOfActivePcs() method
	 * @param SxDictionary is the dictionnay of all the Sx files we get from the SplitSx methods
	 * @return UmxDictionnay which is a Dictionary of (keys = UmxFiles) and (Entries = words whithin
	 * the corresponding UmxFile)
	 * @throws InterruptedException if we get an interrupted thread on the join of threads of slaves
	 */
	public static HashMap<String,ArrayList<String>> 
	splitOnUmx(ArrayList<String> listOfActivePcs,ArrayList<String> SxDictionnary) throws InterruptedException
	{
		// The initialization part. We have a Dictionary of slave threads named simply "threads"
		ArrayList<ShavadoopThread> threads = new ArrayList<ShavadoopThread>();
		// The Umx Dictionary that contains each Umx file and the corresponding words in the file
		HashMap<String,ArrayList<String>> UmxDictionnary = new HashMap<String, ArrayList<String>>();

		int i;
		// indexOfActivePcs is to an index to give each pc an amount of slave threads to run. We use
		// here a "modulo" way for the problem, we mean by that : we loop on the SxFile, whenever we
		// looped on the whole list of pcs, we return to the begining of the pcs list and attribute
		// some more threads for each pc.
		// This method is a bit wacky, and should be replaced in the future by a better system to manage
		// the threads and activePcs.
		int indexOfActivePcs = 0;
		// indexOfFile is an index to name the Umx File (Um + indexOfFile)
		int indexOfFile = 0;

		// **This part of code is here just to get the execution time
		long startTime = System.nanoTime();

		// Some display tricks here..
		System.out.println("\nSpliting on Umx files...");
		ShavadoopUtils.displayStars();

		threads = new ArrayList<ShavadoopThread>();
		// Loop on the SxDictionary
		for (i = 0; i< SxDictionnary.size(); i++)
		{
			String pcToRunOn = listOfActivePcs.get(indexOfActivePcs);
			String SxToRun = SxDictionnary.get(i);
			System.out.println("On lance Slave sur le pc : " + pcToRunOn +
					" pour split le fichier : " + SxToRun);
			// Adding the new thread to the thread Dictionary..
			threads.add(new ShavadoopThread(pcToRunOn,SxToRun,"SxUMx"));
			// ..and starting the thread here
			threads.get(i).start();

			// Updating the indexOfActivePcs by the "modulo" system
			indexOfActivePcs = (indexOfActivePcs +1) % listOfActivePcs.size();
		}

		// Re initializing the indexOfActivePcs for the "join" loop
		indexOfActivePcs = 0;

		for (i = 0; i< SxDictionnary.size(); i++)
		{
			indexOfFile = i+1;
			// The pc we want our thread to wait on for the other threads to be complete
			String pcToRunOn = listOfActivePcs.get(indexOfActivePcs);
			// The Sx file we are currently threating
			String SxToRun = SxDictionnary.get(i);

			// We tell our threads here to wait for the other threads to finish the work, and
			// we set a "timeout" initialized as a static variable, so that we can kill the dead
			// or blocked threads, and restart them later on
			threads.get(i).join(timeout);

			// Checking if the thread is running even after the timeout limit..
			if(threads.get(i).isAlive())
			{
				System.out.println("Thread mort : " + threads.get(i).getName());
				// .. and killing it afterwards
				threads.get(i).interrupt();
			}
			else
			{
				// if the thread finished its work succesfully, we finish the process by creating
				// the Umx file and updating the UmxDictionary, the content of the Umx file is
				// sent by the slave in the outputStream, and we can get it by the getSlaveOutputStream
				// method that we created in the slave
				UmxDictionnary.put("Um"+ indexOfFile +extensionFiles, threads.get(i).getSlaveOutputStream());
				System.out.println("Traitement sur le pc : " + pcToRunOn +
						" pour le fichier : " + SxToRun + " Fini !");

				// Updating the indexOfActivePcs by the "modulo" system
				indexOfActivePcs = (indexOfActivePcs +1) % listOfActivePcs.size();
			}

		}

		// @Todo : restart dead threads..
		// ! we should implement this part later on another version.. !

		System.out.println("\nSpliting on Umx Done!");

		// **This part of code is here just to get the execution time
		long endTime = System.nanoTime();
		long duration = (endTime - startTime) / millisecondsUnit;
		System.out.println("Duration of SPLIT ON UMX : " + duration + "seconds !");
		ShavadoopUtils.displayStars();

		return UmxDictionnary;
	}

	/**
	 * This method does the "reducing" part on the MapReduce algorithm. We take all our Umx Files that
	 * we created on the "Map" phase, and we shuffle each word on the Umx files to regroup them on the
	 * Smx files. The Smx files contains only one word repeated as many time as it appears in the Umx
	 * file with its occurence being conserved.
	 * @param wordsDictionary is the Dictionary of words and their respective Umx file where they
	 * appear. We get this Dictionary by converting the UmxDictionary obtained in the Map phase
	 * and using the function convertUmxDictToWordsDict on the ShavadoopUtils Class
	 * @param listOfActivePcs as seen before..
	 * @return RmxDictionary is the Dictionary of RmxFiles, after the shuffle and reducing phase
	 * @throws InterruptedException is the slave threads are interrupted
	 */
	public static HashMap<String,String> 
	splitOnSmXAndRmx(HashMap<String, HashSet<String>> wordsDictionnary, ArrayList<String> listOfActivePcs) 
			throws InterruptedException
	{	
		HashMap<String, String> RmxDictionnary = new HashMap<String, String>();
		ArrayList<ShavadoopThread> threads = new ArrayList<ShavadoopThread>();
		ArrayList<ShavadoopThread> deadThreads = new ArrayList<ShavadoopThread>();

		// **This part of code is here just to get the execution time
		long startTime = System.nanoTime();

		int i = 0;
		// indexOfActivePcs is to an index to give each pc an amount of slave threads to run. We use
		// here a "modulo" way for the problem, we mean by that : we loop on the SxFile, whenever we
		// looped on the whole list of pcs, we return to the begining of the pcs list and attribute
		// some more threads for each pc.
		// This method is a bit wacky, and should be replaced in the future by a better system to manage
		// the threads and activePcs.
		int indexOfActivePcs = 0;
		// indexOfFile is an index to name the Smx File (Sm + indexOfFile) and Rmx File
		// (Rm + indexOfFile)
		int indexOfFile = 0;

		System.out.println("\nSpliting on Smx And doing the Rmx part...");
		ShavadoopUtils.displayStars();

		// We loop on the words of the file, create the corresponding thread and update the
		// thread dictionary
		for (Entry<String, HashSet<String>> entry : wordsDictionnary.entrySet())
		{
			String pcToRunOn = listOfActivePcs.get(indexOfActivePcs);
			String word = entry.getKey();
			HashSet<String> listUmx = entry.getValue();
			indexOfFile = i + 1;
			String pathSmx = directory + "Sm" + indexOfFile + extensionFiles;

			System.out.println("On lance Slave sur le pc : " + pcToRunOn +
					" pour avoir le reducedFile : " + pathSmx);
			// Adding the new thread to the thread Dictionary..
			threads.add(new ShavadoopThread(pcToRunOn,word,listUmx,pathSmx,"UMxSMx"));
			// ..and starting the thread here
			threads.get(i).start();
			i++;
			indexOfActivePcs = (indexOfActivePcs +1) % listOfActivePcs.size();
		}

		indexOfActivePcs = 0;

		// Looping on the threads dictionary here
		for (i = 0;i< threads.size(); i++)
		{
			int a = i+1;
			String pcToRunOn = listOfActivePcs.get(indexOfActivePcs);
			// Same timeout than the Umx Split part
			threads.get(i).join(timeout);
			// And checking if the thread is still running to kill it
			if(threads.get(i).isAlive())
			{
				System.out.println("Thread mort : " + threads.get(i).getName());
				threads.get(i).interrupt();
				deadThreads.add(threads.get(i));
			}
			// Successfull threads
			if(!threads.get(i).getSlaveOutputStream().isEmpty())
				finalFile += threads.get(i).getSlaveOutputStream().get(0) + "\n";
			RmxDictionnary.put("Rm" + a + extensionFiles, pcToRunOn);
			indexOfActivePcs = (indexOfActivePcs +1) % listOfActivePcs.size();
		}

		System.out.println("\nSmx and Rmx parts Done!");

		// **This part of code is here just to get the execution time
		long endTime = System.nanoTime();
		long duration = (endTime - startTime) / millisecondsUnit;

		System.out.println("Duration of SPLIT ON SMX AND RMX : " + duration + "seconds !");
		ShavadoopUtils.displayStars();
		System.out.println("\nAll Done!");

		return RmxDictionnary;
	}

	/**
	 * This function runs all the precedent methods.
	 * It is launched on the main part for the master
	 * @param SxDictionary is the list of Sx Files after the initial split
	 * @return wordsDictionnary which is the final reduced dictionary
	 * @throws IOException in case we can't load the Input file
	 * @throws InterruptedException in case the threads we start are interrupted
	 */
	public static HashMap<String,HashSet<String>> 
	runOnSlaves(ArrayList<String> SxDictionnary) throws IOException, InterruptedException
	{
		HashMap<String,ArrayList<String>> UmxDictionary = new HashMap<String,ArrayList<String>>();

		// get the list of active pcs first
		ArrayList<String> listOfActivePcs = getListOfActivePcs(pcsFilePath);
		// and then the Map part by spliting on Umx files
		UmxDictionary = splitOnUmx(listOfActivePcs,SxDictionnary);

		// we get then the wordsDictionnary
		HashMap<String,HashSet<String>> wordsDictionary = 
				ShavadoopUtils.convertUmxDictToWordsDict(UmxDictionary);
		System.out.println("Words-Umx Dictionary :");
		System.out.println(Arrays.deepToString(wordsDictionary.entrySet().toArray()));

		// and finally we do the Reducing part
		splitOnSmXAndRmx(wordsDictionary, listOfActivePcs);

		return wordsDictionary;
	}

	/**
	 * Main Master function
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, InterruptedException {

		// Some display tricks here..
		ShavadoopUtils.welcome();
		ShavadoopUtils.deleteOldFiles();
		ShavadoopUtils.displayStars();
		ShavadoopUtils.displayStars();

		// **This part of code is here just to get the execution time
		long startTime = System.nanoTime();

		// Initial Split on Sx files
		ArrayList<String> SxDictionary = ShavadoopUtils.splitSxBloc(args, splitSxPortion);
		// Doing the mapReduce algorithm on the slave threads here..
		runOnSlaves(SxDictionary);
		// and saving the final wordCount file
		ShavadoopUtils.saveFinalFile();

		// **This part of code is here just to get the execution time
		long endTime = System.nanoTime();
		long duration = (endTime - startTime) / millisecondsUnit;
		System.out.println("Total Duration : " + duration + "seconds !");

	}
}
