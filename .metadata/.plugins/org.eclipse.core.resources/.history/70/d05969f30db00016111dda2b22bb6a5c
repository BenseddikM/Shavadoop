package slave;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Benseddik Mohammed
 * @author Sami Bergaoui
 * @version 1.0.1
 * 
 */
public class Slave {

	static String directoryPath = "/cal/homes/bargaoui/shavadoopFiles/";
	static String delimitersFilePath = directoryPath + "motsIgnores.txt";
	static String extensionFiles = ".txt";

	/**
	 * @return delimiters : An ArrayList of delimiters we ignore in the file
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static ArrayList<String> getDelimiters() throws FileNotFoundException, IOException
	{
		ArrayList<String> delimiters = new ArrayList<String>();
		File delimitersFile = new File(delimitersFilePath);
		if(delimitersFile.exists()){
			try (BufferedReader br = new BufferedReader(new FileReader(delimitersFile))) {
				String line;
				while ((line = br.readLine()) != null) {
					delimiters.add(line);
				}
			}
		}	
		return delimiters;
	}

	/**
	 * Refactores the word in the parameter : removes special characters from the word
	 * @param word before refactoring
	 * @return word refactored
	 */
	public static String refactor(String word) 
	{
		String pattern = "(\\w+)(\\.|,|;|:|!)";
		Pattern r = Pattern.compile(pattern);
		Matcher m = r.matcher(word);
		if (m.find( )) {
			word =  word.replaceAll(word, m.group(1));
		}

		return word;
	}

	/**
	 * gets the file index from the fileName
	 * @param args
	 * @return index of the current File
	 */
	public static int getFileIndex(String args)
	{
		Path p = Paths.get(args);
		String file = p.getFileName().toString();
		int index = Integer.parseInt(file.replaceAll("[\\D]", ""));
		return index;
	}

	/**
	 * Saves the generic file (Umx, Smx or Rmx) depending on the fileCategory and the index
	 * @param content is the content we want to save in the file
	 * @param fileCategory : Umx, Smx or Rmx
	 * @param index : index of file
	 * @throws IOException
	 */
	public static void saveFile(String content, String fileCategory, int index) throws IOException
	{
		String fileName = "";

		if(fileCategory.equals("Umx"))
			fileName += "Um" + index;
		if(fileCategory.equals("Smx"))
			fileName += "Sm" + index;
		if(fileCategory.equals("Rmx"))
			fileName += "Rm" + index;

		File fileToSave =  new File(directoryPath + fileName + extensionFiles);
		BufferedWriter bw = new BufferedWriter(new FileWriter(fileToSave));
		bw.write(content);
		bw.close();
	}

	/**
	 * Method that does the conversion from Sx files to Umx files
	 * @param SxfileName
	 * @param delimiters
	 * @return umxContent
	 */
	public static String mapSxToUmx(String SxfileName, ArrayList<String> delimiters)
	{
		String umxContent = "";
		HashSet<String> keys = new HashSet<String>();
		File SxFile = new File(SxfileName);

		if(SxFile.exists()){
			try (BufferedReader br = new BufferedReader(new FileReader(SxFile))) {
				String line = br.readLine();
				while (line != null) {
					if(!line.isEmpty())
					{
						line = line.toLowerCase();
						String[] words = line.split(" ");
						for (String word : words)
						{
							word = refactor(word);
							if(!delimiters.contains(word))
							{
								umxContent += word + " " + 1 + "\n";
								keys.add(word);
							}
						}
						line = br.readLine();
					}
				}
				for (String key : keys) {
					System.out.println(key);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else
		{
			System.out.println("Load input file : Failed!");
		}
		return umxContent;
	}

	/**
	 * Method that does the Shuffling part : conversion from the Umx files to Smx files
	 * @param wordToSearch
	 * @param UmxFiles
	 * @return SmxContent
	 */
	public static String mapUmxToSmx(String wordToSearch, String[] UmxFiles)
	{
		String SmxContent = "";
		for (int i = 0; i< UmxFiles.length; i++)
		{
			File newFile = new File(UmxFiles[i]);
			if(newFile.exists()){
				try (BufferedReader br = new BufferedReader(new FileReader(newFile))) {
					String line;
					while ((line = br.readLine()) != null) {
						String[] wordAndOccurence = line.split(" ");
						String word = wordAndOccurence[0];
						if(word.equals(wordToSearch))
						{
							int occurence = Integer.parseInt(wordAndOccurence[1]);
							SmxContent += word + " " + occurence + "\n";
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			else
			{
				System.out.println("Load input file : Failed!");
			}
		}
		return SmxContent;
	}

	/**
	 * Method that does the reducing part : from Smx files to Rmx files
	 * @param SmxFileName
	 * @return RmxContent
	 */
	public static String mapSmxToRmx(String SmxFileName)
	{
		String RmxContent = "";
		File SmxFile = new File(SmxFileName);
		String word = null;
		int reducedOccurence = 0;

		if(SmxFile.exists()){
			try (BufferedReader br = new BufferedReader(new FileReader(SmxFile))) {
				String line;
				while ((line = br.readLine()) != null) {
					String[] wordAndOccurence = line.split(" ");
					word = wordAndOccurence[0].toLowerCase();
					reducedOccurence += (int) Integer.parseInt(wordAndOccurence[1]);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

			RmxContent += word + " " + reducedOccurence;
		}
		else
		{
			System.out.println("Load input file : Failed!");
		}

		return RmxContent;
	}

	/**
	 * Method that reads the Rmx files and prints it in the outputStream of the slave
	 * @param RmxFilePath
	 */
	public static void readRmxFile(String RmxFilePath)
	{
		File RmxFile = new File(RmxFilePath);
		if(RmxFile.exists()){
			try (BufferedReader br = new BufferedReader(new FileReader(RmxFile))) {
				String line;
				while ((line = br.readLine()) != null) {
					System.out.println(line);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Main function to execute on the slave
	 * @param args
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void main(String[] args) throws FileNotFoundException, IOException{
		String mode = args[0];
		String word;
		String pathRmx = "";
		ArrayList<String> delimiters = getDelimiters();
		if(!(mode.equals("SxUMx") || mode.equals("UMxSMx")))
		{
			System.out.println("First argument of the slave is missing or incorrect !");
		}
		else
		{
			if(mode.equals("SxUMx"))
			{
				String SxFileName = args[1];
				int index = getFileIndex(SxFileName);
				String UmxContent = mapSxToUmx(SxFileName,delimiters);
				saveFile(UmxContent,"Umx", index);
			}
			if(mode.equals("UMxSMx"))
			{
				word = args[1];
				String SmxFile = args[2];
				int index = getFileIndex(SmxFile);
				String[] files = new String[args.length -3];

				for (int i = 0; i<files.length;i++)
				{
					files[i] = args[i+3];
				}

				String maps = mapUmxToSmx(word,files);
				saveFile(maps,"Smx", index);

				String reducedMap = mapSmxToRmx(SmxFile);
				saveFile(reducedMap,"Rmx", index);

				pathRmx = directoryPath + "Rm" + index + extensionFiles;
				readRmxFile(pathRmx);

			}
		}
	}
}
