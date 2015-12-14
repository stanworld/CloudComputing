import java.lang.reflect.Array;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.io.FileReader;
import java.io.*;
public class MP1 {
    Random generator;
    String userName;
    String inputFileName;
    String delimiters = " \t,;.?!-:@[](){}_*/";
    String[] stopWordsArray = {"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
            "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its",
            "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that",
            "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having",
            "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while",
            "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before",
            "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again",
            "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
            "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than",
            "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"};

    void initialRandomGenerator(String seed) throws NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("SHA");
        messageDigest.update(seed.toLowerCase().trim().getBytes());
        byte[] seedMD5 = messageDigest.digest();

        long longSeed = 0;
        for (int i = 0; i < seedMD5.length; i++) {
            longSeed += ((long) seedMD5[i] & 0xffL) << (8 * i);
        }

        this.generator = new Random(longSeed);
    }

    Integer[] getIndexes() throws NoSuchAlgorithmException {
        Integer n = 10000;
        Integer number_of_lines = 50000;
        Integer[] ret = new Integer[n];
        this.initialRandomGenerator(this.userName);
        for (int i = 0; i < n; i++) {
            ret[i] = generator.nextInt(number_of_lines);
        }
        return ret;
    }

    public MP1(String userName, String inputFileName) {
        this.userName = userName;
        this.inputFileName = inputFileName;
    }

    public String[] process() throws Exception {
        String[] ret = new String[20];
		 //TODO
		Map<String,Integer> results= new HashMap<String,Integer>();
		List ignoredWords = Arrays.asList(stopWordsArray);
        Integer[] indexes = getIndexes();
		int lineNumber=0;
		try {
                    BufferedReader br = new BufferedReader(new FileReader(inputFileName));
                    String line;
                    while ((line = br.readLine()) != null) {
		             boolean isIn=false;
				   for(int i=0;i<indexes.length;i++){
					   if(indexes[i]==lineNumber)
						   isIn=true;
				   }
				   if(isIn){
					   StringTokenizer multiTokenizer = new StringTokenizer(line, delimiters);
					   while (multiTokenizer.hasMoreTokens()){
                      
						  String temp=multiTokenizer.nextToken();
						  String target=temp.toLowerCase().trim();
                                                  if(!ignoredWords.contains(target)){
						    if(results.containsKey(target)){
                                                    
                                                      results.put(target, results.get(target)+1);
                                                    }
                                                    else{
                                                      results.put(target, 1);
                                                    }
                                                  }
                                           }
					   
					   
					   
				   }
				lineNumber++;
                    }
               } catch(Exception e){
                    
               }
                
           ValueComparator bvc = new ValueComparator(results);
           TreeMap<String, Integer> sorted_results = new TreeMap(bvc);
           sorted_results.putAll(results);
           int i=0;
           for (Map.Entry<String, Integer> entry : sorted_results.entrySet()) {    
                        if(i<20){
                       //     System.out.println("Key : " + entry.getKey() 
                        //              + " Value : " + entry.getValue());
                            ret[i]=entry.getKey();
                        }
                        else
                            break;
                        i++;
	    }
       
        return ret;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1){
            System.out.println("MP1 <User ID>");
        }
        else {
            String userName = args[0];
            String inputFileName = "./input.txt";
            MP1 mp = new MP1(userName, inputFileName);
            String[] topItems = mp.process();
            for (String item: topItems){
                System.out.println(item);
            }
        }
    }
}
class ValueComparator implements Comparator {
    Map<String,Integer> base1;

    public ValueComparator(Map base) {
        this.base1 = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with
    // equals.
    public int compare(Object a, Object b) {
        String a1=(String)a;
        String b1=(String)b;
        if (base1.get(a1) > base1.get(b1)) {
            return -1;
        } else if (base1.get(a1)== base1.get(b1) && a1.compareTo(b1)<0){
            return -1;
        }
        return 1;
      
    }
    
}