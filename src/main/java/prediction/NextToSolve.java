package prediction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;

import common.Problem;
import common.Utils;

public class NextToSolve {

	private static String userGenreDir = "user_rating/user.genre.rating.txt";
	private static String solvedCountDir = "solved_count/solved.count.txt";
	private static String problemGenreDir = "problem/problem.genre.txt";
	private static String problemRatingDir = "problem/problem.rating.txt";
	
	private static String handle = "";
	private static String genre = "";
	
	private static Integer readUserGenre() throws IOException {
		System.out.println("Reading User Genre");
		String str = "";
		
		FileReader userGenreFileReader = new FileReader(userGenreDir);
		BufferedReader br = new BufferedReader(userGenreFileReader);
		while ((str = br.readLine())!=null && str.length()!=0) {
			String[] tokens = str.split("\t");
			String[] kgtokens = tokens[0].split(",");
			String key = kgtokens[0].substring(1);
			String usergenre = kgtokens[1].substring(0, kgtokens[1].length()-1);
			Integer rating = Integer.valueOf(tokens[1]);
			if (key.equals(handle) && usergenre.equals(genre)) {
				br.close();
				return rating;
			}
		}
		br.close();
		return 0;
	}
	
	private static HashSet<String> readProblemGenre() throws IOException {
		System.out.println("Reading Problem Genre");
		String str = "";
		
		FileReader fileReader = new FileReader(problemGenreDir);
		BufferedReader br = new BufferedReader(fileReader);
		HashSet<String> problems = new HashSet<String>();
		while ((str = br.readLine())!=null && str.length()!=0) {
			String[] tokens = str.split("\t");
			String name = tokens[0];
			if (tokens.length > 1) {
				String[] tags = tokens[1].split(",");
				for (String tag : tags) {
					if (tag.equals(genre)) {
						problems.add(name);
					}
				}
			}
		}
		br.close();
		return problems;
	}
	

	private static HashSet<String> removeSolvedProblem(HashSet<String> problems) throws JsonProcessingException, IOException {
		System.out.println("Removing User Solved Problem");
		String str = "";
		
		FileReader fileReader = new FileReader("data/" + handle + ".json");
		BufferedReader br = new BufferedReader(fileReader);
		while ((str = br.readLine())!=null && str.length()!=0) {
			String json = str.toString();
			ObjectMapper mapper = new ObjectMapper();
			JsonNode node = mapper.readTree(json);
			ArrayNode result = (ArrayNode)node.get("result");
			
			for (JsonNode sub : result) {
				
				if (Utils.filterProblem(sub)) {
					String problemName = sub.get("problem").get("name").getTextValue();
					problems.remove(problemName);
				}
			}
		}
		br.close();
		return problems;
		
	}

	
	private static HashMap<String,Integer> readProblemRating(HashSet<String> problems) throws IOException {
		System.out.println("Reading Problem Rating");
		String str = "";
		
		FileReader fileReader = new FileReader(problemRatingDir);
		BufferedReader br = new BufferedReader(fileReader);
		HashMap<String,Integer> todoMap = new HashMap<String,Integer>();
		while ((str = br.readLine())!=null && str.length()!=0) {
			String[] tokens = str.split("\t");
			String name = tokens[0];
			Integer rating = Integer.valueOf(tokens[1]);
			if (problems.contains(name)) {
				todoMap.put(name, rating);
			}
		}
		br.close();
		return todoMap;
	}
	
	private static ArrayList<Problem> readProblemSolvedCount(HashMap<String,Integer> todoMap) throws IOException {
		System.out.println("Reading Problem Solved Count");
		String str = "";
		
		FileReader fileReader = new FileReader(solvedCountDir);
		BufferedReader br = new BufferedReader(fileReader);
		ArrayList<Problem> proList = new ArrayList<Problem>();
		while ((str = br.readLine())!=null && str.length()!=0) {
			String[] tokens = str.split("\t");
			String name = tokens[0];
			Integer solvedCount = Integer.valueOf(tokens[1]);
			if (todoMap.containsKey(name)) {
				Problem pro = new Problem();
				pro.setName(name);
				pro.setRating(todoMap.get(name));
				pro.setSolvedCount(solvedCount);
				proList.add(pro);
			}
		}
		br.close();
		return proList;
		
	}
	
	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("Usage: java -jar NextToSolve.jar <handle> <genre>");
			return;
		}
		handle = args[0];
		genre = args[1];
		
		try {
			Integer rating = readUserGenre();
			System.out.println("User rating: " + rating);
			HashSet<String> problems = readProblemGenre();
			problems = removeSolvedProblem(problems);
			System.out.println("Problems Found: " + problems.size());
			HashMap<String,Integer> todoMap = readProblemRating(problems);
			ArrayList<Problem> todoList = readProblemSolvedCount(todoMap);
			
			Collections.sort(todoList);
			
			int level = 0;
			Problem bestProblem = null;
			for (Problem p : todoList) {
				if (p.getRating() <= rating) continue;
				if (p.getRating() > rating + level * 50 && p.getRating() <= rating + (level+1) * 50) {
					if (bestProblem == null || bestProblem.getSolvedCount() < p.getSolvedCount()) {
						bestProblem = p;
					}
				}
				else {
					if (bestProblem != null) {
						System.out.println(bestProblem);
					}
					bestProblem = null;
					level += 1;
				}
				if (level == 5) break;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
