package common;

import java.util.Comparator;

public class Problem implements Comparator<Problem>, Comparable<Problem> {
	private String name;
	private Integer rating;
	private Integer solvedCount;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public Integer getRating() {
		return rating;
	}
	public void setRating(Integer rating) {
		this.rating = rating;
	}
	
	public Integer getSolvedCount() {
		return solvedCount;
	}
	public void setSolvedCount(Integer solvedCount) {
		this.solvedCount = solvedCount;
	}
	
	public int compareTo(Problem o) {
		if (this.rating == o.rating) return 0;
		if (this.rating < o.rating) return -1;
		return 1;
	}
	public int compare(Problem o1, Problem o2) {
		if (o1.rating == o2.rating) return 0;
		if (o1.rating < o2.rating) return -1;
		return 1;
	}
	
	public String toString() {
		return this.name + "\t" + String.valueOf(this.rating) + "\t" + String.valueOf(this.solvedCount);
	}
}
