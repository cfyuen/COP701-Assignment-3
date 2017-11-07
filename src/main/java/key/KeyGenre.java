package key;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class KeyGenre implements WritableComparable<KeyGenre> {

	private String key;
	private String genre;
	
	public KeyGenre() {
	}
	
	public KeyGenre(String str) {
		String[] tokens = str.split(",");
		this.key = tokens[0].substring(1);
		this.genre = tokens[1].substring(0, tokens[1].length()-1);
	}
	
	public KeyGenre(String key, String genre) {
		this.key = key;
		this.genre = genre;
	}
	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	
	public String getGenre() {
		return genre;
	}
	public void setGenre(String genre) {
		this.genre = genre;
	}
	
	public void readFields(DataInput in) throws IOException {
		this.key = in.readUTF();
		this.genre = in.readUTF();
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.key);
		out.writeUTF(this.genre);
	}
	
	public int compareTo(KeyGenre other) {
		if (this.key.compareTo(other.key) != 0) return this.key.compareTo(other.key);
		return this.genre.compareTo(other.genre);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
        int result = 1;
        result = prime * result
                + ((this.key == null) ? 0 : this.key.hashCode());
        result = prime * result
                + ((this.genre == null) ? 0 : this.genre.hashCode());
        return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (!(obj instanceof KeyGenre)) return false;
		KeyGenre other = (KeyGenre)obj;
		if (this.key.equals(other.key) && this.genre.equals(other.genre)) return true;
		return false;
	}
	
	public String toString() {
		return "[" + this.key + "," + this.genre + "]";
	}
		
}
