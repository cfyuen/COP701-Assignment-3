package key;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserGenre implements WritableComparable<UserGenre> {

	private String user;
	private String genre;
	
	public UserGenre() {
	}
	
	public UserGenre(String user, String genre) {
		this.user = user;
		this.genre = genre;
	}
	
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	
	public String getGenre() {
		return genre;
	}
	public void setGenre(String genre) {
		this.genre = genre;
	}
	
	public void readFields(DataInput in) throws IOException {
		this.user = in.readUTF();
		this.genre = in.readUTF();
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.user);
		out.writeUTF(this.genre);
	}
	
	public int compareTo(UserGenre other) {
		if (this.user.compareTo(other.user) != 0) return this.user.compareTo(other.user);
		return this.genre.compareTo(other.genre);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
        int result = 1;
        result = prime * result
                + ((this.user == null) ? 0 : this.user.hashCode());
        result = prime * result
                + ((this.genre == null) ? 0 : this.genre.hashCode());
        return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (!(obj instanceof UserGenre)) return false;
		UserGenre other = (UserGenre)obj;
		if (this.user.equals(other.user) && this.genre.equals(other.genre)) return true;
		return false;
	}
	
	public String toString() {
		return "[" + this.user + "," + this.genre + "]";
	}
		
}
