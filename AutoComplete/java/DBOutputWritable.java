import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBOutputWritable implements DBWritable{

    /*
    starting_phrase: the possible user input, length depends on NGram model
    following_word: the next word that comes after the starting_phrase
    count: the frequency of this combination
     */
    private String starting_phrase;
    private String following_word;
    private int count;

    // setup the private variables
    public DBOutputWritable(String starting_phrase, String following_word, int count) {
        this.starting_phrase = starting_phrase;
        this.following_word = following_word;
        this.count= count;
    }
    // read the input
    public void readFields(ResultSet arg0) throws SQLException {
        this.starting_phrase = arg0.getString(1);
        this.following_word = arg0.getString(2);
        this.count = arg0.getInt(3);
    }

    // write out the private variables
    public void write(PreparedStatement arg0) throws SQLException {
        arg0.setString(1, starting_phrase);
        arg0.setString(2, following_word);
        arg0.setInt(3, count);
    }

}
