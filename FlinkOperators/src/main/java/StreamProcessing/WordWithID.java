package StreamProcessing;

import java.io.Serializable;

public class WordWithID implements Serializable {
    private static final long serialVersionUID = 1L;
    public int id;
    public String word;

    public WordWithID() {}

    public WordWithID(int id, String word) {
        this.id = id;
        this.word = word;
    }

    @Override
    public String toString() {
        return "(" + id + ") " + word;
    }
}
