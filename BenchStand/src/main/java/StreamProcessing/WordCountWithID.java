package StreamProcessing;

public class WordCountWithID {
    public int id;
    public String word;
    public int count;

    public WordCountWithID() {}

    public WordCountWithID(int id, String word, int count) {
        this.id = id;
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return "(" + id + ") " + word + ": " + count;
    }
}
