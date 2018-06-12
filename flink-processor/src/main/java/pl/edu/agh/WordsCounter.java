package pl.edu.agh;


public class WordsCounter implements Algorithm {
    private static final String NAME = "WORD_COUNT";
    private long wordsCounter = 0;

    @Override
    public boolean checkName(String name) {
        return name.equals(NAME);
    }

    @Override
    public String map(String value) throws Exception {
        long wordsAmount = value.split(" ").length;
        wordsCounter += wordsAmount;
        String result = "Words in message: " + wordsAmount + ", in total: " + wordsCounter;
        return result;
    }
}
