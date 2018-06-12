package pl.edu.agh.source;

import java.util.Random;
import java.util.stream.Collectors;

public class WordCountGenerator implements StreamGenerator {
    private static final String NAME = "WORD_COUNT";
    private static final Random random = new Random();

    @Override
    public boolean checkName(String name) {
        return name.equals(NAME);
    }

    @Override
    public String generate() {
        return random
                .ints(random.nextInt(1000), 'a', 'z' + 1)
                .mapToObj(x -> String.valueOf((char) x))
                .collect(Collectors.joining(" "));
    }
}
