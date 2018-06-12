package pl.edu.agh.source;

import java.util.Random;

public class LinearRegressionGenerator implements StreamGenerator {
    private static final String NAME = "LINEAR_REGRESSION";
    private static final Random random = new Random();

    @Override
    public boolean checkName(String name) {
        return name.equals(NAME);
    }

    @Override
    public String generate() {
        return Double.toString(-1000 + 2000 * random.nextDouble());
    }
}
