package pl.edu.agh.sparkprocessor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;

import java.util.Arrays;
import java.util.List;

public class LinearRegression implements Algorithm {
    private static final String NAME = "LINEAR_REGRESSION";

    private final int MAX_N = 100000;
    private final double[] x = new double[MAX_N];
    private final double[] y = new double[MAX_N];
    private final boolean verbose = true;

    LinearRegression() {
        for (int i = 0; i < MAX_N; i++) {
            x[i] = i;
        }
    }

    @Override
    public boolean checkName(String name) {
        return name.equals(NAME);
    }

    @Override
    public void run(String broker,
                    String resultTopic,
                    JavaInputDStream<ConsumerRecord<String, String>> messages,
                    Broadcast<KafkaSink> kafkaSink
    ) {

        final JavaDStream<String> numberCounts = messages
                .map(ConsumerRecord::value).flatMap(x -> Arrays.asList(x).iterator());

        numberCounts.foreachRDD(rdd -> {
            final List<String> numbers = rdd.collect();
            for (String number : numbers) {
                this.addNum(Double.parseDouble(number));
                final String resultMessage = this.regression();

                kafkaSink.getValue().send(resultTopic, resultMessage);
            }
        });
    }

    private String regression() {
        int n = 0;

        double sumx = 0.0, sumy = 0.0, sumx2 = 0.0;
        for (int i = 0; i < MAX_N; i++) {
            sumx += x[n];
            sumx2 += x[n] * x[n];
            sumy += y[n];
            n++;
        }
        double xbar = sumx / n;
        double ybar = sumy / n;

        double xxbar = 0.0, yybar = 0.0, xybar = 0.0;
        for (int i = 0; i < n; i++) {
            xxbar += (x[i] - xbar) * (x[i] - xbar);
            yybar += (y[i] - ybar) * (y[i] - ybar);
            xybar += (x[i] - xbar) * (y[i] - ybar);
        }
        double beta1 = xybar / xxbar;
        double beta0 = ybar - beta1 * xbar;

        String res = "y   = " + beta1 + " * x + " + beta0;

        if (verbose) {
            // analyze results
            int df = n - 2;
            double rss = 0.0;      // residual sum of squares
            double ssr = 0.0;      // regression sum of squares
            for (int i = 0; i < n; i++) {
                double fit = beta1 * x[i] + beta0;
                rss += (fit - y[i]) * (fit - y[i]);
                ssr += (fit - ybar) * (fit - ybar);
            }
            double R2 = ssr / yybar;
            double svar = rss / df;
            double svar1 = svar / xxbar;
            double svar0 = svar / n + xbar * xbar * svar1;
            double svar0_ = svar * sumx2 / (n * xxbar);

            System.out.println("y   = " + beta1 + " * x + " + beta0);

            System.out.println("R^2                 = " + R2);
            System.out.println("std error of beta_1 = " + Math.sqrt(svar1));
            System.out.println("std error of beta_0 = " + Math.sqrt(svar0));
            System.out.println("std error of beta_0 = " + Math.sqrt(svar0_));

            System.out.println("SSTO = " + yybar);
            System.out.println("SSE  = " + rss);
            System.out.println("SSR  = " + ssr);
        }
        return res;
    }

    private void addNum(double d) {
        System.arraycopy(y, 0, y, 1, MAX_N - 1);
        y[0] = d;
    }
}
