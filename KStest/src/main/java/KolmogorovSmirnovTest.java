import com.yahoo.sketches.quantiles.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.stream.Stream;

public class KolmogorovSmirnovTest {

    public static void main(String[] args) {
        UpdateDoublesSketch sketch = DoublesSketch.builder().setK(128).build();

        String filePath = "data-examples/standnorm.txt";

        try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
            stream.map(Double::parseDouble)
                  .forEach(sketch::update);
        } catch (IOException e) {
            e.printStackTrace();
        }

        double aproxD = 0;
        DoublesSketchIterator it = sketch.iterator();
        while (it.next()) {
            double x = it.getValue();
            long index = binarySearch(x, sketch);
            double diff = Math.abs((double) index / sketch.getN() - gaussCDF(x));
            aproxD = Math.max(aproxD, diff);
        }

        System.out.println(aproxD * Math.sqrt(sketch.getN()));
    }

    private static long binarySearch(double key, DoublesSketch sketch) {
        long l = 1;
        long r = sketch.getN();

        long m = -1;
        while (l <= r) {
            m = (l + r) >> 1;

            double quantile = sketch.getQuantile((double) m / sketch.getN());

            int comparison = Double.compare(key, quantile);
            if (comparison == 0) {
                return m;
            }
            if (comparison > 0)
                l = m + 1;
            else
                r = m - 1;
        }

        return m;
    }

    // The approximation is accurate to absolute error less than 8 * 10^(-16).
    // return pdf(x) = standard Gaussian pdf
    private static double gaussPDF(double x) {
        return Math.exp(-x * x / 2) / Math.sqrt(2 * Math.PI);
    }

    // return pdf(x, mu, signma) = Gaussian pdf with mean mu and stddev sigma
    private static double gaussPDF(double x, double mu, double sigma) {
        return gaussPDF((x - mu) / sigma) / sigma;
    }

    // return cdf(z) = standard Gaussian cdf using Taylor approximation
    private static double gaussCDF(double z) {
        if (z < -8.0) return 0.0;
        if (z > 8.0) return 1.0;
        double sum = 0.0, term = z;
        for (int i = 3; sum + term != sum; i += 2) {
            sum = sum + term;
            term = term * z * z / i;
        }
        return 0.5 + sum * gaussPDF(z);
    }

    // return cdf(z, mu, sigma) = Gaussian cdf with mean mu and stddev sigma
    private static double gaussCDF(double z, double mu, double sigma) {
        return gaussCDF((z - mu) / sigma);
    }
}
