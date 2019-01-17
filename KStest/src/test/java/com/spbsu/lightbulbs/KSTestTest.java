package com.spbsu.lightbulbs;

import org.junit.Test;

import static org.junit.Assert.*;

import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class KSTestTest {
    private static final int BIG_LIMIT = 100000;
    private static final int LIMIT = 10000;
    private static final int SMALL_LIMIT = 1000;

    private static final int BIG_K = 512;
    private static final int SMALL_K = 256;

    @Test
    public void stdNormAndStdNorm2SampleTest() {
        DoublesSketch sketch1 = createSketch("stdnorm-first.txt", BIG_LIMIT, SMALL_K);
        DoublesSketch sketch2 = createSketch("stdnorm-second.txt", BIG_LIMIT, SMALL_K);
        assertTrue(KSTest.twoSampleTest(sketch1, sketch2) < 2);
    }

    @Test
    public void stdNormAndStdNorm1SampleTest() {
        DoublesSketch sketch = createSketch("stdnorm-first.txt", BIG_LIMIT, SMALL_K);
        assertTrue(KSTest.oneSampleTest(sketch, KSTestTest::gaussCDF) < 2);
    }

    @Test
    public void stdNormAndNorm2SampleTest() {
        DoublesSketch sketch1 = createSketch("stdnorm-first.txt", BIG_LIMIT, SMALL_K);
        DoublesSketch sketch2 = createSketch("norm5-10.txt", BIG_LIMIT, SMALL_K);
        assertTrue(KSTest.twoSampleTest(sketch1, sketch2) > 10);
    }

    @Test
    public void stdNormAndNorm1SampleTest() {
        DoublesSketch sketch = createSketch("norm5-10.txt", BIG_LIMIT, SMALL_K);
        assertTrue(KSTest.oneSampleTest(sketch, KSTestTest::gaussCDF) > 10);
    }

    @Test
    public void stdNormAndExp2SampleTest() {
        DoublesSketch sketch1 = createSketch("stdnorm-first.txt", BIG_LIMIT, SMALL_K);
        DoublesSketch sketch2 = createSketch("exp.txt", BIG_LIMIT, SMALL_K);
        assertTrue(KSTest.twoSampleTest(sketch1, sketch2) > 10);
    }

    @Test
    public void stdNormAndExp1SampleTest() {
        DoublesSketch sketch = createSketch("exp.txt", BIG_LIMIT, SMALL_K);
        assertTrue(KSTest.oneSampleTest(sketch, KSTestTest::gaussCDF) > 10);
    }

    @Test
    public void stdNormAndStdNormWithUniform2SampleTest() {
        DoublesSketch sketch1 = createSketch("stdnorm-first.txt", BIG_LIMIT, SMALL_K);
        DoublesSketch sketch2 = createSketch("stdnorm-and-uniform.txt", BIG_LIMIT, SMALL_K);
        assertTrue(KSTest.twoSampleTest(sketch1, sketch2) > 10);
    }

    @Test
    public void stdNormAndStdNormWithUniform1SampleTest() {
        DoublesSketch sketch = createSketch("stdnorm-and-uniform.txt", BIG_LIMIT, SMALL_K);
        assertTrue(KSTest.oneSampleTest(sketch, KSTestTest::gaussCDF) > 10);
    }

    @Test
    public void stdNormAndUniform2SampleTest() {
        DoublesSketch sketch1 = createSketch("stdnorm-first.txt", BIG_LIMIT, SMALL_K);
        DoublesSketch sketch2 = createSketch("uniform200-350.txt", BIG_LIMIT, SMALL_K);
        assertTrue(KSTest.twoSampleTest(sketch1, sketch2) > 10);
    }

    @Test
    public void stdNormAndUniform1SampleTest() {
        DoublesSketch sketch = createSketch("uniform200-350.txt", BIG_LIMIT, SMALL_K);
        assertTrue(KSTest.oneSampleTest(sketch, KSTestTest::gaussCDF) > 10);
    }

    @Test
    public void Poisson10AndPoisson10_2SampleTest() {
        DoublesSketch sketch1 = createSketch("poisson10-first.txt", BIG_LIMIT, SMALL_K);
        DoublesSketch sketch2 = createSketch("poisson10-second.txt", BIG_LIMIT, SMALL_K);
        assertTrue(KSTest.twoSampleTest(sketch1, sketch2) < 2);
    }

    @Test
    public void Poisson10AndPoisson4_2SampleTest() {
        DoublesSketch sketch1 = createSketch("poisson10-first.txt", BIG_LIMIT, SMALL_K);
        DoublesSketch sketch2 = createSketch("poisson4.txt", BIG_LIMIT, SMALL_K);
        assertTrue(KSTest.twoSampleTest(sketch1, sketch2) > 10);
    }

    private UpdateDoublesSketch createSketch(String file, int limit, int K) {
        UpdateDoublesSketch sketch = DoublesSketch.builder().setK(K).build();

        String filePath = "test-data/" + file;

        try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
            stream.map(Double::parseDouble)
                  .limit(limit)
                  .forEach(sketch::update);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return sketch;
    }

    // The approximation is accurate to absolute error less than 8 * 10^(-16).
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

    // return pdf(x) = standard Gaussian pdf
    private static double gaussPDF(double x) {
        return Math.exp(-x * x / 2) / Math.sqrt(2 * Math.PI);
    }
}