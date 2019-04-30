package StreamProcessing;

public class PiCalculator {
    private static final int SCALE = 10000;
    private static final int ARRINIT = 2000;

    public static void calculateDigits(int digits) {
        int[] arr = new int[digits + 1];
        int carry = 0;

        for (int i = 0; i <= digits; ++i)
            arr[i] = ARRINIT;

        for (int i = digits; i > 0; i-= 14) {
            int sum = 0;
            for (int j = i; j > 0; --j) {
                sum = sum * j + SCALE * arr[j];
                arr[j] = sum % (j * 2 - 1);
                sum /= j * 2 - 1;
            }

            carry = sum % SCALE;
        }
    }
}
