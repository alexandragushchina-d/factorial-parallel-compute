package pgdp.threads;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class Factorial {
  private static final int threadMaxCount =
    Runtime.getRuntime().availableProcessors();

  public static BigInteger facSequential(int n) {
    BigInteger res = BigInteger.ONE;
    if (n < 0) {
      throw new IllegalArgumentException();
    }
    if (n == 0) {
      return BigInteger.ONE;
    }
    for (int i = 2; i <= n; i++) {
      res = res.multiply(BigInteger.valueOf(i));
    }
    return res;
  }

  public static BigInteger facParallel(int n, int threadCount) throws InterruptedException {
    if (n < 0 || threadCount <= 0) {
      throw new IllegalArgumentException();
    }
    if (n == 0 || n == 1) {
      return BigInteger.ONE;
    }
    if (threadCount > n / 4) {
      threadCount = Math.max(n / 4, 1);
    }
    if (threadCount == 1) {
      return Factorial.facSequential(n);
    }
    threadCount = Math.min(threadCount, threadMaxCount);

    List<Thread> threads = new ArrayList<Thread>();
    BigInteger[] results = new BigInteger[threadCount];
    for (int i = 2, step = n / threadCount + 1, threadIndex = 0;
         i <= n; i += step, threadIndex++) {
      Thread thread = new Thread() {
        private int index;
        private int first;
        private int last;

        Thread setIndexAndInterval(int index, int first, int last) {
          this.index = index;
          this.first = first;
          this.last = last;
          return this;
        }

        @Override
        public void run() {
          BigInteger threadResult = BigInteger.ONE;
          for (int j = first; j <= last; j++) {
            threadResult = threadResult.multiply(BigInteger.valueOf(j));
          }
          results[index] = threadResult;
        }
      }.setIndexAndInterval(threadIndex, i, Math.min(i + step - 1, n));
      threads.add(thread);
      thread.start();
    }

    for (int i = 0; i < threads.size(); i++) {
      if (threads.get(i).isAlive()) {
        threads.get(i).join();
      }
    }

    BigInteger result = BigInteger.ONE;
    for (int i = 0; i < results.length; i++) {
      if (results[i] == BigInteger.ZERO) {
        continue;
      }

      result = result.multiply(results[i]);
    }

    return result;
  }

  static void RegularTest(String testName, int N, int threadCount) {
    try {
      long time = System.currentTimeMillis();
      BigInteger expected = Factorial.facSequential(N);
      long seqTime = System.currentTimeMillis() - time;
      time = System.currentTimeMillis();
      BigInteger actual = Factorial.facParallel(N, threadCount);
      long parTime = System.currentTimeMillis() - time;
      System.out.printf(
        "%s (n:%s, threads:%s): %s == %s \n=> %s\n", testName, N, threadCount,
        actual, expected, actual.equals(expected));
      System.out.println(testName + " sequential time (ms): " + seqTime);
      System.out.println(testName + " parallel time (ms): " + parTime);
    } catch (Exception ex) {
      System.out.printf(
        "%s: %s == %s \n=> %s\n", testName, ex.getMessage(), "", false);
    }
    System.out.println();
  }

  static void Test01() {
    int N = 5;
    int threadCount = 2;
    RegularTest("Test01", N, threadCount);
  }

  static void Test02() {
    int N = 10;
    int threadCount = 2;
    RegularTest("Test02", N, threadCount);
  }

  static void Test03() {
    int N = 106;
    int threadCount = 2;
    RegularTest("Test03", N, threadCount);
  }

  static void Test04() {
    int N = 1060;
    int threadCount = 2;
    RegularTest("Test04", N, threadCount);
  }

  static void Test05_01() {
    int N = 40000;
    int threadCount = 2;
    RegularTest("Test05_01", N, threadCount);
  }

  static void Test05_02() {
    int N = 40000;
    int threadCount = 4;
    RegularTest("Test05_02", N, threadCount);
  }

  static void Test05_03() {
    int N = 40000;
    int threadCount = 8;
    RegularTest("Test05_03", N, threadCount);
  }

  static void Test06_01() {
    int N = 100000;
    int threadCount = 2;
    RegularTest("Test06_01", N, threadCount);
  }

  static void Test06_02() {
    int N = 100000;
    int threadCount = 4;
    RegularTest("Test06_02", N, threadCount);
  }

  static void Test06_03() {
    int N = 100000;
    int threadCount = 400;
    RegularTest("Test06_03", N, threadCount);
  }

/*  public static void main(String[] args) throws InterruptedException {
    Test01();
    Test02();
    Test03();
    Test04();
    Test05_01();
    Test05_02();
    Test05_03();
    Test06_01();
    Test06_02();
    Test06_03();
  }*/
}
