package pgdp.threads;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.List;

public class ParallelCompute {
  private static final int threadMaxCount =
    Runtime.getRuntime().availableProcessors();

  public static BigInteger[] parallelComputeFunctions(BigInteger[] data, Function<BigInteger, BigInteger>[] functions,
                                                      int threadCount) throws InterruptedException {
    if (data == null || functions == null) {
      throw new NullPointerException();
    }
    if (data.length == 0 || functions.length == 0 || data.length != functions.length || threadCount <= 0) {
      throw new IllegalArgumentException();
    }
    if (threadCount > data.length / 4) {
      threadCount = Math.max(data.length / 4, 1);
    }
    threadCount = Math.min(threadCount, threadMaxCount);

    List<Thread> threads = new ArrayList<>();
    BigInteger[] results = new BigInteger[data.length];
    for (int i = 0; i < threadCount; i++) {
      results[i] = data[i];
      Thread thread = new Thread() {
        private int index;
        private int step;

        Thread setIndexAndStep(int index, int step) {
          this.index = index;
          this.step = step;
          return this;
        }

        @Override
        public void run() {
          for (int j = index; j < data.length; j += step) {
            results[j] = functions[j].apply(data[j]);
          }
        }
      }.setIndexAndStep(i, threadCount);
      threads.add(thread);
      thread.start();
    }
    for (int i = 0; i < threads.size(); i++) {
      if (threads.get(i).isAlive()) {
        threads.get(i).join();
      }
    }
    return results;
  }

  public static BigInteger parallelReduceArray(BigInteger[] data, BinaryOperator<BigInteger> binOp, int threadCount)
    throws InterruptedException {
    if (data == null || binOp == null) {
      throw new NullPointerException();
    }
    if (data.length == 0 || threadCount <= 0) {
      throw new IllegalArgumentException();
    }
    if (threadCount > data.length / 2) {
      threadCount = Math.max(data.length / 2, 1);
    }
    threadCount = Math.min(threadCount, threadMaxCount);

    List<Thread> threads = new ArrayList<Thread>();
    BigInteger[] results = new BigInteger[threadCount];
    for (int i = 0; i < threadCount; i++) {
      results[i] = data[i];
      Thread thread = new Thread() {
        private int index;
        private int threadCount;

        Thread setIndexAndThreadCount(int index, int threadCount) {
          this.index = index;
          this.threadCount = threadCount;
          return this;
        }

        @Override
        public void run() {
          int step = data.length / threadCount;
          int end = (index + 1) * step;
          if (index == (threadCount - 1)) {
            end = data.length;
          }
          for (int j = index * step + 1; j < end; j++) {
            results[index] = binOp.apply(results[index], data[j]);
          }
        }
      }.setIndexAndThreadCount(i, threadCount);
      threads.add(thread);
      thread.start();
    }

    for (int i = 0; i < threads.size(); i++) {
      if (threads.get(i).isAlive()) {
        threads.get(i).join();
      }
    }

    BigInteger result = results[0];
    for (int i = 1; i < results.length; i++) {
      result = binOp.apply(result, results[i]);
    }
    return result;
  }

  static void ReduceRegularTest(String testName, BigInteger[] data, BinaryOperator<BigInteger> binOp, int threadCount) {
    try {
      BigInteger expected = data[0];
      for (int i = 1; i < data.length; i++) {
        expected = binOp.apply(expected, data[i]);
      }
      BigInteger actual = ParallelCompute.parallelReduceArray(data, binOp, threadCount);
      System.out.printf(
        "%s (data size:%s, threads:%s): %s == %s \n=> %s\n", testName, data.length, threadCount,
        actual, expected, actual.equals(expected));
    } catch (Exception ex) {
      System.out.printf(
        "%s: %s == %s \n=> %s\n", testName, ex.getMessage(), "", false);
    }
    System.out.println();
  }

  static void ReduceTest01() {
    BinaryOperator<BigInteger> op = BinaryOperator.maxBy((a, b) -> (a.compareTo(b) == 1) ? 1 : ((a == b) ? 0 : -1));
    BigInteger[] data = new BigInteger[10];
    data[0] = BigInteger.valueOf(123435);
    data[1] = BigInteger.valueOf(123434);
    data[2] = BigInteger.valueOf(123435);
    data[3] = BigInteger.valueOf(12348);
    data[4] = BigInteger.valueOf(12346);
    data[5] = BigInteger.valueOf(123434);
    data[6] = BigInteger.valueOf(1234436);
    data[7] = BigInteger.valueOf(12348990);
    data[8] = BigInteger.valueOf(123423);
    data[9] = BigInteger.valueOf(1234);
    int threadCount = 2;
    ReduceRegularTest("ReduceTest01", data, op, threadCount);
  }

  static void ReduceTest02() {
    BinaryOperator<BigInteger> multiply = (x, y) -> x.multiply(y);
    BigInteger[] data = new BigInteger[10];
    data[0] = BigInteger.valueOf(123435);
    data[1] = BigInteger.valueOf(123434);
    data[2] = BigInteger.valueOf(123435);
    data[3] = BigInteger.valueOf(12348);
    data[4] = BigInteger.valueOf(12346);
    data[5] = BigInteger.valueOf(123434);
    data[6] = BigInteger.valueOf(1234436);
    data[7] = BigInteger.valueOf(12348990);
    data[8] = BigInteger.valueOf(123423);
    data[9] = BigInteger.valueOf(1234);
    int threadCount = 2;
    ReduceRegularTest("ReduceTest02", data, multiply, threadCount);
  }

  static void ComputeRegularTest(
    String testName, BigInteger[] data,
    Function<BigInteger, BigInteger>[] functions, int threadCount) {
    try {
      BigInteger[] expected = new BigInteger[data.length];
      for (int i = 0; i < data.length; i++) {
        expected[i] = functions[i].apply(data[i]);
      }
      BigInteger[] actual = ParallelCompute.parallelComputeFunctions(
        data, functions, threadCount);
      List<BigInteger> expected_list = Arrays.asList(expected);
      List<BigInteger> actual_list = Arrays.asList(actual);
      System.out.printf(
        "%s (data size:%s, threads:%s): %s == %s \n=> %s\n", testName, data.length, threadCount,
        actual_list, expected_list, actual_list.equals(expected_list));
    } catch (Exception ex) {
      System.out.printf(
        "%s: %s == %s \n=> %s\n", testName, ex.getMessage(), "", false);
    }
    System.out.println();
  }

  static void ComputeTest01() {
    Function<BigInteger, BigInteger>[] functions = new Function[6];
    functions[0] = (x) -> x.multiply(x);
    functions[1] = (x) -> x.add(x);
    functions[2] = (x) -> x.abs();
    functions[3] = (x) -> x.abs();
    functions[4] = (x) -> x.add(x);
    functions[5] = (x) -> x.multiply(x);
    BigInteger[] data = new BigInteger[6];
    data[0] = BigInteger.valueOf(123435);
    data[1] = BigInteger.valueOf(123434);
    data[2] = BigInteger.valueOf(-123435);
    data[3] = BigInteger.valueOf(123435 * 123435);
    data[4] = BigInteger.valueOf(123434 + 123435);
    data[5] = BigInteger.valueOf(123435 - 12343);
    int threadCount = 2;
    ComputeRegularTest("ComputeTest01", data, functions, threadCount);
  }

  public static void main(String[] args) throws InterruptedException {
    ReduceTest01();
    ReduceTest02();
    ComputeTest01();
  }
}