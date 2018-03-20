package lambdasinaction.chap7;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.stream.LongStream;

import static lambdasinaction.chap7.ParallelStreamsHarness.FORK_JOIN_POOL;

public class ForkJoinSumCalculator extends RecursiveTask<Long> {

    //이 값 이하의 서브태스크는 더이상 분할할 수 없다. (threshold : 임계치)
    public static final long THRESHOLD = 10;

    private final long[] numbers;   // 더할 숫자 배열
    private final int start;    //이 서브태스크에서 처리할 배열의 초기 위치
    private final int end;      //이 서비태스크에서 처리할 배열의 최종 위치

    //메인 태스크를 생성할 때 사용할 공개 생성자
    public ForkJoinSumCalculator(long[] numbers) {
        this(numbers, 0, numbers.length);
    }

    // 메인 태스크의 서브태스크를 재귀적으로 만들때 사용할 비공개 생성자
    private ForkJoinSumCalculator(long[] numbers, int start, int end) {
        this.numbers = numbers;
        this.start = start;
        this.end = end;
    }

    /**
     * RecursiveTask의 추상 메서드 오버라이드
     * @return
     */
    @Override
    protected Long compute() {
        int length = end - start;   // 이 태스크에서 더할 배열의 길이

        // 기준값과 같거나 작으면 순차적으로 결과를 계산한다.
        if (length <= THRESHOLD) {
            return computeSequentially();
        }

        // 배열의 첫번째 절반을 더하도록 서브태스크를 생성
        ForkJoinSumCalculator leftTask = new ForkJoinSumCalculator(numbers, start, start + length/2);
        leftTask.fork(); // ForkJoinPool의 다른 스레드로 새로 생성한 태스크를 비동기로 실행한다.

        //배열의 나머지 절반을 더하도록 서브태스크를 생성
        ForkJoinSumCalculator rightTask = new ForkJoinSumCalculator(numbers, start + length/2, end);
        Long rightResult = rightTask.compute(); //두 번째 서브태스크를 동기 실행. 이때 추가로 분할이 일어날 수 있음

        //첫 번째 서브태스크의 결과를 읽거나 아직 결과가 없으면 기다린다.
        Long leftResult = leftTask.join();

        //두 서브태스크의 결과를 조합한 값이 이 태스크의 결과다.
        return leftResult + rightResult;
    }

    /**
     * 더 분할할 수 없을 때 서브태스크의 결과를 계산하는 단순 알고리즘
     * @return
     */
    private long computeSequentially() {
        long sum = 0;
        for (int i = start; i < end; i++) {
            sum += numbers[i];
        }

//        long sum = Arrays.stream(numbers, start, end).reduce(0, (a, b) -> a + b);
        System.out.println(Thread.currentThread().getName() + ", start=" + start + ", end=" + end + ", sum=" + sum);
        return sum;
    }

    public static long forkJoinSum(long n) {
        long[] numbers = LongStream.rangeClosed(1, n).toArray();
        ForkJoinTask<Long> task = new ForkJoinSumCalculator(numbers);
        return FORK_JOIN_POOL.invoke(task);
    }

    //테스트를 위한 메인
    public static void main (String[] args) throws Exception {
        long[] values = LongStream.rangeClosed(1, 20).toArray();
        ForkJoinTask<Long> task = new ForkJoinSumCalculator(values);
        long totalSum = new ForkJoinPool().commonPool().invoke(task);
        System.out.println("Total sum: " + totalSum);
    }

}