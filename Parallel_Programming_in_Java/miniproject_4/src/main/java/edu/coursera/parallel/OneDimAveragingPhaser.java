package edu.coursera.parallel;

import java.util.concurrent.Phaser;

/**
 * Wrapper class for implementing one-dimensional iterative averaging using
 * phasers.
 */
public final class OneDimAveragingPhaser {
    /**
     * Default constructor.
     */
    private OneDimAveragingPhaser() {
    }

    /**
     * Sequential implementation of one-dimensional iterative averaging.
     *
     * @param iterations The number of iterations to run
     * @param myNew A double array that starts as the output array
     * @param myVal A double array that contains the initial input to the
     *        iterative averaging problem
     * @param n The size of this problem
     */
    public static void runSequential(final int iterations, final double[] myNew,
            final double[] myVal, final int n) {
        double[] next = myNew;
        double[] curr = myVal;

        for (int iter = 0; iter < iterations; iter++) {
            for (int j = 1; j <= n; j++) {
                next[j] = (curr[j - 1] + curr[j + 1]) / 2.0;
            }
            double[] tmp = curr;
            curr = next;
            next = tmp;
        }
    }

    /**
     * An example parallel implementation of one-dimensional iterative averaging
     * that uses phasers as a simple barrier (arriveAndAwaitAdvance).
     *
     * @param iterations The number of iterations to run
     * @param myNew A double array that starts as the output array
     * @param myVal A double array that contains the initial input to the
     *        iterative averaging problem
     * @param n The size of this problem
     * @param tasks The number of threads/tasks to use to compute the solution
     */
    public static void runParallelBarrier(final int iterations,
            final double[] myNew, final double[] myVal, final int n,
            final int tasks) {
        Phaser ph = new Phaser(0);
        ph.bulkRegister(tasks);

        Thread[] threads = new Thread[tasks];

        for (int ii = 0; ii < tasks; ii++) {
            final int i = ii;

            threads[ii] = new Thread(() -> {
                double[] threadPrivateMyVal = myVal;
                double[] threadPrivateMyNew = myNew;

                //final int chunkSize = (n + tasks - 1) / tasks;
                //if (right > n) right = n;

                for (int iter = 0; iter < iterations; iter++) {
                    final int left = i * (n/tasks) + 1;
                    final int right = (i + 1) * (n/tasks);

                    for (int j = left; j <= right; j++) {
                        threadPrivateMyNew[j] = (threadPrivateMyVal[j - 1]
                            + threadPrivateMyVal[j + 1]) / 2.0;
                    }
                    ph.arriveAndAwaitAdvance();

                    double[] temp = threadPrivateMyNew;
                    threadPrivateMyNew = threadPrivateMyVal;
                    threadPrivateMyVal = temp;
                }
            });
            threads[ii].start();
        }

        for (int ii = 0; ii < tasks; ii++) {
            try {
                threads[ii].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * A parallel implementation of one-dimensional iterative averaging that
     * uses the Phaser.arrive and Phaser.awaitAdvance APIs to overlap
     * computation with barrier completion.
     *
     * TODO Complete this method based on the provided runSequential and
     * runParallelBarrier methods.
     *
     * @param iterations The number of iterations to run
     * @param myNew A double array that starts as the output array
     * @param myVal A double array that contains the initial input to the
     *              iterative averaging problem
     * @param n The size of this problem
     * @param tasks The number of threads/tasks to use to compute the solution
     */
    public static void runParallelFuzzyBarrier(final int iterations,
            final double[] myNew, final double[] myVal, final int n,
            final int tasks) {
        /*
        Phaser[] phs = new Phaser[tasks];
        for(int i=0; i<phs.length; i++){
            phs[i] = new Phaser(1);
        }

        Thread[] threads = new Thread[tasks];

        for(int i=0; i < tasks; i++){
            final int id = i;

            threads[i] = new Thread(() -> {
                double[] threadPrivateMyVal = myVal;
                double[] threadPrivateMyNew = myNew;

                for(int iter=0; iter<iterations; iter++){
                    final int left = id * (n / tasks) + 1;
                    final int right = (id + 1) * (n / tasks);

                    for(int j=left; j <= right; j++){
                        threadPrivateMyNew[j] = (threadPrivateMyVal[j-1]
                                + threadPrivateMyVal[j+1]) / 2.0;
                    }

                    int curPhaser = phs[id].arrive();

                    if(id-1 >1 ){
                        phs[id-1].awaitAdvance(curPhaser);
                    }
                    if(id+1 < tasks){ // Wrong2: id+1<0 , always the last one leads data race
                        phs[id+1].awaitAdvance(curPhaser);
                    }

                    double[] tmp = threadPrivateMyNew;
                    threadPrivateMyNew = threadPrivateMyVal;
                    threadPrivateMyVal = tmp;
                }
            });
            threads[i].start();
        }

        for (int ii = 0; ii < tasks; ii++) {
            try {
                threads[ii].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
*/
        //System.out.println("iterations " + iterations +
        //        "; myNew size " + myNew.length + "; myVal size " + myVal.length +
        //        "; n " + n + "; tasks " + tasks);

        Phaser[] phs = new Phaser[tasks];
        for(int i=0;i<phs.length;i++){
            phs[i] = new Phaser(1);
        }

        Thread[] threads = new Thread[tasks];

        for (int ii = 0; ii < tasks; ii++) {
            final int i = ii;

            threads[ii] = new Thread(() -> {
                double[] threadPrivateMyVal = myVal;
                double[] threadPrivateMyNew = myNew;

                for (int iter = 0; iter < iterations; iter++) {
                    final int left = i * (n / tasks) + 1;
                    final int right = (i + 1) * (n / tasks);

                    for (int j = left; j <= right; j++) {
                        threadPrivateMyNew[j] = (threadPrivateMyVal[j - 1]
                                + threadPrivateMyVal[j + 1]) / 2.0;
                    }
//                    System.out.println("Arriving task " + i + " left " + left + " right " + right);
                    int currentPhase = phs[i].arrive();
//                    System.out.println("Arrived task: "+ i + ", phase " + currentPhase);
                    if(i-1>=0){
//                        System.out.println("Arrived task"+ i +" Waiting for task" + (i-1));
                        phs[i-1].awaitAdvance(currentPhase);
                    }
                    if(i+1<tasks){
//                        System.out.println("Arrived task"+ i +" Waiting for task"+ (i+1));
                        phs[i+1].awaitAdvance(currentPhase);
                    }

                    double[] temp = threadPrivateMyNew;
                    threadPrivateMyNew = threadPrivateMyVal;
                    threadPrivateMyVal = temp;
                }
            });
            threads[ii].start();
        }

        for (int ii = 0; ii < tasks; ii++) {
            try {
                threads[ii].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("iterations " + iterations + " done");
    }

}
