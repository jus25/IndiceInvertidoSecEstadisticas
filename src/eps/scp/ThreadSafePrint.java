package eps.scp;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

public class ThreadSafePrint {

    private final ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(1000);
    private final ReentrantLock lock = new ReentrantLock();

    public void print(String message) {
        lock.lock();
        try {
            queue.add(message);
        } finally {
            lock.unlock();
        }
    }

    public void flush() {
        lock.lock();
        try {
            while (!queue.isEmpty()) {
                System.out.println(queue.poll());
            }
        } finally {
            lock.unlock();
        }
    }
}
