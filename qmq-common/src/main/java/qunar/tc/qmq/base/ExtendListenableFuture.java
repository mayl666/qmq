package qunar.tc.qmq.base;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface ExtendListenableFuture<T> extends ListenableFuture<T> {

    /**
     * 注册回调
     *
     * @param callback 回调，当结果返回时执行
     * @param executor 回调将在该线程池里执行。注意: 如果将传递MoreExecutors.sameThreadExecutor()给executor的时候则回调工作在
     *                 client的io线程里，这个时候千万千万千万不要在回调里执行阻塞的方法
     */
    void addListener(FutureCallback<T> callback, Executor executor);

    /**
     * 设置支持超时的回调
     *
     * @param callback 回调，当结果返回时或者超时时执行
     * @param timeout  超时时间
     * @param unit     时间单位
     * @param executor 回调在该线程池里执行。注意: 如果将传递MoreExecutors.sameThreadExecutor()给executor的时候则回调工作在
     *                 client的io线程里，这个时候千万千万千万不要在回调里执行阻塞的方法
     */
    void addListener(FutureCallback<T> callback, long timeout, TimeUnit unit, Executor executor);

    /**
     * 该方法是block方法，不要在io线程池里执行
     *
     * @param timeout
     * @param unit
     * @return
     * @throws InterruptedException
     * @throws TimeoutException
     * @throws ExecutionException
     */
    T get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException;

    /**
     * 该方法是block方法，不要在io线程池里执行
     *
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    T get() throws InterruptedException, ExecutionException;
}
