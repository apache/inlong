package org.apache.inlong.tubemq.corerpc.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollMode;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;

public class EventLoopUtil {
    public EventLoopUtil() {
    }

    public static EventLoopGroup newEventLoopGroup(int nThreads, boolean enableBusyWait, ThreadFactory threadFactory) {
        if (!Epoll.isAvailable()) {
            return new NioEventLoopGroup(nThreads, threadFactory);
        } else if (!enableBusyWait) {
            return new EpollEventLoopGroup(nThreads, threadFactory);
        } else {
            EpollEventLoopGroup eventLoopGroup = new EpollEventLoopGroup(nThreads, threadFactory, () -> {
                return (selectSupplier, hasTasks) -> {
                    return -3;
                };
            });
            return eventLoopGroup;
        }
    }

    public static Class<? extends SocketChannel> getClientSocketChannelClass(EventLoopGroup eventLoopGroup) {
        return eventLoopGroup instanceof EpollEventLoopGroup
                ? EpollSocketChannel.class : NioSocketChannel.class;
    }

    public static Class<? extends ServerSocketChannel> getServerSocketChannelClass(EventLoopGroup eventLoopGroup) {
        return eventLoopGroup instanceof EpollEventLoopGroup
                ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
    }

    public static Class<? extends DatagramChannel> getDatagramChannelClass(EventLoopGroup eventLoopGroup) {
        return eventLoopGroup instanceof EpollEventLoopGroup
                ? EpollDatagramChannel.class : NioDatagramChannel.class;
    }

    public static void enableTriggeredMode(ServerBootstrap bootstrap) {
        if (Epoll.isAvailable()) {
            bootstrap.childOption(EpollChannelOption.EPOLL_MODE, EpollMode.LEVEL_TRIGGERED);
        }

    }

    public static CompletableFuture<Void> shutdownGracefully(EventLoopGroup eventLoopGroup) {
        return toCompletableFutureVoid(eventLoopGroup.shutdownGracefully());
    }

    /**
     * get CompletableFuture<Void> by Future
     *
     * @param future Future
     * @return CompletableFuture<Void>
     */
    public static CompletableFuture<Void> toCompletableFutureVoid(Future<?> future) {
        Objects.requireNonNull(future, "future cannot be null");

        CompletableFuture<Void> adapter = new CompletableFuture<>();
        if (future.isDone()) {
            if (future.isSuccess()) {
                adapter.complete(null);
            } else {
                adapter.completeExceptionally(future.cause());
            }
        } else {
            future.addListener(f -> {
                if (f.isSuccess()) {
                    adapter.complete(null);
                } else {
                    adapter.completeExceptionally(f.cause());
                }
            });
        }
        return adapter;
    }
}