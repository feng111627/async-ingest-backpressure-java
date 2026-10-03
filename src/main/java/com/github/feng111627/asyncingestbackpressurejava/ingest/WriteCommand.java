package com.github.feng111627.asyncingestbackpressurejava.ingest;

import java.util.concurrent.CompletableFuture;

/**
 * 一条待写入命令，附带用于通知调用方的 future。
 */
record WriteCommand(String key, String payload, CompletableFuture<WriteAck> result) {
}
