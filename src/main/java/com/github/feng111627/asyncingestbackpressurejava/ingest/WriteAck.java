package com.github.feng111627.asyncingestbackpressurejava.ingest;

/**
 * 写入成功确认：包含业务键与该键内的生效序号（从 0 递增，按提交顺序）。
 */
public record WriteAck(String key, long sequence) {
}
