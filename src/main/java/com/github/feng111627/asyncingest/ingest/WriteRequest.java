package com.github.feng111627.asyncingest.ingest;

/**
 * 一条写入请求。
 *
 * @param requestId 幂等键，重复提交同一 requestId 只会生效一次
 * @param key       业务键，同一键的写入按提交顺序生效
 * @param payload   写入内容
 */
public record WriteRequest(String requestId, String key, String payload) {
}
