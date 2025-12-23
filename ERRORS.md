# Errors from latest run

- Source log: `/home/user/code/HillsInspector/logs/hills_inspector_2025-12-23.log`
- Extracted: 2025-12-23T07:42:35.273192
- Error count: 20
- Context window: 5 lines before/after

## Error 1 (log line 1924)

```text
00:42:01 | INFO     | ingestion_service:ingest_property_async:656 | {} - No legal results; trying ORI party search by owner: ['YGR CARGO LLC*', 'LLC YGR*']
00:42:19 | INFO     | ingestion_service:ingest_property_async:672 | {} - Owner party search yielded 4 relevant documents
00:42:19 | INFO     | ingestion_service:ingest_property_async:684 | {} - Successful search term: OWNER_PARTY:YGR CARGO LLC*
00:42:19 | INFO     | ingestion_service:ingest_property_async:690 | {} - Grouped 4 raw records into 4 unique documents
00:42:41 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://192.168.86.26:1234/v1/chat/completions: HTTPConnectionPool(host='192.168.86.26', port=1234): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50b43aa1b0>, 'Connection to 192.168.86.26 timed out. (connect timeout=180)'))
00:42:41 | ERROR    | vision_service:_try_all_endpoints:1028 | {} - All vision endpoints failed: ['http://10.10.0.33:6969/v1/chat/completions: Timeout', 'http://10.10.1.5:6969/v1/chat/completions: Connection error', 'http://10.10.2.27:6969/v1/chat/completions: Timeout', 'http://192.168.86.26:1234/v1/chat/completions: Timeout']
00:42:41 | ERROR    | vision_service:analyze_images:1194 | {} - All vision endpoints failed for multi-image request (1 images)
00:43:02 | INFO     | ingestion_service:_download_and_analyze_document:906 | {} - Analyzing PDF: unknown_D_DEED_2019403239.pdf
00:43:02 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.0.33:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
00:45:28 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.0.33:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.0.33', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af653c80>, 'Connection to 10.10.0.33 timed out. (connect timeout=180)'))
00:45:28 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.1.5:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
```

## Error 2 (log line 1925)

```text
00:42:19 | INFO     | ingestion_service:ingest_property_async:672 | {} - Owner party search yielded 4 relevant documents
00:42:19 | INFO     | ingestion_service:ingest_property_async:684 | {} - Successful search term: OWNER_PARTY:YGR CARGO LLC*
00:42:19 | INFO     | ingestion_service:ingest_property_async:690 | {} - Grouped 4 raw records into 4 unique documents
00:42:41 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://192.168.86.26:1234/v1/chat/completions: HTTPConnectionPool(host='192.168.86.26', port=1234): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50b43aa1b0>, 'Connection to 192.168.86.26 timed out. (connect timeout=180)'))
00:42:41 | ERROR    | vision_service:_try_all_endpoints:1028 | {} - All vision endpoints failed: ['http://10.10.0.33:6969/v1/chat/completions: Timeout', 'http://10.10.1.5:6969/v1/chat/completions: Connection error', 'http://10.10.2.27:6969/v1/chat/completions: Timeout', 'http://192.168.86.26:1234/v1/chat/completions: Timeout']
00:42:41 | ERROR    | vision_service:analyze_images:1194 | {} - All vision endpoints failed for multi-image request (1 images)
00:43:02 | INFO     | ingestion_service:_download_and_analyze_document:906 | {} - Analyzing PDF: unknown_D_DEED_2019403239.pdf
00:43:02 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.0.33:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
00:45:28 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.0.33:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.0.33', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af653c80>, 'Connection to 10.10.0.33 timed out. (connect timeout=180)'))
00:45:28 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.1.5:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
00:45:28 | WARNING  | vision_service:_try_all_endpoints:1021 | {} - Connection error on endpoint http://10.10.1.5:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.1.5', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by NewConnectionError('<urllib3.connection.HTTPConnection object at 0x7e50af651c70>: Failed to establish a new connection: [Errno 111] Connection refused'))
```

## Error 3 (log line 1935)

```text
00:45:28 | WARNING  | vision_service:_try_all_endpoints:1021 | {} - Connection error on endpoint http://10.10.1.5:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.1.5', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by NewConnectionError('<urllib3.connection.HTTPConnection object at 0x7e50af651c70>: Failed to establish a new connection: [Errno 111] Connection refused'))
00:45:28 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.2.27:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
00:47:56 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.2.27:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.2.27', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af650590>, 'Connection to 10.10.2.27 timed out. (connect timeout=180)'))
00:47:56 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://192.168.86.26:1234/v1/chat/completions (model: qwen/qwen3-vl-8b)
00:50:24 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://192.168.86.26:1234/v1/chat/completions: HTTPConnectionPool(host='192.168.86.26', port=1234): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af6510d0>, 'Connection to 192.168.86.26 timed out. (connect timeout=180)'))
00:50:24 | ERROR    | vision_service:_try_all_endpoints:1028 | {} - All vision endpoints failed: ['http://10.10.0.33:6969/v1/chat/completions: Timeout', 'http://10.10.1.5:6969/v1/chat/completions: Connection error', 'http://10.10.2.27:6969/v1/chat/completions: Timeout', 'http://192.168.86.26:1234/v1/chat/completions: Timeout']
00:50:24 | ERROR    | vision_service:analyze_images:1194 | {} - All vision endpoints failed for multi-image request (3 images)
00:50:43 | INFO     | ingestion_service:_download_and_analyze_document:906 | {} - Analyzing PDF: unknown_MTGNT_MORTGAGE_EXEMPT_TAXES_2023566331.pdf
00:50:43 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.0.33:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
00:53:12 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.0.33:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.0.33', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af652cf0>, 'Connection to 10.10.0.33 timed out. (connect timeout=180)'))
00:53:12 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.1.5:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
```

## Error 4 (log line 1936)

```text
00:45:28 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.2.27:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
00:47:56 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.2.27:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.2.27', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af650590>, 'Connection to 10.10.2.27 timed out. (connect timeout=180)'))
00:47:56 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://192.168.86.26:1234/v1/chat/completions (model: qwen/qwen3-vl-8b)
00:50:24 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://192.168.86.26:1234/v1/chat/completions: HTTPConnectionPool(host='192.168.86.26', port=1234): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af6510d0>, 'Connection to 192.168.86.26 timed out. (connect timeout=180)'))
00:50:24 | ERROR    | vision_service:_try_all_endpoints:1028 | {} - All vision endpoints failed: ['http://10.10.0.33:6969/v1/chat/completions: Timeout', 'http://10.10.1.5:6969/v1/chat/completions: Connection error', 'http://10.10.2.27:6969/v1/chat/completions: Timeout', 'http://192.168.86.26:1234/v1/chat/completions: Timeout']
00:50:24 | ERROR    | vision_service:analyze_images:1194 | {} - All vision endpoints failed for multi-image request (3 images)
00:50:43 | INFO     | ingestion_service:_download_and_analyze_document:906 | {} - Analyzing PDF: unknown_MTGNT_MORTGAGE_EXEMPT_TAXES_2023566331.pdf
00:50:43 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.0.33:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
00:53:12 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.0.33:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.0.33', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af652cf0>, 'Connection to 10.10.0.33 timed out. (connect timeout=180)'))
00:53:12 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.1.5:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
00:53:12 | WARNING  | vision_service:_try_all_endpoints:1021 | {} - Connection error on endpoint http://10.10.1.5:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.1.5', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by NewConnectionError('<urllib3.connection.HTTPConnection object at 0x7e50af653080>: Failed to establish a new connection: [Errno 111] Connection refused'))
```

## Error 5 (log line 1946)

```text
00:53:12 | WARNING  | vision_service:_try_all_endpoints:1021 | {} - Connection error on endpoint http://10.10.1.5:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.1.5', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by NewConnectionError('<urllib3.connection.HTTPConnection object at 0x7e50af653080>: Failed to establish a new connection: [Errno 111] Connection refused'))
00:53:12 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.2.27:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
00:55:40 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.2.27:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.2.27', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af6511c0>, 'Connection to 10.10.2.27 timed out. (connect timeout=180)'))
00:55:40 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://192.168.86.26:1234/v1/chat/completions (model: qwen/qwen3-vl-8b)
00:58:09 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://192.168.86.26:1234/v1/chat/completions: HTTPConnectionPool(host='192.168.86.26', port=1234): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af653d40>, 'Connection to 192.168.86.26 timed out. (connect timeout=180)'))
00:58:09 | ERROR    | vision_service:_try_all_endpoints:1028 | {} - All vision endpoints failed: ['http://10.10.0.33:6969/v1/chat/completions: Timeout', 'http://10.10.1.5:6969/v1/chat/completions: Connection error', 'http://10.10.2.27:6969/v1/chat/completions: Timeout', 'http://192.168.86.26:1234/v1/chat/completions: Timeout']
00:58:09 | ERROR    | vision_service:analyze_images:1194 | {} - All vision endpoints failed for multi-image request (5 images)
00:58:09 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.0.33:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
00:58:22 | SUCCESS  | market_scraper:get_listing_details:125 | {} - Successfully scraped Realtor.com for 2560 REGAL RIVER RD
00:58:23 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.0.33:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
00:58:24 | INFO     | tax_scraper:_extract_tax_from_screenshot:551 | {} - Vision extraction result: {
```

## Error 6 (log line 1947)

```text
00:53:12 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.2.27:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
00:55:40 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.2.27:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.2.27', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af6511c0>, 'Connection to 10.10.2.27 timed out. (connect timeout=180)'))
00:55:40 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://192.168.86.26:1234/v1/chat/completions (model: qwen/qwen3-vl-8b)
00:58:09 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://192.168.86.26:1234/v1/chat/completions: HTTPConnectionPool(host='192.168.86.26', port=1234): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af653d40>, 'Connection to 192.168.86.26 timed out. (connect timeout=180)'))
00:58:09 | ERROR    | vision_service:_try_all_endpoints:1028 | {} - All vision endpoints failed: ['http://10.10.0.33:6969/v1/chat/completions: Timeout', 'http://10.10.1.5:6969/v1/chat/completions: Connection error', 'http://10.10.2.27:6969/v1/chat/completions: Timeout', 'http://192.168.86.26:1234/v1/chat/completions: Timeout']
00:58:09 | ERROR    | vision_service:analyze_images:1194 | {} - All vision endpoints failed for multi-image request (5 images)
00:58:09 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.0.33:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
00:58:22 | SUCCESS  | market_scraper:get_listing_details:125 | {} - Successfully scraped Realtor.com for 2560 REGAL RIVER RD
00:58:23 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.0.33:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
00:58:24 | INFO     | tax_scraper:_extract_tax_from_screenshot:551 | {} - Vision extraction result: {
  "account_number": "A0874984086",
```

## Error 7 (log line 3327)

```text
01:21:13 | WARNING  | vision_service:_try_all_endpoints:1021 | {} - Connection error on endpoint http://10.10.1.5:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.1.5', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by NewConnectionError('<urllib3.connection.HTTPConnection object at 0x7e50f0294ef0>: Failed to establish a new connection: [Errno 111] Connection refused'))
01:21:13 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.2.27:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
01:23:27 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.2.27:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.2.27', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af214bc0>, 'Connection to 10.10.2.27 timed out. (connect timeout=180)'))
01:23:27 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://192.168.86.26:1234/v1/chat/completions (model: qwen/qwen3-vl-8b)
01:25:42 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://192.168.86.26:1234/v1/chat/completions: HTTPConnectionPool(host='192.168.86.26', port=1234): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af20da30>, 'Connection to 192.168.86.26 timed out. (connect timeout=180)'))
01:25:42 | ERROR    | vision_service:_try_all_endpoints:1028 | {} - All vision endpoints failed: ['http://10.10.0.33:6969/v1/chat/completions: Timeout', 'http://10.10.1.5:6969/v1/chat/completions: Connection error', 'http://10.10.2.27:6969/v1/chat/completions: Timeout', 'http://192.168.86.26:1234/v1/chat/completions: Timeout']
01:25:42 | ERROR    | vision_service:analyze_images:1194 | {} - All vision endpoints failed for multi-image request (5 images)
01:25:57 | INFO     | ingestion_service:_download_and_analyze_document:906 | {} - Analyzing PDF: unknown_D_DEED_2022506352.pdf
01:25:58 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.0.33:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
01:28:12 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.0.33:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.0.33', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af202240>, 'Connection to 10.10.0.33 timed out. (connect timeout=180)'))
01:28:12 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.1.5:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
```

## Error 8 (log line 3328)

```text
01:21:13 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.2.27:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
01:23:27 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.2.27:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.2.27', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af214bc0>, 'Connection to 10.10.2.27 timed out. (connect timeout=180)'))
01:23:27 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://192.168.86.26:1234/v1/chat/completions (model: qwen/qwen3-vl-8b)
01:25:42 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://192.168.86.26:1234/v1/chat/completions: HTTPConnectionPool(host='192.168.86.26', port=1234): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af20da30>, 'Connection to 192.168.86.26 timed out. (connect timeout=180)'))
01:25:42 | ERROR    | vision_service:_try_all_endpoints:1028 | {} - All vision endpoints failed: ['http://10.10.0.33:6969/v1/chat/completions: Timeout', 'http://10.10.1.5:6969/v1/chat/completions: Connection error', 'http://10.10.2.27:6969/v1/chat/completions: Timeout', 'http://192.168.86.26:1234/v1/chat/completions: Timeout']
01:25:42 | ERROR    | vision_service:analyze_images:1194 | {} - All vision endpoints failed for multi-image request (5 images)
01:25:57 | INFO     | ingestion_service:_download_and_analyze_document:906 | {} - Analyzing PDF: unknown_D_DEED_2022506352.pdf
01:25:58 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.0.33:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
01:28:12 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.0.33:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.0.33', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af202240>, 'Connection to 10.10.0.33 timed out. (connect timeout=180)'))
01:28:12 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.1.5:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
01:28:12 | WARNING  | vision_service:_try_all_endpoints:1021 | {} - Connection error on endpoint http://10.10.1.5:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.1.5', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by NewConnectionError('<urllib3.connection.HTTPConnection object at 0x7e50e02642f0>: Failed to establish a new connection: [Errno 111] Connection refused'))
```

## Error 9 (log line 3338)

```text
01:28:12 | WARNING  | vision_service:_try_all_endpoints:1021 | {} - Connection error on endpoint http://10.10.1.5:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.1.5', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by NewConnectionError('<urllib3.connection.HTTPConnection object at 0x7e50e02642f0>: Failed to establish a new connection: [Errno 111] Connection refused'))
01:28:12 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.2.27:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
01:30:27 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.2.27:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.2.27', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50f02942f0>, 'Connection to 10.10.2.27 timed out. (connect timeout=180)'))
01:30:27 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://192.168.86.26:1234/v1/chat/completions (model: qwen/qwen3-vl-8b)
01:32:42 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://192.168.86.26:1234/v1/chat/completions: HTTPConnectionPool(host='192.168.86.26', port=1234): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50f0294f20>, 'Connection to 192.168.86.26 timed out. (connect timeout=180)'))
01:32:42 | ERROR    | vision_service:_try_all_endpoints:1028 | {} - All vision endpoints failed: ['http://10.10.0.33:6969/v1/chat/completions: Timeout', 'http://10.10.1.5:6969/v1/chat/completions: Connection error', 'http://10.10.2.27:6969/v1/chat/completions: Timeout', 'http://192.168.86.26:1234/v1/chat/completions: Timeout']
01:32:42 | ERROR    | vision_service:analyze_images:1194 | {} - All vision endpoints failed for multi-image request (2 images)
01:32:42 | INFO     | ingestion_service:ingest_property_async:749 | {} - Building Chain of Title...
01:32:42 | SUCCESS  | ingestion_service:ingest_property_async:859 | {} - Ingestion complete for 292024CA000157A001HC (4 total docs)
01:32:42 | INFO     | orchestrator:_enrich_property:766 | {} - Phase 3: Starting Analysis for 192817439000004000230A
01:32:42 | INFO     | permit_scraper:_scrape_accela:252 | {} - Searching City of Tampa Global permits for: 2318 E 111TH AVE, TAMPA, FL- 33612
```

## Error 10 (log line 3339)

```text
01:28:12 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.2.27:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
01:30:27 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.2.27:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.2.27', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50f02942f0>, 'Connection to 10.10.2.27 timed out. (connect timeout=180)'))
01:30:27 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://192.168.86.26:1234/v1/chat/completions (model: qwen/qwen3-vl-8b)
01:32:42 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://192.168.86.26:1234/v1/chat/completions: HTTPConnectionPool(host='192.168.86.26', port=1234): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50f0294f20>, 'Connection to 192.168.86.26 timed out. (connect timeout=180)'))
01:32:42 | ERROR    | vision_service:_try_all_endpoints:1028 | {} - All vision endpoints failed: ['http://10.10.0.33:6969/v1/chat/completions: Timeout', 'http://10.10.1.5:6969/v1/chat/completions: Connection error', 'http://10.10.2.27:6969/v1/chat/completions: Timeout', 'http://192.168.86.26:1234/v1/chat/completions: Timeout']
01:32:42 | ERROR    | vision_service:analyze_images:1194 | {} - All vision endpoints failed for multi-image request (2 images)
01:32:42 | INFO     | ingestion_service:ingest_property_async:749 | {} - Building Chain of Title...
01:32:42 | SUCCESS  | ingestion_service:ingest_property_async:859 | {} - Ingestion complete for 292024CA000157A001HC (4 total docs)
01:32:42 | INFO     | orchestrator:_enrich_property:766 | {} - Phase 3: Starting Analysis for 192817439000004000230A
01:32:42 | INFO     | permit_scraper:_scrape_accela:252 | {} - Searching City of Tampa Global permits for: 2318 E 111TH AVE, TAMPA, FL- 33612
01:32:42 | INFO     | orchestrator:_run_survival_analysis:1178 | {} -   Survival for 192817439000004000230A: Survived: 1, Extinguished: 1, Historical: 0, Foreclosing: 1
```

## Error 11 (log line 3357)

```text
01:35:20 | WARNING  | vision_service:_try_all_endpoints:1021 | {} - Connection error on endpoint http://10.10.1.5:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.1.5', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by NewConnectionError('<urllib3.connection.HTTPConnection object at 0x7e50b42011f0>: Failed to establish a new connection: [Errno 111] Connection refused'))
01:35:20 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.2.27:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
01:37:35 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.2.27:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.2.27', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50b4200a10>, 'Connection to 10.10.2.27 timed out. (connect timeout=180)'))
01:37:35 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://192.168.86.26:1234/v1/chat/completions (model: qwen/qwen3-vl-8b)
01:39:50 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://192.168.86.26:1234/v1/chat/completions: HTTPConnectionPool(host='192.168.86.26', port=1234): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50b42034d0>, 'Connection to 192.168.86.26 timed out. (connect timeout=180)'))
01:39:50 | ERROR    | vision_service:_try_all_endpoints:1028 | {} - All vision endpoints failed: ['http://10.10.0.33:6969/v1/chat/completions: Timeout', 'http://10.10.1.5:6969/v1/chat/completions: Connection error', 'http://10.10.2.27:6969/v1/chat/completions: Timeout', 'http://192.168.86.26:1234/v1/chat/completions: Timeout']
01:39:50 | ERROR    | vision_service:analyze_images:1194 | {} - All vision endpoints failed for multi-image request (5 images)
01:40:10 | INFO     | ingestion_service:_download_and_analyze_document:906 | {} - Analyzing PDF: unknown_JUD_JUDGMENT_2017246900.pdf
01:40:11 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.0.33:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
01:40:33 | SUCCESS  | ingestion_service:_download_and_analyze_document:914 | {} - Extracted data from (JUD) JUDGMENT: 2017246900
01:40:33 | INFO     | ingestion_service:_update_parties_from_extraction:959 | {} -   Updated party1 from vLLM: RHONDA COOK, UNMARRIED
```

## Error 12 (log line 3358)

```text
01:35:20 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.2.27:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
01:37:35 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.2.27:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.2.27', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50b4200a10>, 'Connection to 10.10.2.27 timed out. (connect timeout=180)'))
01:37:35 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://192.168.86.26:1234/v1/chat/completions (model: qwen/qwen3-vl-8b)
01:39:50 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://192.168.86.26:1234/v1/chat/completions: HTTPConnectionPool(host='192.168.86.26', port=1234): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50b42034d0>, 'Connection to 192.168.86.26 timed out. (connect timeout=180)'))
01:39:50 | ERROR    | vision_service:_try_all_endpoints:1028 | {} - All vision endpoints failed: ['http://10.10.0.33:6969/v1/chat/completions: Timeout', 'http://10.10.1.5:6969/v1/chat/completions: Connection error', 'http://10.10.2.27:6969/v1/chat/completions: Timeout', 'http://192.168.86.26:1234/v1/chat/completions: Timeout']
01:39:50 | ERROR    | vision_service:analyze_images:1194 | {} - All vision endpoints failed for multi-image request (5 images)
01:40:10 | INFO     | ingestion_service:_download_and_analyze_document:906 | {} - Analyzing PDF: unknown_JUD_JUDGMENT_2017246900.pdf
01:40:11 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.0.33:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
01:40:33 | SUCCESS  | ingestion_service:_download_and_analyze_document:914 | {} - Extracted data from (JUD) JUDGMENT: 2017246900
01:40:33 | INFO     | ingestion_service:_update_parties_from_extraction:959 | {} -   Updated party1 from vLLM: RHONDA COOK, UNMARRIED
01:40:33 | INFO     | ingestion_service:ingest_property_async:749 | {} - Building Chain of Title...
```

## Error 13 (log line 3679)

```text
02:07:15 | WARNING  | vision_service:_try_all_endpoints:1021 | {} - Connection error on endpoint http://10.10.1.5:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.1.5', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by NewConnectionError('<urllib3.connection.HTTPConnection object at 0x7e50f02c85f0>: Failed to establish a new connection: [Errno 111] Connection refused'))
02:07:15 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.2.27:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
02:09:30 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.2.27:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.2.27', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50f02cb9b0>, 'Connection to 10.10.2.27 timed out. (connect timeout=180)'))
02:09:30 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://192.168.86.26:1234/v1/chat/completions (model: qwen/qwen3-vl-8b)
02:11:45 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://192.168.86.26:1234/v1/chat/completions: HTTPConnectionPool(host='192.168.86.26', port=1234): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50f02c92e0>, 'Connection to 192.168.86.26 timed out. (connect timeout=180)'))
02:11:45 | ERROR    | vision_service:_try_all_endpoints:1028 | {} - All vision endpoints failed: ['http://10.10.0.33:6969/v1/chat/completions: Timeout', 'http://10.10.1.5:6969/v1/chat/completions: Connection error', 'http://10.10.2.27:6969/v1/chat/completions: Timeout', 'http://192.168.86.26:1234/v1/chat/completions: Timeout']
02:11:45 | ERROR    | vision_service:analyze_images:1194 | {} - All vision endpoints failed for multi-image request (1 images)
02:11:58 | INFO     | ingestion_service:_download_and_analyze_document:906 | {} - Analyzing PDF: unknown_AFF_AFFIDAVIT_-6735622.pdf
02:11:58 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.0.33:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
02:12:02 | SUCCESS  | ingestion_service:_download_and_analyze_document:914 | {} - Extracted data from (AFF) AFFIDAVIT: -6735622
02:12:15 | INFO     | ingestion_service:_download_and_analyze_document:906 | {} - Analyzing PDF: unknown_SAT_SATISFACTION_2267.pdf
```

## Error 14 (log line 3680)

```text
02:07:15 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.2.27:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
02:09:30 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.2.27:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.2.27', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50f02cb9b0>, 'Connection to 10.10.2.27 timed out. (connect timeout=180)'))
02:09:30 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://192.168.86.26:1234/v1/chat/completions (model: qwen/qwen3-vl-8b)
02:11:45 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://192.168.86.26:1234/v1/chat/completions: HTTPConnectionPool(host='192.168.86.26', port=1234): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50f02c92e0>, 'Connection to 192.168.86.26 timed out. (connect timeout=180)'))
02:11:45 | ERROR    | vision_service:_try_all_endpoints:1028 | {} - All vision endpoints failed: ['http://10.10.0.33:6969/v1/chat/completions: Timeout', 'http://10.10.1.5:6969/v1/chat/completions: Connection error', 'http://10.10.2.27:6969/v1/chat/completions: Timeout', 'http://192.168.86.26:1234/v1/chat/completions: Timeout']
02:11:45 | ERROR    | vision_service:analyze_images:1194 | {} - All vision endpoints failed for multi-image request (1 images)
02:11:58 | INFO     | ingestion_service:_download_and_analyze_document:906 | {} - Analyzing PDF: unknown_AFF_AFFIDAVIT_-6735622.pdf
02:11:58 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.0.33:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
02:12:02 | SUCCESS  | ingestion_service:_download_and_analyze_document:914 | {} - Extracted data from (AFF) AFFIDAVIT: -6735622
02:12:15 | INFO     | ingestion_service:_download_and_analyze_document:906 | {} - Analyzing PDF: unknown_SAT_SATISFACTION_2267.pdf
02:12:16 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.0.33:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
```

## Error 15 (log line 3915)

```text
02:34:17 | WARNING  | vision_service:_try_all_endpoints:1021 | {} - Connection error on endpoint http://10.10.1.5:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.1.5', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by NewConnectionError('<urllib3.connection.HTTPConnection object at 0x7e50aedfbfe0>: Failed to establish a new connection: [Errno 111] Connection refused'))
02:34:17 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.2.27:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
02:36:32 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.2.27:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.2.27', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af6b1310>, 'Connection to 10.10.2.27 timed out. (connect timeout=180)'))
02:36:32 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://192.168.86.26:1234/v1/chat/completions (model: qwen/qwen3-vl-8b)
02:38:47 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://192.168.86.26:1234/v1/chat/completions: HTTPConnectionPool(host='192.168.86.26', port=1234): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af6b2750>, 'Connection to 192.168.86.26 timed out. (connect timeout=180)'))
02:38:47 | ERROR    | vision_service:_try_all_endpoints:1028 | {} - All vision endpoints failed: ['http://10.10.0.33:6969/v1/chat/completions: Timeout', 'http://10.10.1.5:6969/v1/chat/completions: Connection error', 'http://10.10.2.27:6969/v1/chat/completions: Timeout', 'http://192.168.86.26:1234/v1/chat/completions: Timeout']
02:38:47 | ERROR    | vision_service:analyze_images:1194 | {} - All vision endpoints failed for multi-image request (2 images)
02:39:08 | INFO     | ingestion_service:_download_and_analyze_document:906 | {} - Analyzing PDF: unknown_MTG_MORTGAGE_2020336996.pdf
02:39:09 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.0.33:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
02:41:23 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.0.33:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.0.33', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af6b25d0>, 'Connection to 10.10.0.33 timed out. (connect timeout=180)'))
02:41:23 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.1.5:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
```

## Error 16 (log line 3916)

```text
02:34:17 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.2.27:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
02:36:32 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.2.27:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.2.27', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af6b1310>, 'Connection to 10.10.2.27 timed out. (connect timeout=180)'))
02:36:32 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://192.168.86.26:1234/v1/chat/completions (model: qwen/qwen3-vl-8b)
02:38:47 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://192.168.86.26:1234/v1/chat/completions: HTTPConnectionPool(host='192.168.86.26', port=1234): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af6b2750>, 'Connection to 192.168.86.26 timed out. (connect timeout=180)'))
02:38:47 | ERROR    | vision_service:_try_all_endpoints:1028 | {} - All vision endpoints failed: ['http://10.10.0.33:6969/v1/chat/completions: Timeout', 'http://10.10.1.5:6969/v1/chat/completions: Connection error', 'http://10.10.2.27:6969/v1/chat/completions: Timeout', 'http://192.168.86.26:1234/v1/chat/completions: Timeout']
02:38:47 | ERROR    | vision_service:analyze_images:1194 | {} - All vision endpoints failed for multi-image request (2 images)
02:39:08 | INFO     | ingestion_service:_download_and_analyze_document:906 | {} - Analyzing PDF: unknown_MTG_MORTGAGE_2020336996.pdf
02:39:09 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.0.33:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
02:41:23 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.0.33:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.0.33', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af6b25d0>, 'Connection to 10.10.0.33 timed out. (connect timeout=180)'))
02:41:23 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.1.5:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
02:41:23 | WARNING  | vision_service:_try_all_endpoints:1021 | {} - Connection error on endpoint http://10.10.1.5:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.1.5', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by NewConnectionError('<urllib3.connection.HTTPConnection object at 0x7e50af6b2930>: Failed to establish a new connection: [Errno 111] Connection refused'))
```

## Error 17 (log line 3926)

```text
02:41:23 | WARNING  | vision_service:_try_all_endpoints:1021 | {} - Connection error on endpoint http://10.10.1.5:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.1.5', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by NewConnectionError('<urllib3.connection.HTTPConnection object at 0x7e50af6b2930>: Failed to establish a new connection: [Errno 111] Connection refused'))
02:41:23 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.2.27:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
02:43:38 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.2.27:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.2.27', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af6b2e10>, 'Connection to 10.10.2.27 timed out. (connect timeout=180)'))
02:43:38 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://192.168.86.26:1234/v1/chat/completions (model: qwen/qwen3-vl-8b)
02:45:53 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://192.168.86.26:1234/v1/chat/completions: HTTPConnectionPool(host='192.168.86.26', port=1234): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50aedf9e50>, 'Connection to 192.168.86.26 timed out. (connect timeout=180)'))
02:45:53 | ERROR    | vision_service:_try_all_endpoints:1028 | {} - All vision endpoints failed: ['http://10.10.0.33:6969/v1/chat/completions: Timeout', 'http://10.10.1.5:6969/v1/chat/completions: Connection error', 'http://10.10.2.27:6969/v1/chat/completions: Timeout', 'http://192.168.86.26:1234/v1/chat/completions: Timeout']
02:45:53 | ERROR    | vision_service:analyze_images:1194 | {} - All vision endpoints failed for multi-image request (5 images)
02:46:11 | INFO     | ingestion_service:_download_and_analyze_document:906 | {} - Analyzing PDF: unknown_MTG_MORTGAGE_2015496454.pdf
02:46:12 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.0.33:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
02:48:27 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.0.33:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.0.33', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50aedfab40>, 'Connection to 10.10.0.33 timed out. (connect timeout=180)'))
02:48:27 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.1.5:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
```

## Error 18 (log line 3927)

```text
02:41:23 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.2.27:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
02:43:38 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.2.27:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.2.27', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af6b2e10>, 'Connection to 10.10.2.27 timed out. (connect timeout=180)'))
02:43:38 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://192.168.86.26:1234/v1/chat/completions (model: qwen/qwen3-vl-8b)
02:45:53 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://192.168.86.26:1234/v1/chat/completions: HTTPConnectionPool(host='192.168.86.26', port=1234): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50aedf9e50>, 'Connection to 192.168.86.26 timed out. (connect timeout=180)'))
02:45:53 | ERROR    | vision_service:_try_all_endpoints:1028 | {} - All vision endpoints failed: ['http://10.10.0.33:6969/v1/chat/completions: Timeout', 'http://10.10.1.5:6969/v1/chat/completions: Connection error', 'http://10.10.2.27:6969/v1/chat/completions: Timeout', 'http://192.168.86.26:1234/v1/chat/completions: Timeout']
02:45:53 | ERROR    | vision_service:analyze_images:1194 | {} - All vision endpoints failed for multi-image request (5 images)
02:46:11 | INFO     | ingestion_service:_download_and_analyze_document:906 | {} - Analyzing PDF: unknown_MTG_MORTGAGE_2015496454.pdf
02:46:12 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.0.33:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
02:48:27 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.0.33:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.0.33', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50aedfab40>, 'Connection to 10.10.0.33 timed out. (connect timeout=180)'))
02:48:27 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.1.5:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
02:48:27 | WARNING  | vision_service:_try_all_endpoints:1021 | {} - Connection error on endpoint http://10.10.1.5:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.1.5', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by NewConnectionError('<urllib3.connection.HTTPConnection object at 0x7e50aedf97f0>: Failed to establish a new connection: [Errno 111] Connection refused'))
```

## Error 19 (log line 3937)

```text
02:48:27 | WARNING  | vision_service:_try_all_endpoints:1021 | {} - Connection error on endpoint http://10.10.1.5:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.1.5', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by NewConnectionError('<urllib3.connection.HTTPConnection object at 0x7e50aedf97f0>: Failed to establish a new connection: [Errno 111] Connection refused'))
02:48:27 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.2.27:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
02:50:42 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.2.27:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.2.27', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af6b3ce0>, 'Connection to 10.10.2.27 timed out. (connect timeout=180)'))
02:50:42 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://192.168.86.26:1234/v1/chat/completions (model: qwen/qwen3-vl-8b)
02:52:57 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://192.168.86.26:1234/v1/chat/completions: HTTPConnectionPool(host='192.168.86.26', port=1234): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af6b2f90>, 'Connection to 192.168.86.26 timed out. (connect timeout=180)'))
02:52:57 | ERROR    | vision_service:_try_all_endpoints:1028 | {} - All vision endpoints failed: ['http://10.10.0.33:6969/v1/chat/completions: Timeout', 'http://10.10.1.5:6969/v1/chat/completions: Connection error', 'http://10.10.2.27:6969/v1/chat/completions: Timeout', 'http://192.168.86.26:1234/v1/chat/completions: Timeout']
02:52:57 | ERROR    | vision_service:analyze_images:1194 | {} - All vision endpoints failed for multi-image request (5 images)
02:53:22 | INFO     | ingestion_service:_download_and_analyze_document:906 | {} - Analyzing PDF: unknown_MTGNIT_MORTGAGE_NO_INTANGIBLE_TAXES_2020464215.pdf
02:53:23 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.0.33:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
02:53:43 | SUCCESS  | ingestion_service:_download_and_analyze_document:914 | {} - Extracted data from (MTGNIT) MORTGAGE NO INTANGIBLE TAXES: 2020464215
02:53:43 | INFO     | ingestion_service:_update_parties_from_extraction:966 | {} -   Updated party2 from vLLM: Navy Federal Credit Union, A Corporation
```

## Error 20 (log line 3938)

```text
02:48:27 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.2.27:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
02:50:42 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://10.10.2.27:6969/v1/chat/completions: HTTPConnectionPool(host='10.10.2.27', port=6969): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af6b3ce0>, 'Connection to 10.10.2.27 timed out. (connect timeout=180)'))
02:50:42 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://192.168.86.26:1234/v1/chat/completions (model: qwen/qwen3-vl-8b)
02:52:57 | WARNING  | vision_service:_try_all_endpoints:1017 | {} - Timeout on endpoint http://192.168.86.26:1234/v1/chat/completions: HTTPConnectionPool(host='192.168.86.26', port=1234): Max retries exceeded with url: /v1/chat/completions (Caused by ConnectTimeoutError(<urllib3.connection.HTTPConnection object at 0x7e50af6b2f90>, 'Connection to 192.168.86.26 timed out. (connect timeout=180)'))
02:52:57 | ERROR    | vision_service:_try_all_endpoints:1028 | {} - All vision endpoints failed: ['http://10.10.0.33:6969/v1/chat/completions: Timeout', 'http://10.10.1.5:6969/v1/chat/completions: Connection error', 'http://10.10.2.27:6969/v1/chat/completions: Timeout', 'http://192.168.86.26:1234/v1/chat/completions: Timeout']
02:52:57 | ERROR    | vision_service:analyze_images:1194 | {} - All vision endpoints failed for multi-image request (5 images)
02:53:22 | INFO     | ingestion_service:_download_and_analyze_document:906 | {} - Analyzing PDF: unknown_MTGNIT_MORTGAGE_NO_INTANGIBLE_TAXES_2020464215.pdf
02:53:23 | INFO     | vision_service:_try_all_endpoints:1006 | {} - Trying vision endpoint: http://10.10.0.33:6969/v1/chat/completions (model: Qwen/Qwen3-VL-8B-Instruct)
02:53:43 | SUCCESS  | ingestion_service:_download_and_analyze_document:914 | {} - Extracted data from (MTGNIT) MORTGAGE NO INTANGIBLE TAXES: 2020464215
02:53:43 | INFO     | ingestion_service:_update_parties_from_extraction:966 | {} -   Updated party2 from vLLM: Navy Federal Credit Union, A Corporation
02:53:55 | INFO     | ingestion_service:_download_and_analyze_document:906 | {} - Analyzing PDF: unknown_AFF_AFFIDAVIT_2017264506.pdf
```

