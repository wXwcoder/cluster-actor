# cluster-actor

## GET /api/cluster/info

GET /:8181/api/cluster/info

### 请求参数

> 返回示例

> 200 Response

```json
{"code":0,"data":{"cluster_name":"cluster-actor-demo","is_running":true,"member_count":1,"node_address":"127.0.0.1","node_name":"node-1","start_time":"2026-05-27T10:37:04+08:00"}}
```

### 返回结果

|状态码|状态码含义|说明|数据模型|
|---|---|---|---|
|200|[OK]|none|Inline|

### 返回数据结构

## GET /api/cluster/members

GET /:8181/api/cluster/members

### 请求参数


> 返回示例

> 200 Response

```json
{"code":0,"data":{"cluster_name":"cluster-actor-demo","is_running":true,"member_count":1,"node_address":"127.0.0.1","node_name":"node-1","start_time":"2026-05-27T10:37:04+08:00"}}
```

### 返回结果

|状态码|状态码含义|说明|数据模型|
|---|---|---|---|
|200|[OK]|none|Inline|

### 返回数据结构

## GET /api/actor/kinds

GET /:8181/api/actor/kinds

### 请求参数

> 返回示例

> 200 Response

```json
{"code":0,"data":{"cluster_name":"cluster-actor-demo","is_running":true,"member_count":1,"node_address":"127.0.0.1","node_name":"node-1","start_time":"2026-05-27T10:37:04+08:00"}}
```

### 返回结果

|状态码|状态码含义|说明|数据模型|
|---|---|---|---|
|200|[OK]|none|Inline|

### 返回数据结构

## GET /api/actor/instances

GET /:8181/api/actor/instances

### 请求参数

> 返回示例

> 200 Response

```json
{"code":0,"data":{"cluster_name":"cluster-actor-demo","is_running":true,"member_count":1,"node_address":"127.0.0.1","node_name":"node-1","start_time":"2026-05-27T10:37:04+08:00"}}
```

### 返回结果

|状态码|状态码含义|说明|数据模型|
|---|---|---|---|
|200|[OK]|none|Inline|

### 返回数据结构

## GET /api/actor/call/hello/user-1

GET /:8181/api/actor/call/hello/user-1

### 请求参数

|name|query|string| 否 ||none|
 
> 返回示例

> 200 Response

```json
{"code":0,"data":{"cluster_name":"cluster-actor-demo","is_running":true,"member_count":1,"node_address":"127.0.0.1","node_name":"node-1","start_time":"2026-05-27T10:37:04+08:00"}}
```

### 返回结果

|状态码|状态码含义|说明|数据模型|
|---|---|---|---|
|200|[OK]|none|Inline|

### 返回数据结构

## GET /api/actor/call/hello/user-2

GET /:8181/api/actor/call/hello/user-2

### 请求参数
  
|name|query|string| 否 ||none|
 

> 返回示例

> 200 Response

```json
{"code":0,"data":{"cluster_name":"cluster-actor-demo","is_running":true,"member_count":1,"node_address":"127.0.0.1","node_name":"node-1","start_time":"2026-05-27T10:37:04+08:00"}}
```

### 返回结果

|状态码|状态码含义|说明|数据模型|
|---|---|---|---|
|200|[OK]|none|Inline|

### 返回数据结构

## GET /api/actor/call/hello/user-3

GET /:8181/api/actor/call/hello/user-3

### 请求参数

|name|query|string| 否 ||none|

> 返回示例

> 200 Response

```json
{"code":0,"data":{"cluster_name":"cluster-actor-demo","is_running":true,"member_count":1,"node_address":"127.0.0.1","node_name":"node-1","start_time":"2026-05-27T10:37:04+08:00"}}
```

### 返回结果

|状态码|状态码含义|说明|数据模型|
|---|---|---|---|
|200|[OK]|none|Inline|

### 返回数据结构

## GET /api/actor/call/hello/user-4

GET /:8181/api/actor/call/hello/user-6

### 请求参数

|name|query|string| 否 ||none|

> 返回示例

> 200 Response

```json
{"code":0,"data":{"cluster_name":"cluster-actor-demo","is_running":true,"member_count":1,"node_address":"127.0.0.1","node_name":"node-1","start_time":"2026-05-27T10:37:04+08:00"}}
```

### 返回结果

|状态码|状态码含义|说明|数据模型|
|---|---|---|---|
|200|[OK]|none|Inline|

### 返回数据结构

## GET /api/actor/local-instances

GET /:8181/api/actor/local-instances

### 请求参数

> 返回示例

> 200 Response

```json
{"code":0,"data":{"cluster_name":"cluster-actor-demo","is_running":true,"member_count":1,"node_address":"127.0.0.1","node_name":"node-1","start_time":"2026-05-27T10:37:04+08:00"}}
```

### 返回结果

|状态码|状态码含义|说明|数据模型|
|---|---|---|---|
|200|[OK]|none|Inline|

### 返回数据结构

## GET /api/actor/call/user/user-1

GET /:8181/api/actor/call/user/user-1

### 请求参数

|name|query|string| 否 ||none|

> 返回示例

> 200 Response

```json
{"code":0,"data":{"cluster_name":"cluster-actor-demo","is_running":true,"member_count":1,"node_address":"127.0.0.1","node_name":"node-1","start_time":"2026-05-27T10:37:04+08:00"}}
```

### 返回结果

|状态码|状态码含义|说明|数据模型|
|---|---|---|---|
|200|[OK]|none|Inline|