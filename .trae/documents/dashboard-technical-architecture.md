# Cluster Actor Dashboard - 技术架构文档

## 1. 技术栈选择

### 1.1 前端框架
- **原生 HTML/CSS/JavaScript**：无需构建工具，直接部署到 Go 静态文件服务
- 理由：项目已有 web 目录结构，router.go 中配置了静态文件服务

### 1.2 数据可视化
- **Chart.js**：轻量级图表库，支持 CDN 引入
- 理由：简单易用，支持多种图表类型，性能良好

### 1.3 样式方案
- **CSS 自定义属性 (CSS Variables)**：主题管理和样式复用
- **CSS Grid + Flexbox**：响应式布局
- **CSS 动画**：过渡效果和微交互

### 1.4 图标库
- **Lucide Icons**：轻量级 SVG 图标库
- 理由：现代设计风格，按需引入

## 2. 文件结构设计

```
web/
├── dashboard.html          # Dashboard 主页面
├── static/
│   ├── css/
│   │   ├── style.css       # 现有聊天室样式
│   │   └── dashboard.css   # Dashboard 专用样式
│   └── js/
│       ├── chat.js         # 现有聊天室脚本
│       └── dashboard.js    # Dashboard 核心逻辑
└── index.html              # 现有登录页（保持不变）
```

## 3. 模块架构

### 3.1 整体架构

```
┌─────────────────────────────────────────────────────┐
│                    Dashboard                        │
├─────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │
│  │  Cluster    │  │   Actor     │  │  KV Store   │  │
│  │   Module    │  │   Module    │  │   Module    │  │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  │
│         │                │                │         │
├─────────┴────────────────┴────────────────┴─────────┤
│                   Data Layer                        │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐    │
│  │ API Client │  │   Cache    │  │  Refresh   │    │
│  └────────────┘  └────────────┘  └────────────┘    │
├─────────────────────────────────────────────────────┤
│                  Render Layer                       │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐    │
│  │   Charts   │  │   Tables   │  │   Cards    │    │
│  └────────────┘  └────────────┘  └────────────┘    │
└─────────────────────────────────────────────────────┘
```

### 3.2 数据层设计

#### 3.2.1 API 客户端 (ApiClient)
```javascript
class ApiClient {
  // 基础配置
  - baseURL: string
  - timeout: number
  
  // 方法
  + get(endpoint): Promise<Response>
  + request(endpoint, options): Promise<Response>
}
```

#### 3.2.2 缓存管理器 (CacheManager)
```javascript
class CacheManager {
  // 内部状态
  - cache: Map<string, CacheEntry>
  - ttl: number (默认 30s)
  
  // 方法
  + get(key): any | null
  + set(key, value): void
  + has(key): boolean
  + invalidate(key): void
  + clear(): void
}
```

#### 3.2.3 刷新控制器 (RefreshController)
```javascript
class RefreshController {
  // 内部状态
  - interval: number (默认 30s)
  - timer: NodeJS.Timeout
  - callbacks: Array<Function>
  
  // 方法
  + start(): void
  + stop(): void
  + refreshNow(): Promise<void>
  + onRefresh(callback): void
}
```

### 3.3 渲染层设计

#### 3.3.1 集群模块 (ClusterModule)
```javascript
class ClusterModule {
  // DOM 引用
  - elements: Object
  
  // 数据
  - info: Object
  - status: Object
  - members: Array
  
  // 方法
  + init(): void
  + renderInfo(data): void
  + renderStatus(data): void
  + renderMembers(data): void
  + updateStatusIndicator(status): void
}
```

#### 3.3.2 Actor 模块 (ActorModule)
```javascript
class ActorModule {
  // DOM 引用
  - elements: Object
  
  // 图表实例
  - kindPieChart: Chart
  - instanceBarChart: Chart
  
  // 数据
  - kinds: Array
  - instances: Array
  
  // 方法
  + init(): void
  + renderKinds(data): void
  + renderInstances(data): void
  + renderCharts(): void
  + setupFilters(): void
}
```

#### 3.3.3 KV Store 模块 (KVStoreModule)
```javascript
class KVStoreModule {
  // DOM 引用
  - elements: Object
  
  // 数据
  - status: Object
  
  // 方法
  + init(): void
  + renderStatus(data): void
  + updateHealthIndicator(health): void
}
```

## 4. 数据流设计

```
用户操作 / 定时器触发
        ↓
┌───────────────┐
│ RefreshController │
└───────┬───────┘
        ↓ 触发刷新回调
┌───────────────┐
│   ApiClient     │ ← → CacheManager
│   请求 API      │     缓存管理
└───────┬───────┘
        ↓ 返回数据
┌───────────────┐
│  Module.render() │
│  更新 DOM 和图表  │
└───────┬───────┘
        ↓
用户看到更新后的界面
```

## 5. API 接口规范

### 5.1 统一响应格式
```json
{
  "code": 0,
  "data": {},
  "count": 0,
  "message": ""
}
```

### 5.2 接口详情

| 接口 | 方法 | 响应结构 |
|------|------|----------|
| `/api/cluster/info` | GET | `{cluster_name, node_name, node_address, member_count, is_running, start_time}` |
| `/api/cluster/status` | GET | `{status, member_count, members[], timestamp}` |
| `/api/cluster/members` | GET | `[{node_name, address, alive, ...}]` |
| `/api/actor/kinds` | GET | `{count, data: [{kind_name}]}` |
| `/api/actor/instances` | GET | `{count, data: [{kind_name, identity, node_name, node_address}]}` |
| `/api/kvstore/status` | GET | 待实现 |

## 6. 响应式设计方案

### 6.1 断点定义
```css
--breakpoint-sm: 640px;   /* 手机横屏 */
--breakpoint-md: 768px;   /* 平板竖屏 */
--breakpoint-lg: 1024px;  /* 平板横屏 */
--breakpoint-xl: 1280px;  /* 桌面端 */
```

### 6.2 布局策略
- **≥1280px**：三列网格布局
- **1024px-1279px**：两列 + 一列堆叠
- **768px-1023px**：两列布局
- **<768px**：单列布局，可折叠面板

## 7. 性能优化策略

### 7.1 数据请求优化
- 并发请求：使用 `Promise.all` 并行获取数据
- 请求缓存：30 秒 TTL，避免重复请求
- 错误重试：指数退避策略，最多 3 次

### 7.2 渲染优化
- 防抖处理：搜索和筛选操作添加 300ms 防抖
- 虚拟滚动：大数据量列表使用虚拟滚动
- 图表优化：数据更新时复用图表实例，避免重建

### 7.3 资源优化
- CDN 引入第三方库（Chart.js、Lucide）
- CSS 按需加载，避免全局样式冲突
- 图片资源使用 WebP 格式

## 8. 错误处理

### 8.1 网络错误
- 显示错误提示横幅
- 提供重试按钮
- 保持上次成功的数据状态

### 8.2 数据解析错误
- 捕获 JSON 解析异常
- 记录错误日志
- 显示降级界面

### 8.3 图表渲染错误
- 捕获 Chart.js 异常
- 显示占位符文本
- 不影响其他模块渲染

## 9. 安全考虑

- XSS 防护：对用户输入进行转义
- CSRF 防护：使用同源策略
- 数据脱敏：敏感信息不在前端明文显示

## 10. 部署方案

### 10.1 静态文件服务
- Go 路由器已配置：`r.engine.Static("/static", "./web/static")`
- HTML 文件直接放在 `web/` 目录
- 访问路径：`http://host:port/dashboard.html`

### 10.2 无需构建
- 纯前端实现，无需 Node.js 构建工具
- 直接部署 HTML/CSS/JS 文件即可
