/**
 * Cluster Actor Dashboard - 核心逻辑
 * 整合集群、Actor和KV Store模块的数据获取、缓存、渲染功能
 */

// ==================== 工具函数 ====================

/**
 * 安全地获取嵌套对象属性
 */
function safeGet(obj, path, defaultValue = '--') {
    return path.split('.').reduce((current, key) => {
        return current && current[key] !== undefined ? current[key] : defaultValue;
    }, obj);
}

/**
 * 格式化时间
 */
function formatTime(dateString) {
    if (!dateString) return '--';
    const date = new Date(dateString);
    return date.toLocaleTimeString('zh-CN', { hour12: false });
}

/**
 * 格式化日期时间
 */
function formatDateTime(dateString) {
    if (!dateString) return '--';
    const date = new Date(dateString);
    return date.toLocaleString('zh-CN', { hour12: false });
}

/**
 * 防抖函数
 */
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

/**
 * 转义HTML特殊字符，防止XSS
 */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// ==================== 缓存管理器 ====================

class CacheManager {
    constructor(ttl = 30000) {
        this.cache = new Map();
        this.ttl = ttl;
    }

    get(key) {
        const entry = this.cache.get(key);
        if (!entry) return null;
        
        if (Date.now() - entry.timestamp > this.ttl) {
            this.cache.delete(key);
            return null;
        }
        
        return entry.data;
    }

    set(key, data) {
        this.cache.set(key, {
            data,
            timestamp: Date.now()
        });
    }

    invalidate(key) {
        this.cache.delete(key);
    }

    clear() {
        this.cache.clear();
    }
}

// ==================== API 客户端 ====================

class ApiClient {
    constructor(baseURL = '') {
        this.baseURL = baseURL;
        this.timeout = 5000;
    }

    async get(endpoint) {
        return this.request(endpoint, { method: 'GET' });
    }

    async request(endpoint, options = {}) {
        const url = `${this.baseURL}${endpoint}`;
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), this.timeout);

        try {
            const response = await fetch(url, {
                ...options,
                signal: controller.signal,
                headers: {
                    'Content-Type': 'application/json',
                    ...options.headers
                }
            });

            clearTimeout(timeoutId);

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            return await response.json();
        } catch (error) {
            clearTimeout(timeoutId);
            if (error.name === 'AbortError') {
                throw new Error('请求超时');
            }
            throw error;
        }
    }
}

// ==================== 刷新控制器 ====================

class RefreshController {
    constructor(interval = 30000) {
        this.interval = interval;
        this.timer = null;
        this.callbacks = [];
        this.isAutoRefresh = true;
    }

    start() {
        this.stop();
        if (this.isAutoRefresh) {
            this.timer = setInterval(() => this.trigger(), this.interval);
        }
    }

    stop() {
        if (this.timer) {
            clearInterval(this.timer);
            this.timer = null;
        }
    }

    refreshNow() {
        return this.trigger();
    }

    setAutoRefresh(enabled) {
        this.isAutoRefresh = enabled;
        if (enabled) {
            this.start();
        } else {
            this.stop();
        }
    }

    onRefresh(callback) {
        this.callbacks.push(callback);
    }

    async trigger() {
        for (const callback of this.callbacks) {
            try {
                await callback();
            } catch (error) {
                console.error('刷新回调执行失败:', error);
            }
        }
        this.updateLastUpdateTime();
    }

    updateLastUpdateTime() {
        const element = document.getElementById('lastUpdate');
        if (element) {
            element.textContent = new Date().toLocaleTimeString('zh-CN', { hour12: false });
        }
    }
}

// ==================== 集群模块 ====================

class ClusterModule {
    constructor(apiClient) {
        this.api = apiClient;
        this.members = [];
        this.filteredMembers = [];
        this.sortField = null;
        this.sortDirection = 'asc';
        this.memberFilter = 'all';
    }

    async loadData() {
        const [infoData, statusData, membersData] = await Promise.all([
            this.api.get('/api/cluster/info').catch(() => null),
            this.api.get('/api/cluster/status').catch(() => null),
            this.api.get('/api/cluster/members').catch(() => null)
        ]);

        if (infoData && infoData.code === 0) {
            this.renderInfo(infoData.data);
        }

        if (statusData && statusData.code === 0) {
            this.renderStatus(statusData.data);
        }

        if (membersData && membersData.code === 0) {
            this.members = membersData.data || [];
            this.applyFilters();
        }

        this.removeSkeleton('clusterInfo');
    }

    renderInfo(data) {
        document.getElementById('clusterName').textContent = escapeHtml(safeGet(data, 'cluster_name'));
        document.getElementById('nodeName').textContent = escapeHtml(safeGet(data, 'node_name'));
        document.getElementById('nodeAddress').textContent = escapeHtml(safeGet(data, 'node_address'));
        document.getElementById('memberCount').textContent = safeGet(data, 'member_count', 0);
        document.getElementById('isRunning').textContent = data.is_running ? '运行中' : '已停止';
        document.getElementById('startTime').textContent = formatDateTime(safeGet(data, 'start_time'));
    }

    renderStatus(data) {
        const statusEl = document.getElementById('clusterStatus');
        const dot = statusEl.querySelector('.status-dot');
        const text = statusEl.querySelector('.status-text');

        const status = safeGet(data, 'status', 'unknown');
        dot.className = 'status-dot ' + status;
        text.textContent = status === 'healthy' ? '健康' : status === 'degraded' ? '降级' : status === 'unhealthy' ? '异常' : status;
    }

    applyFilters() {
        if (this.memberFilter === 'alive') {
            this.filteredMembers = this.members.filter(m => m.alive);
        } else if (this.memberFilter === 'dead') {
            this.filteredMembers = this.members.filter(m => !m.alive);
        } else {
            this.filteredMembers = [...this.members];
        }

        if (this.sortField) {
            this.filteredMembers.sort((a, b) => {
                let aVal = a[this.sortField];
                let bVal = b[this.sortField];
                
                if (typeof aVal === 'boolean') {
                    aVal = aVal ? 1 : 0;
                    bVal = bVal ? 1 : 0;
                }
                
                if (aVal < bVal) return this.sortDirection === 'asc' ? -1 : 1;
                if (aVal > bVal) return this.sortDirection === 'asc' ? 1 : -1;
                return 0;
            });
        }

        this.renderMembers();
    }

    renderMembers() {
        const tbody = document.getElementById('memberTableBody');
        const emptyState = document.getElementById('memberEmpty');

        if (this.filteredMembers.length === 0) {
            tbody.innerHTML = '';
            emptyState.style.display = 'flex';
            return;
        }

        emptyState.style.display = 'none';
        
        tbody.innerHTML = this.filteredMembers.map(member => `
            <tr>
                <td><code>${escapeHtml(safeGet(member, 'node_name', 'unknown'))}</code></td>
                <td><code>${escapeHtml(safeGet(member, 'address', '--'))}</code></td>
                <td>
                    <span class="status-badge ${member.alive ? 'alive' : 'dead'}">
                        ${member.alive ? '在线' : '离线'}
                    </span>
                </td>
            </tr>
        `).join('');
    }

    setFilter(filter) {
        this.memberFilter = filter;
        document.querySelectorAll('.member-filters .filter-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.filter === filter);
        });
        this.applyFilters();
    }

    setSort(field) {
        if (this.sortField === field) {
            this.sortDirection = this.sortDirection === 'asc' ? 'desc' : 'asc';
        } else {
            this.sortField = field;
            this.sortDirection = 'asc';
        }

        document.querySelectorAll('.data-table th').forEach(th => {
            th.classList.toggle('sorted', th.dataset.sort === field);
        });

        this.applyFilters();
    }

    removeSkeleton(id) {
        const el = document.getElementById(id);
        if (el) el.classList.add('loaded');
    }
}

// ==================== Actor 模块 ====================

class ActorModule {
    constructor(apiClient) {
        this.api = apiClient;
        this.kinds = [];
        this.instances = [];
        this.filteredInstances = [];
        this.currentPage = 1;
        this.pageSize = 20;
        this.kindPieChart = null;
    }

    async loadData() {
        const [kindsData, instancesData] = await Promise.all([
            this.api.get('/api/actor/kinds').catch(() => null),
            this.api.get('/api/actor/instances').catch(() => null)
        ]);

        if (kindsData && kindsData.code === 0) {
            this.kinds = kindsData.data || [];
            this.renderKinds();
            document.getElementById('kindCount').textContent = this.kinds.length;
        }

        if (instancesData && instancesData.code === 0) {
            this.instances = instancesData.data || [];
            document.getElementById('instanceCount').textContent = this.instances.length;
            this.populateFilters();
            this.applyFilters();
        }

        this.removeSkeleton('kindsContainer');
        this.removeSkeleton('instancesContainer');
    }

    renderKinds() {
        const kindsList = document.getElementById('kindsList');
        const instanceCounts = this.getInstanceCountsByKind();

        kindsList.innerHTML = this.kinds.map(kind => `
            <div class="kind-item">
                <span class="kind-name">${escapeHtml(safeGet(kind, 'kind_name', 'unknown'))}</span>
                <span class="kind-count">${instanceCounts[safeGet(kind, 'kind_name', '')] || 0}</span>
            </div>
        `).join('');

        this.renderPieChart(instanceCounts);
    }

    getInstanceCountsByKind() {
        const counts = {};
        this.instances.forEach(inst => {
            const kindName = safeGet(inst, 'kind_name', '');
            counts[kindName] = (counts[kindName] || 0) + 1;
        });
        return counts;
    }

    renderPieChart(instanceCounts) {
        const ctx = document.getElementById('kindPieChart').getContext('2d');

        if (this.kindPieChart) {
            this.kindPieChart.destroy();
        }

        const labels = this.kinds.map(k => safeGet(k, 'kind_name', 'unknown'));
        const data = labels.map(l => instanceCounts[l] || 0);
        
        const colors = [
            '#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6',
            '#06b6d4', '#ec4899', '#84cc16', '#f97316', '#6366f1'
        ];

        if (data.length === 0 || data.every(d => d === 0)) {
            this.kindPieChart = new Chart(ctx, {
                type: 'doughnut',
                data: {
                    labels: ['无数据'],
                    datasets: [{
                        data: [1],
                        backgroundColor: ['#334155'],
                        borderWidth: 0
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            display: true,
                            position: 'bottom',
                            labels: { color: '#94a3b8', font: { size: 12 } }
                        }
                    }
                }
            });
            return;
        }

        this.kindPieChart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels,
                datasets: [{
                    data,
                    backgroundColor: colors.slice(0, labels.length),
                    borderWidth: 2,
                    borderColor: '#1e293b'
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: true,
                        position: 'bottom',
                        labels: {
                            color: '#94a3b8',
                            font: { size: 12 },
                            padding: 12,
                            usePointStyle: true,
                            pointStyleWidth: 10
                        }
                    },
                    tooltip: {
                        backgroundColor: '#0f172a',
                        titleColor: '#f8fafc',
                        bodyColor: '#94a3b8',
                        borderColor: '#334155',
                        borderWidth: 1,
                        cornerRadius: 8,
                        padding: 12,
                        callbacks: {
                            label: function(context) {
                                const total = context.dataset.data.reduce((a, b) => a + b, 0);
                                const percentage = ((context.parsed / total) * 100).toFixed(1);
                                return `${context.label}: ${context.parsed} (${percentage}%)`;
                            }
                        }
                    }
                }
            }
        });
    }

    populateFilters() {
        const kindFilter = document.getElementById('kindFilter');
        const nodeFilter = document.getElementById('nodeFilter');

        const kinds = [...new Set(this.instances.map(i => safeGet(i, 'kind_name', '')))].sort();
        const nodes = [...new Set(this.instances.map(i => safeGet(i, 'node_name', '')))].sort();

        kindFilter.innerHTML = '<option value="">全部类型</option>' + 
            kinds.map(k => `<option value="${escapeHtml(k)}">${escapeHtml(k)}</option>`).join('');

        nodeFilter.innerHTML = '<option value="">全部节点</option>' + 
            nodes.map(n => `<option value="${escapeHtml(n)}">${escapeHtml(n)}</option>`).join('');
    }

    applyFilters() {
        const kindFilter = document.getElementById('kindFilter').value;
        const nodeFilter = document.getElementById('nodeFilter').value;
        const searchTerm = document.getElementById('instanceSearch').value.toLowerCase();

        this.filteredInstances = this.instances.filter(inst => {
            const kind = safeGet(inst, 'kind_name', '');
            const node = safeGet(inst, 'node_name', '');
            const identity = safeGet(inst, 'identity', '').toLowerCase();

            if (kindFilter && kind !== kindFilter) return false;
            if (nodeFilter && node !== nodeFilter) return false;
            if (searchTerm && !identity.includes(searchTerm)) return false;

            return true;
        });

        this.currentPage = 1;
        this.renderInstances();
    }

    renderInstances() {
        const tbody = document.getElementById('instanceTableBody');
        const emptyState = document.getElementById('instanceEmpty');
        const pagination = document.getElementById('instancePagination');

        if (this.filteredInstances.length === 0) {
            tbody.innerHTML = '';
            emptyState.style.display = 'flex';
            pagination.style.display = 'none';
            return;
        }

        emptyState.style.display = 'none';
        pagination.style.display = 'flex';

        const totalPages = Math.ceil(this.filteredInstances.length / this.pageSize);
        if (this.currentPage > totalPages) this.currentPage = totalPages;

        const start = (this.currentPage - 1) * this.pageSize;
        const end = start + this.pageSize;
        const pageData = this.filteredInstances.slice(start, end);

        tbody.innerHTML = pageData.map(inst => `
            <tr>
                <td><code>${escapeHtml(safeGet(inst, 'kind_name', '--'))}</code></td>
                <td><code>${escapeHtml(safeGet(inst, 'identity', '--'))}</code></td>
                <td>${escapeHtml(safeGet(inst, 'node_name', '--'))}</td>
                <td><code>${escapeHtml(safeGet(inst, 'node_address', '--'))}</code></td>
            </tr>
        `).join('');

        document.getElementById('pageInfo').textContent = `第 ${this.currentPage} 页 / 共 ${totalPages} 页`;
        document.getElementById('prevPage').disabled = this.currentPage <= 1;
        document.getElementById('nextPage').disabled = this.currentPage >= totalPages;
    }

    goToPage(direction) {
        const totalPages = Math.ceil(this.filteredInstances.length / this.pageSize);
        this.currentPage = Math.max(1, Math.min(totalPages, this.currentPage + direction));
        this.renderInstances();
    }

    removeSkeleton(id) {
        const el = document.getElementById(id);
        if (el) el.classList.add('loaded');
    }
}

// ==================== KV Store 模块 ====================

class KVStoreModule {
    constructor(apiClient) {
        this.api = apiClient;
    }

    async loadData() {
        try {
            const statusData = await this.api.get('/api/kvstore/status');
            
            if (statusData && statusData.code === 0) {
                this.renderStatus(statusData.data);
            } else {
                this.renderUnavailable();
            }
        } catch (error) {
            console.warn('KV Store API未实现或不可用:', error.message);
            this.renderUnavailable();
        }

        this.removeSkeleton('kvstoreInfo');
    }

    renderStatus(data) {
        document.getElementById('kvCount').textContent = safeGet(data, 'key_count', 0);
        document.getElementById('kvLastUpdate').textContent = formatDateTime(safeGet(data, 'last_update'));
        document.getElementById('kvHealth').textContent = this.getHealthText(safeGet(data, 'health', 'unknown'));
        
        const usage = safeGet(data, 'usage_percent', 0);
        document.getElementById('kvUsage').textContent = `${usage}%`;
        document.getElementById('usageFill').style.width = `${usage}%`;
        document.getElementById('usagePercent').textContent = `${usage}%`;

        const statusEl = document.getElementById('kvstoreStatus');
        const dot = statusEl.querySelector('.status-dot');
        const text = statusEl.querySelector('.status-text');
        
        const health = safeGet(data, 'health', 'unknown');
        dot.className = 'status-dot ' + health;
        text.textContent = this.getHealthText(health);
    }

    renderUnavailable() {
        document.getElementById('kvCount').textContent = '--';
        document.getElementById('kvLastUpdate').textContent = '--';
        document.getElementById('kvHealth').textContent = '服务未就绪';
        document.getElementById('kvUsage').textContent = '--';
        document.getElementById('usageFill').style.width = '0%';
        document.getElementById('usagePercent').textContent = '--';

        const statusEl = document.getElementById('kvstoreStatus');
        const dot = statusEl.querySelector('.status-dot');
        const text = statusEl.querySelector('.status-text');
        
        dot.className = 'status-dot unhealthy';
        text.textContent = '未实现';
    }

    getHealthText(health) {
        switch (health) {
            case 'healthy': return '健康';
            case 'warning': return '警告';
            case 'unhealthy': return '异常';
            default: return health;
        }
    }

    removeSkeleton(id) {
        const el = document.getElementById(id);
        if (el) el.classList.add('loaded');
    }
}

// ==================== Dashboard 主控制器 ====================

class Dashboard {
    constructor() {
        this.api = new ApiClient();
        this.cache = new CacheManager(30000);
        this.refreshController = new RefreshController(30000);
        
        this.clusterModule = new ClusterModule(this.api);
        this.actorModule = new ActorModule(this.api);
        this.kvstoreModule = new KVStoreModule(this.api);

        this.init();
    }

    init() {
        this.bindEvents();
        this.loadData();
        this.refreshController.onRefresh(() => this.loadData());
        this.refreshController.start();
    }

    async loadData() {
        const refreshBtn = document.getElementById('refreshBtn');
        refreshBtn.classList.add('loading');
        this.hideError();

        try {
            await Promise.all([
                this.clusterModule.loadData(),
                this.actorModule.loadData(),
                this.kvstoreModule.loadData()
            ]);
        } catch (error) {
            console.error('数据加载失败:', error);
            this.showError('数据加载失败: ' + error.message);
        } finally {
            refreshBtn.classList.remove('loading');
        }
    }

    bindEvents() {
        document.getElementById('refreshBtn').addEventListener('click', () => {
            this.cache.clear();
            this.refreshController.refreshNow();
        });

        document.getElementById('autoRefresh').addEventListener('change', (e) => {
            this.refreshController.setAutoRefresh(e.target.checked);
        });

        document.querySelectorAll('.member-filters .filter-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                this.clusterModule.setFilter(btn.dataset.filter);
            });
        });

        document.querySelectorAll('.data-table th[data-sort]').forEach(th => {
            th.addEventListener('click', () => {
                this.clusterModule.setSort(th.dataset.sort);
            });
        });

        document.getElementById('kindFilter').addEventListener('change', () => {
            this.actorModule.applyFilters();
        });

        document.getElementById('nodeFilter').addEventListener('change', () => {
            this.actorModule.applyFilters();
        });

        document.getElementById('instanceSearch').addEventListener('input', debounce(() => {
            this.actorModule.applyFilters();
        }, 300));

        document.getElementById('prevPage').addEventListener('click', () => {
            this.actorModule.goToPage(-1);
        });

        document.getElementById('nextPage').addEventListener('click', () => {
            this.actorModule.goToPage(1);
        });

        document.getElementById('retryBtn').addEventListener('click', () => {
            this.cache.clear();
            this.loadData();
        });

        const backToTop = document.getElementById('backToTop');
        window.addEventListener('scroll', () => {
            backToTop.classList.toggle('visible', window.scrollY > 400);
        });

        backToTop.addEventListener('click', () => {
            window.scrollTo({ top: 0, behavior: 'smooth' });
        });
    }

    showError(message) {
        const errorEl = document.getElementById('globalError');
        document.getElementById('errorMessage').textContent = message;
        errorEl.classList.remove('hidden');
    }

    hideError() {
        document.getElementById('globalError').classList.add('hidden');
    }
}

// ==================== 初始化 ====================

document.addEventListener('DOMContentLoaded', () => {
    window.dashboard = new Dashboard();
});
