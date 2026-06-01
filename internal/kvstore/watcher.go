package kvstore

import (
	"log"
	"time"

	consulapi "github.com/hashicorp/consul/api"
)

// Watcher 前缀监听器
type Watcher struct {
	Prefix    string        // 监听前缀
	stopCh    chan struct{} // 停止信号
	lastIndex uint64        // 上次查询索引
	kv        *KVStore      // 关联的KVStore
}

// AddWatcher 添加前缀监听器
func (k *KVStore) AddWatcher(prefix string) error {
	k.watchersMu.Lock()
	defer k.watchersMu.Unlock()

	if _, exists := k.watchers[prefix]; exists {
		return nil
	}

	watcher := &Watcher{
		Prefix:    prefix,
		stopCh:    make(chan struct{}),
		lastIndex: 0,
		kv:        k,
	}

	k.watchers[prefix] = watcher

	if k.isRunning {
		go watcher.start()
	}

	log.Printf("KVStore.AddWatcher: prefix=%s", prefix)
	return nil
}

// RemoveWatcher 移除前缀监听器
func (k *KVStore) RemoveWatcher(prefix string) {
	k.watchersMu.Lock()
	defer k.watchersMu.Unlock()

	watcher, exists := k.watchers[prefix]
	if !exists {
		return
	}

	close(watcher.stopCh)
	delete(k.watchers, prefix)

	log.Printf("KVStore.RemoveWatcher: prefix=%s", prefix)
}

// start 启动Watch监听循环
func (w *Watcher) start() {
	log.Printf("Watcher.start: prefix=%s", w.Prefix)

	for {
		select {
		case <-w.stopCh:
			log.Printf("Watcher.stop: prefix=%s", w.Prefix)
			return
		default:
			err := w.watchOnce()
			if err != nil {
				log.Printf("Watcher.watchOnce错误: prefix=%s, err=%v", w.Prefix, err)
				time.Sleep(1 * time.Second)
			}
		}
	}
}

// watchOnce 执行一次Watch查询（阻塞直到变更或超时）
func (w *Watcher) watchOnce() error {
	opts := &consulapi.QueryOptions{
		WaitIndex: w.lastIndex,
		WaitTime:  5 * time.Minute,
	}

	pairs, meta, err := w.kv.client.KV().List(w.Prefix, opts)
	if err != nil {
		return err
	}

	if meta.LastIndex <= w.lastIndex {
		// 没有变更，超时返回
		return nil
	}

	w.lastIndex = meta.LastIndex
	w.kv.syncCache(w.Prefix, pairs)

	return nil
}

// syncCache 同步Consul KV到本地缓存
func (k *KVStore) syncCache(prefix string, pairs consulapi.KVPairs) {
	k.mu.Lock()
	defer k.mu.Unlock()

	// 构建新KV集合
	newKeys := make(map[string]bool)
	for _, pair := range pairs {
		newKeys[pair.Key] = true

		existing, exists := k.cache[pair.Key]
		now := time.Now()

		if !exists {
			// 新增KV
			k.cache[pair.Key] = &KVEntry{
				Key:         pair.Key,
				Value:       pair.Value,
				ModifyIndex: pair.ModifyIndex,
				CreatedAt:   now,
				UpdatedAt:   now,
			}
		} else if pair.ModifyIndex > existing.ModifyIndex {
			// 更新KV
			existing.Value = pair.Value
			existing.ModifyIndex = pair.ModifyIndex
			existing.UpdatedAt = now
		}

		log.Printf("KVStore updateCache: prefix=%s, key=%s, value=%s", prefix, pair.Key, string(pair.Value))
	}

	// 删除已不存在的KV
	for key := range k.cache {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			if !newKeys[key] {
				log.Printf("KVStore deleteCache: prefix=%s, key=%s, value=%s", prefix, key, string(k.cache[key].Value))
				delete(k.cache, key)
			}
		}
	}
	log.Printf("KVStore.syncCache: prefix=%s, total=%d", prefix, len(k.cache))
}

// Start 启动所有Watch监听
func (k *KVStore) Start() error {
	k.watchersMu.Lock()
	defer k.watchersMu.Unlock()

	if k.isRunning {
		return nil
	}

	k.isRunning = true
	k.stopCh = make(chan struct{})

	// 初始加载所有监听前缀的数据
	for prefix := range k.watchers {
		pairs, meta, err := k.client.KV().List(prefix, nil)
		if err != nil {
			log.Printf("KVStore.Start 初始加载失败: prefix=%s, err=%v", prefix, err)
			continue
		}

		k.watchers[prefix].lastIndex = meta.LastIndex
		k.syncCache(prefix, pairs)
	}

	// 启动所有Watch goroutine
	for _, watcher := range k.watchers {
		go watcher.start()
	}

	log.Printf("KVStore.Start: watchers=%d", len(k.watchers))
	return nil
}

// Stop 停止所有Watch监听
func (k *KVStore) Stop() error {
	k.watchersMu.Lock()
	defer k.watchersMu.Unlock()

	if !k.isRunning {
		return nil
	}

	k.isRunning = false
	close(k.stopCh)

	for prefix, watcher := range k.watchers {
		close(watcher.stopCh)
		log.Printf("KVStore.Stop: stopping watcher prefix=%s", prefix)
	}

	k.watchers = make(map[string]*Watcher)

	log.Printf("KVStore.Stop: all watchers stopped")
	return nil
}

// GetCacheSize 获取缓存大小
func (k *KVStore) GetCacheSize() int {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return len(k.cache)
}
