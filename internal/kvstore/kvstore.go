package kvstore

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/cluster-actor/server/internal/config"
	consulapi "github.com/hashicorp/consul/api"
)

// KVEntry KV条目
type KVEntry struct {
	Key         string    // 键
	Value       []byte    // 值
	ModifyIndex uint64    // 最后修改索引
	CreatedAt   time.Time // 创建时间
	UpdatedAt   time.Time // 更新时间
}

// KVStore 分布式KV存储，基于Consul KV和本地缓存
type KVStore struct {
	cfg         *config.ConsulConfig // Consul配置
	client      *consulapi.Client    // Consul客户端
	cache       map[string]*KVEntry  // 本地KV缓存
	mu          sync.RWMutex         // 读写锁
	watchers    map[string]*Watcher  // 前缀监听器
	watchersMu  sync.Mutex           // 监听器锁
	stopCh      chan struct{}        // 停止信号
	isRunning   bool                 // 运行状态
	clusterName string               // 集群名称
	cachePrefix string               // 缓存前缀
}

// NewKVStore 创建KVStore实例
func NewKVStore(cfg *config.ClusterConfig) (*KVStore, error) {
	consulCfg := consulapi.DefaultConfig()
	consulCfg.Address = cfg.Consul.Address
	consulCfg.Scheme = cfg.Consul.Scheme
	if cfg.Consul.Token != "" {
		consulCfg.Token = cfg.Consul.Token
	}
	if cfg.Consul.Datacenter != "" {
		consulCfg.Datacenter = cfg.Consul.Datacenter
	}

	client, err := consulapi.NewClient(consulCfg)
	if err != nil {
		return nil, fmt.Errorf("创建Consul客户端失败: %w", err)
	}

	kv := &KVStore{
		cfg:         &cfg.Consul,
		client:      client,
		cache:       make(map[string]*KVEntry),
		watchers:    make(map[string]*Watcher),
		stopCh:      make(chan struct{}),
		clusterName: cfg.ClusterName,
		cachePrefix: cfg.Consul.CachePrefix,
	}

	if kv.cachePrefix == "" {
		kv.cachePrefix = fmt.Sprintf("%s/", cfg.ClusterName)
	}
	// 添加默认的前缀监听器
	watchPrefixes := []string{
		kv.cachePrefix,
	}
	for _, prefix := range watchPrefixes {
		kv.watchers[prefix] = &Watcher{
			Prefix:    prefix,
			stopCh:    make(chan struct{}),
			lastIndex: 0,
			kv:        kv,
		}
	}
	log.Printf("KVStore watchers: cachePrefix=%s", watchPrefixes)
	return kv, nil
}

// Get 从本地缓存读取KV
func (k *KVStore) Get(key string) ([]byte, bool) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	entry, found := k.cache[key]
	if !found {
		return nil, false
	}
	return entry.Value, true
}

// GetEntry 从本地缓存读取完整KVEntry
func (k *KVStore) GetEntry(key string) (*KVEntry, bool) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	entry, found := k.cache[key]
	if !found {
		return nil, false
	}
	return entry, true
}

// Set 写入KV到Consul（不等待Watch同步）
func (k *KVStore) Set(key string, value []byte) error {
	pair := &consulapi.KVPair{
		Key:   key,
		Value: value,
	}

	_, err := k.client.KV().Put(pair, nil)
	if err != nil {
		return fmt.Errorf("写入Consul KV失败: %w", err)
	}

	log.Printf("KVStore.Set: key=%s, value_len=%d", key, len(value))
	return nil
}

// SetWithCAS 使用CAS（Compare-And-Swap）写入KV
func (k *KVStore) SetWithCAS(key string, value []byte, modifyIndex uint64) (bool, error) {
	pair := &consulapi.KVPair{
		Key:         key,
		Value:       value,
		ModifyIndex: modifyIndex,
	}

	success, _, err := k.client.KV().CAS(pair, nil)
	if err != nil {
		return false, fmt.Errorf("CAS写入Consul KV失败: %w", err)
	}

	if !success {
		return false, fmt.Errorf("CAS冲突，当前modifyIndex=%d", modifyIndex)
	}

	log.Printf("KVStore.SetWithCAS: key=%s, modifyIndex=%d", key, modifyIndex)
	return true, nil
}

// Delete 从Consul删除KV
func (k *KVStore) Delete(key string) error {
	_, err := k.client.KV().Delete(key, nil)
	if err != nil {
		return fmt.Errorf("删除Consul KV失败: %w", err)
	}

	log.Printf("KVStore.Delete: key=%s", key)
	return nil
}

// List 列出指定前缀的所有KV（从Consul查询）
func (k *KVStore) List(prefix string) ([]*KVEntry, error) {
	pairs, _, err := k.client.KV().List(prefix, nil)
	if err != nil {
		return nil, fmt.Errorf("查询Consul KV列表失败: %w", err)
	}

	entries := make([]*KVEntry, 0, len(pairs))
	for _, pair := range pairs {
		entries = append(entries, &KVEntry{
			Key:         pair.Key,
			Value:       pair.Value,
			ModifyIndex: pair.ModifyIndex,
		})
	}

	return entries, nil
}

// ListFromCache 从本地缓存列出指定前缀的所有KV
func (k *KVStore) ListFromCache(prefix string) []*KVEntry {
	k.mu.RLock()
	defer k.mu.RUnlock()

	entries := make([]*KVEntry, 0)
	for key, entry := range k.cache {
		if len(prefix) == 0 || len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			entries = append(entries, entry)
		}
	}

	return entries
}

// GetPrefixKey 获取带集群前缀的完整key
func (k *KVStore) GetPrefixKey(category, subKey string) string {
	return fmt.Sprintf("cluster/%s/%s/%s", k.clusterName, category, subKey)
}
