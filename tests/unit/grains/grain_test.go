package grains_test

import (
	"testing"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/cluster-actor/server/internal/grains"
)

func TestHelloGrainActor(t *testing.T) {
	// 创建 ActorSystem
	system := actor.NewActorSystem()
	rootCtx := system.Root

	// 创建 HelloGrain Props
	props := grains.NewHelloGrainProps("test-grain-1")

	// Spawn actor
	pid := rootCtx.Spawn(props)

	// 停止 actor
	_ = rootCtx.StopFuture(pid).Wait()
}

func TestHelloGrainRegistry(t *testing.T) {
	// 创建 ActorSystem
	system := actor.NewActorSystem()
	rootCtx := system.Root

	// 创建 Props
	props := grains.NewHelloGrainProps("")

	// 创建注册表
	registry := newGrainRegistryTest("hello", props, rootCtx)

	// 测试 KindName
	if registry.KindName != "hello" {
		t.Errorf("期望 KindName=hello, 实际=%s", registry.KindName)
	}

	// 测试激活Grain
	pid1, err := registry.ActivateGrain("user-1")
	if err != nil {
		t.Fatalf("激活Grain失败: %v", err)
	}
	if pid1 == nil {
		t.Error("激活Grain应该返回有效的PID")
	}

	// 再次激活同一Grain应该返回相同实例
	pid2, err := registry.ActivateGrain("user-1")
	if err != nil {
		t.Fatalf("激活Grain失败: %v", err)
	}
	if pid1.String() != pid2.String() {
		t.Error("同一identity的Grain应该返回相同PID")
	}

	// 测试获取Grain
	found, exists := registry.GetGrain("user-1")
	if !exists {
		t.Error("应该能找到已激活的Grain")
	}
	if found.Id != "user-1" {
		t.Errorf("期望 identity=user-1, 实际=%s", found.Id)
	}

	// 测试获取不存在的Grain
	_, exists = registry.GetGrain("non-existent")
	if exists {
		t.Error("不应该能找到不存在的Grain")
	}

	// 测试Grain数量
	if count := registry.GrainCount(); count != 1 {
		t.Errorf("期望 GrainCount=1, 实际=%d", count)
	}

	// 测试停用Grain
	if err := registry.DeactivateGrain("user-1"); err != nil {
		t.Fatalf("停用Grain失败: %v", err)
	}

	if count := registry.GrainCount(); count != 0 {
		t.Errorf("期望 GrainCount=0, 实际=%d", count)
	}
}

func TestGetActiveGrains(t *testing.T) {
	// 创建 ActorSystem
	system := actor.NewActorSystem()
	rootCtx := system.Root

	// 创建 Props
	props := grains.NewHelloGrainProps("")

	// 创建注册表
	registry := newGrainRegistryTest("hello", props, rootCtx)

	// 激活多个Grain
	registry.ActivateGrain("grain-1")
	registry.ActivateGrain("grain-2")
	registry.ActivateGrain("grain-3")

	activeGrains := registry.GetActiveGrains()
	if len(activeGrains) != 3 {
		t.Errorf("期望活跃Grain数=3, 实际=%d", len(activeGrains))
	}

	// 清理
	registry.DeactivateGrain("grain-1")
	registry.DeactivateGrain("grain-2")
	registry.DeactivateGrain("grain-3")
}

// newGrainRegistryTest 创建用于测试的 GrainRegistry
func newGrainRegistryTest(kindName string, props *actor.Props, rootCtx *actor.RootContext) *testGrainRegistry {
	return &testGrainRegistry{
		KindName: kindName,
		Props:    props,
		grains:   make(map[string]*actor.PID),
		rootCtx:  rootCtx,
	}
}

type testGrainRegistry struct {
	KindName string
	Props    *actor.Props
	grains   map[string]*actor.PID
	rootCtx  *actor.RootContext
}

func (r *testGrainRegistry) ActivateGrain(identity string) (*actor.PID, error) {
	if pid, exists := r.grains[identity]; exists {
		return pid, nil
	}

	pid, err := r.rootCtx.SpawnNamed(r.Props, identity)
	if err != nil {
		return nil, err
	}
	r.grains[identity] = pid
	return pid, nil
}

func (r *testGrainRegistry) DeactivateGrain(identity string) error {
	pid, exists := r.grains[identity]
	if !exists {
		return nil
	}

	_ = r.rootCtx.StopFuture(pid).Wait()
	delete(r.grains, identity)
	return nil
}

func (r *testGrainRegistry) GetGrain(identity string) (*actor.PID, bool) {
	pid, exists := r.grains[identity]
	return pid, exists
}

func (r *testGrainRegistry) GetActiveGrains() map[string]*actor.PID {
	cp := make(map[string]*actor.PID, len(r.grains))
	for k, v := range r.grains {
		cp[k] = v
	}
	return cp
}

func (r *testGrainRegistry) GrainCount() int {
	return len(r.grains)
}
