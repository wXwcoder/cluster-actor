package telemetry

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/zipkin"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

// ZipkinConfig Zipkin 追踪配置结构
type ZipkinConfig struct {
	ServiceName        string
	Endpoint           string
	SampleRatio        float64
	ResourceAttributes map[string]string
}

// TracerProvider 追踪提供者包装器
type TracerProvider struct {
	provider *sdktrace.TracerProvider
}

// Shutdown 关闭追踪提供者，释放资源
func (tp *TracerProvider) Shutdown(ctx context.Context) error {
	if tp.provider != nil {
		return tp.provider.Shutdown(ctx)
	}
	return nil
}

// InitZipkin 初始化 Zipkin 追踪并返回 TracerProvider
// serviceName: 服务名称，用于在 Zipkin UI 中识别服务
// endpoint: Zipkin 服务器地址，例如 "http://localhost:9411/api/v2/spans"
// sampleRatio: 采样率，0.0 表示不采样，1.0 表示全部采样
func InitZipkin(ctx context.Context, serviceName, endpoint string, sampleRatio float64) (*TracerProvider, error) {
	config := ZipkinConfig{
		ServiceName: serviceName,
		Endpoint:    endpoint,
		SampleRatio: sampleRatio,
	}
	return InitZipkinWithConfig(ctx, config)
}

// InitZipkinWithConfig 使用完整配置初始化 Zipkin 追踪
func InitZipkinWithConfig(ctx context.Context, cfg ZipkinConfig) (*TracerProvider, error) {
	if cfg.Endpoint == "" {
		cfg.Endpoint = "http://localhost:9411/api/v2/spans"
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "unknown-service"
	}
	if cfg.SampleRatio <= 0 || cfg.SampleRatio > 1 {
		cfg.SampleRatio = 1.0
	}

	log.Printf("初始化 Zipkin 追踪: 服务名=%s, 端点=%s, 采样率=%.2f",
		cfg.ServiceName, cfg.Endpoint, cfg.SampleRatio)

	// 创建 Zipkin exporter
	exporter, err := zipkin.New(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("创建 Zipkin exporter 失败: %w", err)
	}

	// 构建资源属性
	attrs := []attribute.KeyValue{
		semconv.ServiceName(cfg.ServiceName),
	}

	for key, value := range cfg.ResourceAttributes {
		attrs = append(attrs, attribute.String(key, value))
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(attrs...),
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
	)
	if err != nil {
		log.Printf("警告: 创建完整资源失败，使用最小资源: %v", err)
		res = resource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceName(cfg.ServiceName),
		)
	}

	// 创建 TracerProvider，使用更激进的 Batcher 配置
	// BatchSpanProcessor 配置：
	// - MaxExportBatchSize: 1 (每 1 个 span 就导出)
	// - BatchTimeout: 1s (每秒检查一次是否有数据要导出)
	// - MaxQueueSize: 2048 (队列大小)
	batchOpts := sdktrace.WithBatcher(
		exporter,
		sdktrace.WithMaxExportBatchSize(1),
		sdktrace.WithBatchTimeout(1*time.Second),
		sdktrace.WithMaxQueueSize(2048),
	)

	tp := sdktrace.NewTracerProvider(
		batchOpts,
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	// 设置全局 TracerProvider
	otel.SetTracerProvider(tp)

	// 设置全局传播器（支持 W3C TraceContext 和 Baggage）
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	log.Printf("Zipkin 追踪初始化完成（已优化 Batcher 配置以确保数据及时发送）")
	return &TracerProvider{provider: tp}, nil
}

// GetTracer 获取指定名称的 Tracer
func GetTracer(name string) trace.Tracer {
	return otel.Tracer(name)
}

// TestSpan 创建一个测试 span 用于验证 Zipkin 连接
func TestSpan(ctx context.Context, serviceName string) {
	tracer := GetTracer(serviceName + "-test")
	ctx, span := tracer.Start(ctx, "test-operation")
	defer span.End()

	span.SetAttributes(
		attribute.String("test.key", "test-value"),
		attribute.String("test.message", "这是测试 span，用于验证 Zipkin 连接"),
	)

	log.Printf("已创建测试 span: test-operation")
}
