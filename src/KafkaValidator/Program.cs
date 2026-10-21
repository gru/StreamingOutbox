using KafkaFlow;
using KafkaValidator.Configuration;
using KafkaValidator.Health;
using KafkaValidator.Kafka;
using KafkaValidator.Metrics;
using KafkaValidator.Validation;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Prometheus;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOptions<KafkaValidatorOptions>()
    .BindConfiguration(KafkaValidatorOptions.SectionName);
builder.Services.AddSingleton(sp => sp.GetRequiredService<IOptions<KafkaValidatorOptions>>().Value);

builder.Services.AddSingleton<KafkaValidatorMetrics>();
builder.Services.AddSingleton<RunRegistry>();
builder.Services.AddSingleton<ValidatorEngine>();
builder.Services.AddSingleton<ValidationHandler>();
builder.Services.AddSingleton<ValidationService>();
builder.Services.AddControllers();

builder.Services.AddHttpClient<WriterClient>((sp, http) =>
{
    var cfg = sp.GetRequiredService<IOptions<KafkaValidatorOptions>>().Value;
    http.BaseAddress = new Uri(cfg.Writer.BaseUrl.TrimEnd('/') + "/");
    http.Timeout = TimeSpan.FromSeconds(5);
});

// KafkaFlow — consumer topic outbox.events. Ключевая семантика: группа
// kafka-validator + Earliest + WithoutStoringOffsets — оффсеты НИКОГДА не
// коммитятся, поэтому каждый старт процесса перематывает topic с начала и
// полностью восстанавливает счётчики всех прогонов. Закоммиченные оффсеты
// после рестарта «съели» бы историю — и валидатор ложно сообщил бы о потерях.
// Побочный эффект: kafka_consumergroup_lag для этой группы не возникает.
var kafkaBrokers = (builder.Configuration["KafkaValidator:Kafka:Brokers"] ?? "kafka1:9092,kafka2:9092,kafka3:9092")
    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
var kafkaTopic = builder.Configuration["KafkaValidator:Kafka:Topic"] ?? "outbox.events";
var kafkaGroupId = builder.Configuration["KafkaValidator:Kafka:GroupId"] ?? "kafka-validator";
var bufferSize = int.TryParse(builder.Configuration["KafkaValidator:Kafka:BufferSize"], out var bs) ? Math.Clamp(bs, 100, 100_000) : 1000;
var workersCount = int.TryParse(builder.Configuration["KafkaValidator:Kafka:WorkersCount"], out var wc) ? Math.Clamp(wc, 1, 16) : 1;

builder.Services.AddKafkaFlowHostedService(kafka => kafka
    .UseConsoleLog()
    .AddCluster(cluster => cluster
        .WithBrokers(kafkaBrokers)
        .AddConsumer(consumer => consumer
            .Topic(kafkaTopic)
            .WithGroupId(kafkaGroupId)
            .WithAutoOffsetReset(AutoOffsetReset.Earliest)
            .WithoutStoringOffsets()
            .WithBufferSize(bufferSize)
            .WithWorkersCount(workersCount)
            .AddMiddlewares(m => m
                .AddDeserializer<PassthroughDeserializer>()
                .AddTypedHandlers(h => h.AddHandler<ValidationHandler>())))));

// /ready: готовность = KafkaFlow-шина запущена (running); см. ValidatorEngineHealthCheck
builder.Services.AddHealthChecks()
    .AddCheck<ValidatorEngineHealthCheck>("validator-engine");

// ПОСЛЕ AddKafkaFlowHostedService: hosted-сервисы стартуют в порядке
// регистрации — running выставляется, когда шина (и консюмер) уже запущены
builder.Services.AddHostedService<ValidatorLifecycleHostedService>();

var app = builder.Build();

var cfg = app.Services.GetRequiredService<IOptions<KafkaValidatorOptions>>().Value;

app.Logger.LogInformation(
    "KafkaValidator запускается; brokers: {Brokers}; topic: {Topic} группа: {GroupId} (без сохранения оффсетов — старт всегда с начала topic); Writer: {WriterUrl} (fetchFromWriter)",
    cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.GroupId, cfg.Writer.BaseUrl);

app.MapControllers();

// /health — liveness без опроса зависимостей; /ready — чеки выше (validator-engine)
app.MapHealthChecks("/health", new HealthCheckOptions
{
    Predicate = _ => false,
    ResponseWriter = HealthResponses.WriteAsync,
});
app.MapHealthChecks("/ready", new HealthCheckOptions
{
    ResponseWriter = HealthResponses.WriteAsync,
});

app.MapMetrics(); // /metrics — pull для Prometheus (prometheus-net 5.0: достаточно MapMetrics)

await app.RunAsync();
