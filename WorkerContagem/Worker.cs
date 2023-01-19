using System.Text.Json;
using Azure.Storage.Queues;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using WorkerContagem.Data;
using WorkerContagem.Models;

namespace WorkerContagem;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _configuration;
    private readonly ContagemRepository _repository;
    private readonly string _queueName;
    private readonly QueueClient _queueClient;
    private readonly TelemetryConfiguration _telemetryConfig;

    public Worker(ILogger<Worker> logger,
        IConfiguration configuration,
        ContagemRepository repository,
        TelemetryConfiguration telemetryConfig)
    {
        _logger = logger;
        _configuration = configuration;
        _repository = repository;
        _telemetryConfig = telemetryConfig;
        _queueName = _configuration["AzureQueueStorage:Queue"]!;
        _queueClient = new QueueClient(
            _configuration.GetConnectionString("AzureQueueStorage"), _queueName);

        _logger.LogInformation($"Azure Queue Storage - Queue = {_queueName}");
        _logger.LogInformation(
            "Iniciando o processamento de mensagens...");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var message = await _queueClient.ReceiveMessageAsync();
            if (message.Value != null)
            {
                await _queueClient.DeleteMessageAsync(
                    message.Value.MessageId, message.Value.PopReceipt);

                var messageContent = message.Value.MessageText;
                _logger.LogInformation(
                    $"[{_queueName} | Nova mensagem] " + messageContent);
                var telemetryClient = new TelemetryClient(_telemetryConfig);

                ResultadoContador? resultado;            
                try
                {
                    resultado = JsonSerializer.Deserialize<ResultadoContador>(messageContent,
                        new JsonSerializerOptions()
                        {
                            PropertyNameCaseInsensitive = true
                        });
                    telemetryClient.TrackTrace(
                        $"Desserializacao realizada com sucesso | {messageContent}");
                }
                catch
                {
                    _logger.LogError("Dados inválidos para o Resultado");
                    resultado = null;
                }

                if (resultado is not null)
                {
                    try
                    {
                        _repository.Save(resultado);
                        _logger.LogInformation("Resultado registrado com sucesso!");
                        telemetryClient.TrackTrace(
                            $"Resultado processado | Valor = {resultado.ValorAtual}");
                        _logger.LogInformation("Aguardando nova mensagem...");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"Erro durante a gravação: {ex.Message}");
                    }
                }
            }
        }
    }
}
