package io.grochevski.telemetry.route;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.camel.AsyncProcessor;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.hibernate.reactive.mutiny.Mutiny;

import io.grochevski.telemetry.entity.VehicleData;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;

@ApplicationScoped
public class VehicleRoute extends RouteBuilder {

    @Inject
    Mutiny.SessionFactory sessionFactory;

    @Inject
    Validator validator;

    @Override
    public void configure() throws Exception {

        onException(Exception.class)
            .maximumRedeliveries(3)
            .redeliveryDelay(2000)
            .useOriginalMessage()
            .handled(true)
            .logHandled(true)
            .log("❌ [DLQ] Falha ao persistir apos retentativas — mensagem enviada para vehicle-telemetry-dlq. Erro: ${exception.message}")
            .to("kafka:vehicle-telemetry-dlq?brokers={{kafka.bootstrap.servers}}");

        from("kafka:vehicle-telemetry?brokers={{kafka.bootstrap.servers}}")
            .unmarshal().json(JsonLibrary.Jackson, VehicleData.class)
            .process(new AsyncPersistProcessor());
    }

    /**
     * Processor assíncrono real: em vez de bloquear a thread do Camel com
     * .await().indefinitely() (versão anterior), a persistência reativa
     * (Mutiny) é conectada a um CompletableFuture — a thread do Camel é
     * liberada imediatamente e o future só completa quando a operação
     * assíncrona termina de fato, sem travar nenhuma thread esperando.
     */
    private class AsyncPersistProcessor implements AsyncProcessor {

        @Override
        public void process(Exchange exchange) throws Exception {
            // Fallback sincrono exigido pela interface Processor (pai de
            // AsyncProcessor). O motor assincrono do Camel usa
            // processAsync()/process(Exchange, AsyncCallback) no caminho
            // normal desta rota; este metodo so existiria como fallback
            // de algum caminho interno raro do Camel que ainda chame a
            // via sincrona diretamente.
            processAsync(exchange).join();
        }

        @Override
        public boolean process(Exchange exchange, org.apache.camel.AsyncCallback callback) {
            processAsync(exchange).whenComplete((result, throwable) -> callback.done(false));
            return false;
        }

        @Override
        public CompletableFuture<Exchange> processAsync(Exchange exchange) {
            CompletableFuture<Exchange> future = new CompletableFuture<>();

            VehicleData data = exchange.getIn().getBody(VehicleData.class);

            Set<ConstraintViolation<VehicleData>> violations = validator.validate(data);
            if (!violations.isEmpty()) {
                StringBuilder errors = new StringBuilder();
                for (ConstraintViolation<VehicleData> v : violations) {
                    errors.append(v.getPropertyPath()).append(": ").append(v.getMessage()).append("; ");
                }
                log.warn("⚠️ [KAFKA] Evento invalido descartado (nao sera persistido): " + errors);
                future.complete(exchange);
                return future;
            }

            data.eventHash = data.computeEventHash();

            if (data.isSpeeding()) {
                log.warn("🚨 [KAFKA] ALERTA DE VELOCIDADE: Veículo " + data.vehicleId + " a " + data.speed + " km/h!");
            } else {
                log.info("📥 [KAFKA] Telemetria processada para o veículo " + data.vehicleId);
            }

            sessionFactory.openSession()
                .flatMap(session -> session.persist(data)
                    .flatMap(v -> session.flush())
                    .onTermination().call(session::close)
                )
                .subscribe().with(
                    success -> {
                        log.info("💾 [BANCO] Telemetria de " + data.vehicleId + " gravada com sucesso!");
                        future.complete(exchange);
                    },
                    failure -> {
                        if (isDuplicateKeyViolation(failure)) {
                            log.info("🔁 [KAFKA] Evento duplicado detectado e ignorado (ja processado): " + data.vehicleId);
                        } else {
                            // Erro genuíno: marca a exceção no exchange para
                            // o onException (retry + DLQ) assumir o tratamento.
                            exchange.setException(failure);
                        }
                        future.complete(exchange);
                    }
                );

            return future;
        }
    }

    private boolean isDuplicateKeyViolation(Throwable t) {
        Throwable current = t;
        while (current != null) {
            String msg = current.getMessage();
            if (msg != null) {
                String lower = msg.toLowerCase();
                if (lower.contains("duplicate key") || lower.contains("unique constraint") || lower.contains("eventhash") || lower.contains("event_hash")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }
}
