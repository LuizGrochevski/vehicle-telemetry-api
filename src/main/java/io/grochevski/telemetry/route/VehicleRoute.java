package io.grochevski.telemetry.route;

import java.util.Set;

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

        // Dead Letter Channel real: após esgotar as retentativas, a
        // mensagem ORIGINAL (o JSON cru recebido do Kafka, via
        // useOriginalMessage()) é reenviada para o tópico
        // vehicle-telemetry-dlq em vez de simplesmente descartada.
        // Isso permite inspeção e reprocessamento manual posterior,
        // ao contrário de um log-and-drop que perde o dado
        // definitivamente.
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
            .process(exchange -> {
                VehicleData data = exchange.getIn().getBody(VehicleData.class);

                Set<ConstraintViolation<VehicleData>> violations = validator.validate(data);
                if (!violations.isEmpty()) {
                    StringBuilder errors = new StringBuilder();
                    for (ConstraintViolation<VehicleData> v : violations) {
                        errors.append(v.getPropertyPath()).append(": ").append(v.getMessage()).append("; ");
                    }
                    log.warn("⚠️ [KAFKA] Evento invalido descartado (nao sera persistido): " + errors);
                    return;
                }

                data.eventHash = data.computeEventHash();

                if (data.isSpeeding()) {
                    log.warn("🚨 [KAFKA] ALERTA DE VELOCIDADE: Veículo " + data.vehicleId + " a " + data.speed + " km/h!");
                } else {
                    log.info("📥 [KAFKA] Telemetria processada para o veículo " + data.vehicleId);
                }

                try {
                    sessionFactory.openSession()
                        .flatMap(session -> session.persist(data)
                            .flatMap(v -> session.flush())
                            .onTermination().call(session::close)
                        )
                        .await().indefinitely();

                    log.info("💾 [BANCO] Telemetria de " + data.vehicleId + " gravada com sucesso!");
                } catch (Exception e) {
                    if (isDuplicateKeyViolation(e)) {
                        // Reprocessamento do Kafka (at-least-once delivery):
                        // a mesma mensagem já foi persistida antes. Não é um
                        // erro real — não deve ir para a DLQ nem disparar
                        // retry, apenas loga e segue.
                        log.info("🔁 [KAFKA] Evento duplicado detectado e ignorado (ja processado): " + data.vehicleId);
                    } else {
                        // Erro genuíno (ex: banco fora do ar) — relança para
                        // o onException tratar com retry e, se esgotar,
                        // encaminhar para a DLQ.
                        throw e;
                    }
                }
            });
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
