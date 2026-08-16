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

        onException(Exception.class)
            .maximumRedeliveries(3)
            .redeliveryDelay(2000)
            .handled(true)
            .logHandled(true)
            .log("❌ [ERRO CRÍTICO] Falha ao persistir telemetria no banco após retentativas. Mensagem descartada.");

        from("kafka:vehicle-telemetry?brokers={{kafka.bootstrap.servers}}")
            .unmarshal().json(JsonLibrary.Jackson, VehicleData.class)
            .process(exchange -> {
                VehicleData data = exchange.getIn().getBody(VehicleData.class);

                // Aplica as mesmas constraints de Bean Validation usadas no
                // endpoint REST (@NotBlank, @DecimalMin/Max, @Min), já que
                // eventos vindos do Kafka não passam pelo JAX-RS e portanto
                // não são validados automaticamente por @Valid.
                Set<ConstraintViolation<VehicleData>> violations = validator.validate(data);
                if (!violations.isEmpty()) {
                    StringBuilder errors = new StringBuilder();
                    for (ConstraintViolation<VehicleData> v : violations) {
                        errors.append(v.getPropertyPath()).append(": ").append(v.getMessage()).append("; ");
                    }
                    log.warn("⚠️ [KAFKA] Evento invalido descartado (nao sera persistido): " + errors);
                    return;
                }

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
                    .await().indefinitely();

                log.info("💾 [BANCO] Telemetria de " + data.vehicleId + " gravada com sucesso!");
            });
    }
}
