package io.grochevski.telemetry.entity;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;

import io.quarkus.hibernate.reactive.panache.PanacheEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.DecimalMax;

@Entity
public class VehicleData extends PanacheEntity {

    @NotBlank(message = "O ID do veículo é obrigatório")
    public String vehicleId;

    @DecimalMin("-90.0")
    @DecimalMax("90.0")
    public double latitude;

    @DecimalMin("-180.0")
    @DecimalMax("180.0")
    public double longitude;

    @Min(value = 0, message = "A velocidade não pode ser negativa")
    public int speed;

    public Instant timestamp;

    // Hash determinístico dos campos do evento, usado como chave de
    // idempotência: se a mesma mensagem for reprocessada pelo Kafka
    // (comportamento at-least-once), a constraint única no banco rejeita
    // a segunda tentativa de insert, evitando telemetria duplicada.
    @Column(unique = true, length = 64)
    public String eventHash;

    public boolean isSpeeding() {
        return this.speed > 110;
    }

    public String computeEventHash() {
        String raw = vehicleId + "|" + timestamp + "|" + latitude + "|" + longitude + "|" + speed;
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(raw.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 é garantido presente em qualquer JVM padrão; isso
            // nunca deve acontecer, mas evita silenciar o erro.
            throw new IllegalStateException("SHA-256 indisponível na JVM", e);
        }
    }

    public static io.smallrye.mutiny.Uni<java.util.List<VehicleData>> findByVehicle(String vehicleId) {
        return list("vehicleId", vehicleId);
    }
}
