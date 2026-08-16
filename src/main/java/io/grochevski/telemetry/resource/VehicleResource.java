package io.grochevski.telemetry.resource;

import java.time.Instant;
import java.util.List;

import io.grochevski.telemetry.entity.VehicleData;
import org.jboss.logging.Logger;

import io.quarkus.hibernate.reactive.panache.Panache;
import io.smallrye.mutiny.Uni;
import jakarta.validation.Valid;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/telemetry")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class VehicleResource {

    private static final Logger LOG = Logger.getLogger(VehicleResource.class);
    
    @GET
    public Uni<List<VehicleData>> getAll() {
    return VehicleData.listAll(io.quarkus.panache.common.Sort.descending("id"));
}

    @GET
    @Path("/{vehicleId}")
    public io.smallrye.mutiny.Uni<java.util.List<VehicleData>> getVehicleHistory(@PathParam("vehicleId") String vehicleId) {
        return VehicleData.findByVehicle(vehicleId);
    }

    @POST
    public Uni<Response> receiveTelemetry(@Valid VehicleData data) {
        if (data.timestamp == null) {
            data.timestamp = Instant.now();
        }

        data.eventHash = data.computeEventHash();

        if (data.isSpeeding()) {
            LOG.warnf("⚠️ ALERTA: Veículo %s atingiu %d km/h! Localização: %f, %f", 
                data.vehicleId, data.speed, data.latitude, data.longitude);
        } else {
            LOG.infof("Telemetria recebida para o veículo %s (%d km/h)", data.vehicleId, data.speed);
        }
        
        return Panache.withTransaction(data::persist)
                .replaceWith(Response.status(Response.Status.CREATED).entity(data).build());
    }
}
