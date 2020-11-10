package rockthejvm.part4_techniques;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorAttributes;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import libs.Await;
import libs.Flows;
import lombok.AllArgsConstructor;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static io.vavr.API.List;
import static io.vavr.API.Map;
import static io.vavr.API.println;
import static io.vavr.concurrent.Future.ofSupplier;
import static java.time.temporal.ChronoUnit.MILLIS;
import static libs.Await.await;

public class IntegratingWithExternalServices {

    public static Logger LOGGER = LoggerFactory.getLogger(IntegratingWithExternalServices.class);

    private static final ActorSystem system = ActorSystem.create("IntegratingWithExternalServices");
    //final MessageDispatcher dispatcher = system.dispatchers().lookup("dedicated-dispatcher");
    private static final ActorMaterializer mat = ActorMaterializer.create(system);

    // example: simplified PagerDuty
    @AllArgsConstructor
    @ToString
    static class PagerEvent{ String application; String description; Date date; }

    static class PagerService {

        private List<String> engineers = List("Daniel", "John", "Lady Gaga");
        private Map<String, String> emails = Map(
                "Daniel", "daniel@rockthejvm.com",
                "John", "john@rockthejvm.com",
                "Lady Gaga", "ladygaga@rtjvm.com");

        public CompletableFuture<String> processEvent(PagerEvent pagerEvent) {
            return ofSupplier(() -> {
                final Long engineerIndex = (pagerEvent.date.toInstant().getEpochSecond() / (24 * 3600)) % engineers.length();
                final String engineer = engineers.get(engineerIndex.intValue());
                final String engineerEmail = emails.get(engineer).getOrNull();

                LOGGER.debug("Sending " + engineerEmail + " a high priority notification: " + pagerEvent);

                await(1000, MILLIS);
                return engineerEmail;
            }).toCompletableFuture();
        }
    }

    static final Source<PagerEvent, NotUsed> eventSource = Source.from(List(
            new PagerEvent("AkkaInfra", "Infrastructure broke", new Date()),
            new PagerEvent("FastDataPipeline", "Illegal elements in the data pipeline", new Date()),
            new PagerEvent("AkkaInfra", "A service stopped responding", new Date()),
            new PagerEvent("SuperFrontend", "A button doesn't work", new Date())
    ));

    public static void main(String[] args) {

        new IntegratingWithExternalServices();
        PagerService pagerService = new PagerService();

        final Source<PagerEvent, NotUsed> infraEvents = eventSource.filter(__ -> __.application.equals("AkkaInfra"));

        final Source<String, NotUsed> pagedEngineersEmails = infraEvents.mapAsync(4, pagerService::processEvent);

        // guarantees the relative order of elements
        final Sink<String, CompletionStage<Done>> pagedEmailsSink =
                Sink.<String>foreach(email -> LOGGER.debug("Successfully sent notification to " + email))
                        .withAttributes(ActorAttributes.dispatcher("dedicated-dispatcher"));

        pagedEngineersEmails.to(pagedEmailsSink).run(mat);

        Await.await(2000, MILLIS);
        system.terminate();
    }
}
