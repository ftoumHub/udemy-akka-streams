package rockthejvm.part4_techniques;

import static io.vavr.API.println;

import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.dispatch.MessageDispatcher;
import akka.stream.ActorAttributes;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.concurrent.Future;
import lombok.AllArgsConstructor;

public class IntegratingWithExternalServices {

    // example: simplified PagerDuty
    @AllArgsConstructor
    class PagerEvent{ String application; String description; Date date; }

    final Source<PagerEvent, NotUsed> eventSource = Source.from(List.of(
            new PagerEvent("AkkaInfra", "Infrastructure broke", new Date()),
            new PagerEvent("FastDataPipeline", "Illegal elements in the data pipeline", new Date()),
            new PagerEvent("AkkaInfra", "A service stopped responding", new Date()),
            new PagerEvent("SuperFrontend", "A button doesn't work", new Date())
    ));

    public static void main(String[] args) {

        ActorSystem system = ActorSystem.create("IntegratingWithExternalServices");
        //final MessageDispatcher dispatcher = system.dispatchers().lookup("dedicated-dispatcher");
        ActorMaterializer mat = ActorMaterializer.create(system);

        class PagerService {
            private List<String> engineers = List.of("Daniel", "John", "Lady Gaga");
            private Map<String, String> emails = HashMap.of(
                    "Daniel", "daniel@rockthejvm.com",
                    "John", "john@rockthejvm.com",
                    "Lady Gaga", "ladygaga@rtjvm.com");

            public CompletableFuture<String> processEvent(PagerEvent pagerEvent) {
                return Future.ofSupplier(() -> {
                    final Long engineerIndex = (pagerEvent.date.toInstant().getEpochSecond() / (24 * 3600)) % engineers.length();
                    final String engineer = engineers.get(engineerIndex.intValue());
                    final String engineerEmail = emails.get(engineer).getOrNull();

                    println("Sending engineer " + engineerEmail + " a high priority notification: " + pagerEvent);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    return engineerEmail;
                }).toCompletableFuture();
            }
        }

        final Source<PagerEvent, NotUsed> infraEvents = new IntegratingWithExternalServices()
                .eventSource.filter(__ -> __.application.equals("AkkaInfra"));
        final Source<String, NotUsed> pagedEngineersEmails =
                infraEvents.mapAsync(4, event -> new PagerService().processEvent(event));
        // guarantees the relative order of elements
        final Sink<String, CompletionStage<Done>> pagedEmailsSink =
                Sink.<String>foreach(email -> println("Successfully sent notification to " + email))
                        .withAttributes(ActorAttributes.dispatcher("dedicated-dispatcher"));

        pagedEngineersEmails.to(pagedEmailsSink).run(mat);
    }
}
