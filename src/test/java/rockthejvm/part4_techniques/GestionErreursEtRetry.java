package rockthejvm.part4_techniques;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.RestartSource;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import io.vavr.API;
import io.vavr.collection.List;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;

import static io.vavr.API.println;

public class GestionErreursEtRetry {

    public static void main(String[] args) {

        ActorSystem actorSystem = ActorSystem.create();

        Source.from(List.of("message-a", "message-b", "message-c"))
                .flatMapConcat(msg -> {
                    AtomicInteger counter = new AtomicInteger(0);

                    return RestartSource.onFailuresWithBackoff(
                            Duration.ofSeconds(1),
                            Duration.ofSeconds(5),
                            0,
                            3,
                            () -> {
                                println("gererLeMessage "+msg+" - "+counter.get()+" - "+ LocalDateTime.now());
                                return gererLeMessage(msg, counter.incrementAndGet());
                            }
                    ).recover(Exception.class, () -> "Default String");
                    //
                    //                    return gererLeMessage(msg, 0)
                    //                            .recoverWithRetries(3, MyException.class, () -> {
                    //                                int count = counter.incrementAndGet();
                    //                                System.out.println("Recover " + " - " + msg + " - " + count);
                    //                                return gererLeMessage(msg, count);
                    //                            });
                })
                //.recover(Exception.class, () -> "RECOVER")
                .watchTermination((nu, whenDone) ->
                        whenDone.whenComplete((d, e) -> {
                            if (e != null) {
                                e.printStackTrace();
                            } else {
                                println("Done");
                            }
                        })
                )
                .runWith(Sink.foreach(API::println), ActorMaterializer.create(actorSystem));

    }


    public static Source<String, NotUsed> gererLeMessage(String msg, Integer count) {
        if (count < 5) {
            return Source.failed(new MyException());
        } else {
            return Source.single(msg.toUpperCase()+count);
        }
    }

    public static class MyException extends RuntimeException {

    }

}
