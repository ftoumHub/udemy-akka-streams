package rockthejvm.part4_techniques;

import java.time.Duration;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import akka.actor.ActorSystem;
import akka.japi.function.Function;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.RestartSource;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Sink;

public class FaultTolerance {

    ActorSystem system;
    ActorMaterializer mat;

    final Function<Integer, Integer> faultyFunction = e -> {
        if (e == 6) {
            throw new RuntimeException();
        } else {
            return e;
        }
    };

    @Before
    public void setup() {
        system = ActorSystem.create("FaultTolerance");
        mat = ActorMaterializer.create(system);
    }

    @Test // 1 - logging
    public void logging(){
        // Dans ce cas, on se contente de logguer le crash du stream
        // Pour voir chaque élément dans les logs on doit être en DEBUG
        Source.range(0, 10).map(faultyFunction)
                .log("trackingElements") // on ajoute un tag sur la source
                .to(Sink.ignore()).run(mat);
        // Lorsqu'un élement du flux lance une exception, celà à pour effet de terminer le stream.
    }

    @Test // 2 - gracefully terminating a stream
    public void gracefullyTerminatingAStream() {
        // Dans ce cas, l'upstream va se terminer proprement, sans crash
        Source.range(0, 10).map(faultyFunction)
                // recover prend en paramètre une PartialFonction de throwable vers une valeur
                .recover(RuntimeException.class, () -> Integer.min(0, 10))
                .log("gracefulSource")
                .to(Sink.ignore())
                .run(mat);
    }

    @Test // 3 - recover with another stream
    public void recoverWithAnotherStream(){
        // Au lieur de terminer le stream avec une valeur, on peut également
        // remplacer le stream initial par un autre stream
        Source.range(0, 10).map(faultyFunction)
                .recoverWithRetries(3, RuntimeException.class, () -> Source.range(90, 99))
                .log("recoverWithRetries")
                .to(Sink.ignore())
                .run(mat);
    }

    // 4 - backoff supervision
    public static void main(String[] args) {
        ActorSystem actorSystem = ActorSystem.create();

        RestartSource.onFailuresWithBackoff(
                Duration.ofSeconds(1),
                Duration.ofSeconds(30),
                0.2,
                3,
                () -> {
                    final int randomNumber = new Random().nextInt(20);
                    System.out.println("RandomNumber: " + randomNumber);
                    return Source.range(0, 10).map(elem -> {
                        if (elem == randomNumber) {
                            throw new RuntimeException();
                        } else {
                            return elem;
                        }
                    });
                })
                .log("restartBackoff")
                .to(Sink.ignore())
                .run(ActorMaterializer.create(actorSystem));
    }
}
