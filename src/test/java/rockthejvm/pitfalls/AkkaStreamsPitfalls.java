package rockthejvm.pitfalls;

import static io.vavr.API.println;

import java.util.concurrent.CompletableFuture;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import akka.actor.ActorSystem;
import akka.japi.function.Function;
import akka.japi.pf.PFBuilder;
import akka.stream.ActorMaterializer;
import akka.stream.ActorMaterializerSettings;
import akka.stream.Materializer;
import akka.stream.Supervision;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import io.vavr.control.Try;

/**
 * https://blog.softwaremill.com/akka-streams-pitfalls-to-avoid-part-1-75ef6403c6e6
 */
public class AkkaStreamsPitfalls {

    ActorSystem system;
    ActorMaterializer mat;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        system = ActorSystem.create("FirstPrinciples");
        mat = ActorMaterializer.create(system);
    }

    /**
     * 1. Swallowed exceptions
     */
    @Test
    public void swallowExceptions() {
        // The default behavior for most stages when the exception is thrown is to… swallow it.
        Source.range(1, 10)
                .map(i -> i / 0)
                .runWith(Sink.ignore(), mat);
    }

    @Test
    public void withRecover() {
        //In such case the stream will just be completed without any logs. To actually log exceptions there are 2 options:
        // - log or rethrow exception in the recover stage. This way all exceptions from upstream will be caught and logged:

        //https://doc.akka.io/docs/akka/2.5.4/java/stream/stream-error.html
        Source.range(1, 10)
                .map(i -> i / 0)
                .recover(new PFBuilder()
                        .match(RuntimeException.class, ex -> {
                            println("exception : " + ex);
                            return null;
                        })
                        .build())
                .runWith(Sink.ignore(), mat);
    }

    @Test
    public void withSupervisionStrategy() {
        // define custom supervision strategy and use it in stream attributes or in materializer settings:
        final Function<Throwable, Supervision.Directive> decider = exc -> {
            println("Exception: " + exc);
            return Supervision.stop();
        };

        final Materializer loggingMaterializer  = ActorMaterializer.create(
                ActorMaterializerSettings.create(system).withSupervisionStrategy(decider),
                system);
        Source.range(1, 10)
                .map(i -> i / 0)
                .runWith(Sink.ignore(), loggingMaterializer );
    }

    /**
     * 4. mapAsync — keep everything in the Future
     */
    @Test
    public void keepEverythingInTheFuture() {
        // If you want to process something in parallel, there is a good chance that you
        // would usemapAsync. Good choice, just remember to put everything in the
        // Future. Otherwise you will end up with blocking code.
        Source.range(1, 5)
                .mapAsync(5, i -> {
                    println("very slow action "+ i + " ...");
                    Try.of(() -> {
                        Thread.sleep(1700);
                        return true;
                    });
                    return CompletableFuture.completedFuture(i);
                })
                .grouped(5)
                .runForeach(__ -> println("done..."), mat);
        // Because the Future is not enclosing the blocking code, it will actually block the actor responsible for this stage.
    }

    @Test
    public void keepEverythingInTheFutureBetter() {
        // Remember that all stages not marked asynchronous will run in one single actor. With a corrected example:
        Source.range(1, 5)
                .mapAsync(5, i ->
                    CompletableFuture.runAsync(() -> {
                        println("very slow action "+ i + " ...");
                        Try.of(() -> {
                            Thread.sleep(2000);
                            return true;
                        });
                    }, system.getDispatcher())
                )
                .grouped(5)
                .runForeach(__ -> println("done..."), mat);
    }
}
