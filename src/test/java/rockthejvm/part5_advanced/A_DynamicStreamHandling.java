package rockthejvm.part5_advanced;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.*;
import akka.stream.javadsl.MergeHub;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import io.vavr.API;
import io.vavr.collection.Stream;

import java.util.concurrent.CompletionStage;

import static akka.stream.javadsl.Keep.right;
import static java.time.Duration.ofSeconds;

/**
 * Stop or abort a stream at runtime
 * <p>
 * Dynamically add fan-in/fan-out branches
 */
public class A_DynamicStreamHandling {

    static ActorSystem system = ActorSystem.create("DynamicStreamHandling");
    static ActorMaterializer mat = ActorMaterializer.create(system);

    private static final Source<Integer, NotUsed> counter =
            Source.from(Stream.from(1)).throttle(1, ofSeconds(1)).log("counter");

    /**
     * Comment arrêter un flow volontairement à l'aide du KillSwitch
     */
    public static void main(String[] args) {

        killSwitch();
        //sharedKillSwitch();
        //mergeHub();
    }

    private static void killSwitch() {
        // A kill switch is a special kind of flow that emits the same elements that go through it,
        // but it materializes to a special value that has some additional methods.
        // La valeur matérialisée est de type UniqueKillSwitch
        final Graph<FlowShape<Integer, Integer>, UniqueKillSwitch> killSwitchFlow = KillSwitches.single();

        final Sink<Integer, CompletionStage<Done>> sink = Sink.ignore();

        final UniqueKillSwitch killSwitch = counter
                .viaMat(killSwitchFlow, right())
                .to(sink)
                .run(mat);

        // on provoque un shutdown au bout de 3 secondes...
        system.scheduler().scheduleOnce(ofSeconds(3), () -> {
            killSwitch.shutdown();
            system.terminate();
        }, system.dispatcher());

        // En exposant le killswitch matérialisé on peut ainsi arrêter le flow quand on le souhaite.
    }

    private static void sharedKillSwitch() {
        final Source<Integer, NotUsed> anotherCounter =
                Source.from(Stream.from(1))
                        .throttle(2, ofSeconds(1)).log("anotherCounter");

        var sharedKillSwitch = KillSwitches.shared("oneButtonToRuleThemAll");

        counter.via(sharedKillSwitch.flow()).runWith(Sink.ignore(), mat);
        anotherCounter.via(sharedKillSwitch.flow()).runWith(Sink.ignore(), mat);

        system.scheduler().scheduleOnce(ofSeconds(3), () -> {
            sharedKillSwitch.shutdown(); // on va arrête les deux flow ici
            system.terminate();
        }, system.dispatcher());
    }

    private static void mergeHub() {

        final Source<Integer, Sink<Integer, NotUsed>> dynamicMerge = MergeHub.of(Integer.class);
        final Sink<Integer, NotUsed> materializedSink = dynamicMerge.to(Sink.foreach(API::println)).run(mat);

        //Source.range(1, 10).runWith(materializedSink, mat);
        counter.runWith(materializedSink, mat);
    }
}
