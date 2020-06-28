package rockthejvm.part5_advanced;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.*;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import io.vavr.collection.Stream;

import java.util.concurrent.CompletionStage;

import static akka.stream.javadsl.Keep.right;
import static java.time.Duration.ofSeconds;

public class DynamicStreamHandling {

    static ActorSystem system = ActorSystem.create("DynamicStreamHandling");
    static ActorMaterializer materializer = ActorMaterializer.create(system);

    /**
     * Comment arrêter un flow volontairement
     */
    public static void main(String[] args) {

        // #1: Kill Switch

        // La valeur matérialisée est de type UniqueKillSwitch
        final Graph<FlowShape<Integer, Integer>, UniqueKillSwitch> killSwitchFlow = KillSwitches.<Integer>single();

        final Source<Integer, NotUsed> counter = Source.from(Stream.from(1)).throttle(1, ofSeconds(1)).log("counter");
        final Sink<Integer, CompletionStage<Done>> sink = Sink.<Integer>ignore();

        //final UniqueKillSwitch killSwitch = counter.viaMat(killSwitchFlow, right()).to(sink).run(materializer);

        //system.scheduler().scheduleOnce(ofSeconds(3), () -> killSwitch.shutdown(), system.dispatcher());

        final Source<Integer, NotUsed> anotherCounter = Source.from(Stream.from(1)).throttle(2, ofSeconds(1)).log("anotherCounter");
        final SharedKillSwitch sharedKillSwitch = KillSwitches.<Integer>shared("oneButtonToRuleThemAll");

        counter.via(sharedKillSwitch.flow()).runWith(Sink.ignore(), materializer);
        anotherCounter.via(sharedKillSwitch.flow()).runWith(Sink.ignore(), materializer);

        system.scheduler().scheduleOnce(ofSeconds(3), () -> {
            sharedKillSwitch.shutdown();
            system.terminate();
        }, system.dispatcher());

        // MergeHub

    }
}
