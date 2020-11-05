package rockthejvm.part3_graphs;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.function.Function;
import akka.stream.*;
import akka.stream.javadsl.*;
import io.vavr.collection.List;

import static io.vavr.API.List;
import static io.vavr.API.printf;

public class BidirectionalFlows {

    private static final ActorSystem system = ActorSystem.create("BidirectionalFlows");
    private static final ActorMaterializer mat = ActorMaterializer.create(system);

    public static void main(String[] args) throws Exception {
        //println(encrypt(3, "Akka"));
        //println(decrypt(3, "Dnnd"));

        final Graph<BidiShape<String, String, String, String>, NotUsed> bidiCryptoStaticGraph = GraphDSL.create(builder -> {
                    final FlowShape<String, String> encryptionFlowShape = builder.add(Flow.<String>create().map(encrypt.apply(3)));
                    final FlowShape<String, String> decryptionFlowShape = builder.add(Flow.<String>create().map(decrypt.apply(3)));

                    return BidiShape.fromFlows(encryptionFlowShape, decryptionFlowShape);
                }
        );

        final List<String> unencryptedStrings = List("akka", "is", "awesome", "testing", "bidirectional", "flows");
        final Source<String, NotUsed> unencryptedSource = Source.from(unencryptedStrings);
        final Source<String, NotUsed> decryptedSource = Source.from(unencryptedStrings.map(encryptf.apply(3)));

        final RunnableGraph<NotUsed> cryptoBidiGraph = RunnableGraph.fromGraph(
                GraphDSL.create(builder -> {
                            // step 2: declaring components
                            final SourceShape<String> unencryptedSourceShape = builder.add(unencryptedSource);
                            final SourceShape<String> encryptedSourceShape = builder.add(decryptedSource);
                            final BidiShape<String, String, String, String> bidi = builder.add(bidiCryptoStaticGraph);
                            final SinkShape<String> encryptedSinkShape = builder.add(Sink.foreach(s -> printf("Encrypted: %s\n", s)));
                            final SinkShape<String> decryptedSinkShape = builder.add(Sink.foreach(s -> printf("Decrypted: %s\n", s)));

                            // step 3: tying them together
                            builder.from(unencryptedSourceShape).toInlet(bidi.in1());
                            builder.from(bidi.out1()).to(encryptedSinkShape);
                            builder.from(encryptedSourceShape).toInlet(bidi.in2());
                            builder.from(bidi.out2()).to(decryptedSinkShape);

                            // step 4
                            return ClosedShape.getInstance();
                        }
                )
        );
        cryptoBidiGraph.run(mat);
    }

    private static Function<Integer, Function<String, String>> encrypt = n -> string -> encrypt(n, string);

    private static Function<Integer, Function<String, String>> decrypt = n -> string -> decrypt(n, string);

    private static java.util.function.Function<Integer, java.util.function.Function<String, String>> encryptf = n -> string -> encrypt(n, string);

    private static String encrypt(Integer n, String string) {
        StringBuffer buff = new StringBuffer();
        for (char c : string.toCharArray()) {
            buff.append((char) (c + n));
        }
        return buff.toString();
    }

    private static String decrypt(Integer n, String string) {
        StringBuffer buff = new StringBuffer();
        for (char c : string.toCharArray()) {
            buff.append((char) (c - n));
        }
        return buff.toString();
    }
}