package libs;

import java.time.Duration;
import java.time.temporal.TemporalUnit;

import static io.vavr.API.Try;

public interface Await {


    static void await(long time, TemporalUnit unit) {
        await(Duration.of(time, unit));
    }

    static void await(Duration duration) {
        Try(() -> {
            Thread.sleep(duration.toMillis());
            return Unit.unit();
        });
    }

}
