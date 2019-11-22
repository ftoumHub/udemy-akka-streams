package rockthejvm.part1_recap;

import akka.actor.*;

/**
 * Let’s look at the actor lifecycle control between actors with an example based on characters from The
 * Simpsons . Mr. Burns is the boss and has a nuclear power plant. He hires two employees, Homer Simpson
 * and Frank Grimes, but then only fires Frank Grimes.
 */
public class StartingActorsDemo {

    public static void main(String[] args) throws Exception {
        ActorSystem actorSystem = ActorSystem.create("StartingActorsSystem");

        ActorRef mrBurns = actorSystem.actorOf(Props.create(Boss.class), "MrBurns");

        mrBurns.tell(new Hire("HomerSimpson"), mrBurns);
        mrBurns.tell(new Hire("FrankGrimes"), mrBurns);

        // we wait some office cycles
        Thread.sleep(4000);
        // we look for Frank and we fire him
        System.out.println("Firing Frank Grimes ...");
        ActorSelection grimes = actorSystem.actorSelection("../user/MrBurns/FrankGrimes");

        // PoisonPill, an Akka special message
        grimes.tell(PoisonPill.getInstance(), ActorRef.noSender());
        System.out.println("now Frank Grimes is fired");

        //actorSystem.terminate();
    }

    static public class Hire {
        private String personName;

        public Hire(String personName){
            this.personName = personName;
        }
    }

    static public class Name {
        private String name;

        public Name(String name){
            this.name = name;
        }
    }

    public static class Boss extends AbstractActor {

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Hire.class, m -> {
                        // here the boss hire personnel
                        System.out.println(String.format("%s is about to be hired", m.personName));
                        ActorRef employee = context().actorOf(Employee.props(m.personName), m.personName);
                        employee.tell(new Name(m.personName), employee);
                    })
                    .matchAny(m -> System.out.println("The Boss can't handle this message.")) // cas par défaut
                    .build();
        }
    }

    public static class Employee extends AbstractActor {

        /**
         * Create Props for an actor of this type.
         * @param name The name to be passed to this actor’s constructor.
         * @return a Props for creating this actor, which can then be further configured
         *         (e.g. calling `.withDispatcher()` on it)
         */
        static Props props(String name) {
            // You need to specify the actual type of the returned actor
            // since Java 8 lambdas have some runtime type information erased
            return Props.create(Employee.class, () -> new Employee(name));
        }

        static String name = "Employee name"; // All the cats born with 7 lives

        public Employee(String name) {
            this.name = name;
        }

        @Override
        public void postStop() throws Exception {
            System.out.println("I'm ("+this.name+") and Mr. Burns fired me: "+ getSelf().path());
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Name.class, n -> this.name = n.name)
                    .matchAny(n ->
                            System.out.println(String.format("The Employee %s can't handle this message.", this.name)))
                    .build();
        }
    }
}
