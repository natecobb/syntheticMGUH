import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.mitre.synthea.engine.Module;
import org.mitre.synthea.engine.State;
import org.mitre.synthea.helpers.Config;
import org.mitre.synthea.helpers.Utilities;
import org.mitre.synthea.world.agents.Person;

/** Regression checks using the installed Synthea engine, without generating a population. */
public class CirrhosisModuleTest {
  private static int failures;

  private static class FixedDrawPerson extends Person {
    private double draw;

    FixedDrawPerson() {
      super(42L);
      attributes.put(Person.BIRTHDATE, -Utilities.convertTime("years", 50));
    }

    @Override
    public double rand() {
      return draw;
    }
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }

  private static void check(String name, Runnable test) {
    try {
      test.run();
      System.out.println("PASS: " + name);
    } catch (AssertionError error) {
      failures++;
      System.err.println("FAIL: " + name + ": " + error.getMessage());
    }
  }

  public static void main(String[] args) throws Exception {
    Path modules = Path.of(args.length == 0 ? "synthea/modules" : args[0]);
    JsonObject definition = JsonParser.parseString(
        Files.readString(modules.resolve("cirrhosis.json"))).getAsJsonObject();
    JsonObject states = definition.getAsJsonObject("states");

    Config.load(Path.of("synthea/synthea.properties").toFile());
    // Use the same local-module loader as App's -d flag; exercise the JAR's real transitions.
    Module.addModules(modules.toFile());
    Module module = Module.getModuleByPath("cirrhosis");
    State management = module.getState("Manage_Delay");

    check("cirrhosis distributions are exhaustive", () -> {
      for (Map.Entry<String, JsonElement> entry : states.entrySet()) {
        JsonObject state = entry.getValue().getAsJsonObject();
        if (!state.has("distributed_transition")) {
          continue;
        }
        double total = 0;
        for (JsonElement element : state.getAsJsonArray("distributed_transition")) {
          JsonObject branch = element.getAsJsonObject();
          double probability = branch.get("distribution").getAsDouble();
          require(Double.isFinite(probability) && probability >= 0 && probability <= 1,
              entry.getKey() + " has an invalid probability: " + probability);
          require(states.has(branch.get("transition").getAsString()),
              entry.getKey() + " references a missing state");
          total += probability;
        }
        require(Math.abs(total - 1.0) < 1e-12,
            entry.getKey() + " probabilities sum to " + total + ", not 1.0");
      }
    });

    check("installed engine routes 66/10/4/20 percent of management decisions", () -> {
      FixedDrawPerson person = new FixedDrawPerson();
      Map<String, Integer> actual = new HashMap<>();
      // Midpoints avoid floating-point boundary ambiguity and statistical sampling noise.
      for (int i = 0; i < 10000; i++) {
        person.draw = (i + 0.5) / 10000.0;
        actual.merge(management.transition(person, 0L), 1, Integer::sum);
      }
      Map<String, Integer> expected = Map.of(
          "Followup_Encounter", 6600, "Complication_Encounter", 1000,
          "Disease_Death", 400, "Manage_Delay", 2000);
      require(actual.equals(expected), "actual branch counts: " + actual);
    });

    // Start at management to isolate continuation from age gates and encounter/provider setup.
    JsonObject loopDefinition = definition.deepCopy();
    JsonObject initial = loopDefinition.getAsJsonObject("states").getAsJsonObject("Initial");
    initial.remove("distributed_transition");
    initial.addProperty("direct_transition", "Manage_Delay");
    Module loop = new Module(loopDefinition, false);
    check("continued management starts a fresh delay without a clinical event", () -> {
      FixedDrawPerson person = new FixedDrawPerson();
      person.draw = 0.9;
      require(management.transition(person, 0L).equals("Manage_Delay"),
          "the residual 20% must continue management");
      require(!loop.process(person, 0L), "management must wait before deciding");
      for (int i = 0; i < 2; i++) {
        State.Delay current = (State.Delay) person.history.get(0);
        long nextDecision = current.next;
        require(!loop.process(person, nextDecision), "continuation must remain active");
        State.Delay next = (State.Delay) person.history.get(0);
        require(next != current, "the completed delay must not be reused");
        long interval = next.next - nextDecision;
        require(interval >= Utilities.convertTime("months", 4)
                && interval <= Utilities.convertTime("months", 10),
            "continuation must schedule another 4-10 month interval");
        require(person.alive(nextDecision), "continuation must not cause death");
        require(person.record.encounters.isEmpty(), "continuation must not create an encounter");
      }
    });

    if (failures > 0) {
      throw new AssertionError(failures + " cirrhosis regression check(s) failed");
    }
  }
}
