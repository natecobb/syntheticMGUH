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
import org.mitre.synthea.world.agents.PayerManager;
import org.mitre.synthea.world.concepts.HealthRecord.EncounterType;

/** Exercise shared liver modules through real CallSubmodule states in the stock engine. */
public class LiverModelTest {
  static final long START = 1577836800000L;
  static final long DAY = Utilities.convertTime("days", 1);
  static final String[] ETIOLOGIES = {"alcohol_cirrhosis", "acetaminophen", "non_acetaminophen", "cirrhosis"};
  static int checks;

  static void require(boolean condition, String message) {
    checks++;
    if (!condition) throw new AssertionError(message);
  }

  static void put(Person p, String key, Object value) { p.attributes.put("liver_" + key, value); }
  static Object get(Person p, String key) { return p.attributes.get("liver_" + key); }
  static double number(Person p, String key) { return ((Number) get(p, key)).doubleValue(); }
  static boolean flag(Person p, String key) { return Boolean.TRUE.equals(get(p, key)); }
  static int observations(Person p) { return p.record.encounters.get(0).observations.size(); }

  static Person person(String etiology, String course, double severity) {
    Person p = new Person(42L);
    p.attributes.put(Person.BIRTHDATE, START - Utilities.convertTime("years", 50));
    p.coverage.setPlanToNoInsurance((long) p.attributes.get(Person.BIRTHDATE));
    p.coverage.setPlanToNoInsurance(START + Utilities.convertTime("years", 1));
    p.record.encounterStart(START, EncounterType.INPATIENT);
    put(p, "etiology", etiology);
    put(p, "trajectory", course);
    put(p, "initial_severity", severity);
    return p;
  }

  // Completed parent history is removed to represent a new caller invocation. Submodule history
  // is managed by CallSubmodule itself, including across a blocking 24-hour delay.
  static void call(Person p, String path, long time) throws Exception {
    JsonObject definition = JsonParser.parseString("""
        {"name":"Liver fixture","gmf_version":2,"states":{
          "Initial":{"type":"Initial","direct_transition":"Call"},
          "Call":{"type":"CallSubmodule","submodule":"liver/%s","direct_transition":"Terminal"},
          "Terminal":{"type":"Terminal"}}}
        """.formatted(path)).getAsJsonObject();
    Module caller = new Module(definition, false);
    boolean done = caller.process(p, time);
    if (!done) {
      require(path.equals("advance"), "only advance may block");
      require(!caller.process(p, time + DAY - 1), "advance must wait a full day");
      done = caller.process(p, time + DAY);
    }
    require(done, path + " must complete");
    p.attributes.remove(caller.name);
  }

  static void initialize(Person p) throws Exception {
    call(p, "initialize", START);
    require(get(p, "error") == null, "valid initialization: " + get(p, "error"));
    require(observations(p) == 9, "nine initial numeric labs");
    require(number(p, "episode") == 1, "first episode identity");
  }

  public static void main(String[] args) throws Exception {
    Config.load(Path.of("synthea/synthea.properties").toFile());
    PayerManager.loadNoInsurance();
    Module.addModules(Path.of(args.length > 0 ? args[0] : "synthea/modules").toFile());
    JsonObject config = JsonParser.parseString(Files.readString(
        Path.of(args.length > 1 ? args[1] : "synthea/liver_model.json"))).getAsJsonObject();

    Map<String, Double> ast = new HashMap<>();
    Map<String, Double> bili = new HashMap<>();
    for (String etiology : ETIOLOGIES) {
      Person p = person(etiology, "recovery", 0.9);
      put(p, "renal_baseline", 1.8);
      initialize(p);
      ast.put(etiology, number(p, "ast"));
      bili.put(etiology, number(p, "bilirubin"));
      require(flag(p, "airway_support_required"), "severe encephalopathy needs support flag");
      double priorInr = number(p, "inr");
      double priorAst = number(p, "ast");
      double priorSodium = number(p, "sodium");
      for (int day = 0; day < 28; day++) {
        call(p, "advance", START + day * DAY);
        require(number(p, "inr") <= priorInr, etiology + " recovery INR");
        require(number(p, "ast") <= priorAst, etiology + " recovery AST");
        require(number(p, "sodium") >= priorSodium, etiology + " recovery sodium");
        priorInr = number(p, "inr");
        priorAst = number(p, "ast");
        priorSodium = number(p, "sodium");
      }
      require(number(p, "severity") == 0, etiology + " resolves acute dysfunction");
      require(!flag(p, "airway_support_required") && !flag(p, "renal_support_required")
          && !flag(p, "circulatory_support_required"), "resolved support flags");
      require(Math.abs(number(p, "creatinine") - 1.8) < 1e-9, "retain caller's chronic renal baseline");
      require(Math.abs(number(p, "inr") - (etiology.equals("alcohol_cirrhosis") || etiology.equals("cirrhosis") ? 1.4 : 1.0)) < 1e-9,
          "recovery retains cirrhosis baseline only for chronic disease");
      require(p.getSymptom("Confusion") == 0 && p.getSymptom("Jaundice") == 0,
          "recovery clears symptoms");
      call(p, "choose_outcome", START + 28 * DAY);
      require(get(p, "suggested_outcome").equals("recovery"), "resolved episode cannot draw death");
      require(observations(p) == 29 * 9, "exactly one panel per completed day");
      for (var observation : p.record.encounters.get(0).observations) {
        require(observation.value instanceof Double, "numeric CSV-compatible observation values");
        require(Double.isFinite(((Number) observation.value).doubleValue()), "finite labs");
      }
    }
    require(ast.get("acetaminophen") > ast.get("non_acetaminophen")
        && ast.get("non_acetaminophen") > ast.get("alcohol_cirrhosis"), "distinct injury patterns");
    require(bili.get("non_acetaminophen") > bili.get("acetaminophen"), "distinct bilirubin patterns");

    Person repeatA = person("acetaminophen", "recovery", 0.9);
    Person repeatB = person("acetaminophen", "recovery", 0.9);
    initialize(repeatA);
    initialize(repeatB);
    for (int day = 0; day < 3; day++) {
      call(repeatA, "advance", START + day * DAY);
      call(repeatB, "advance", START + day * DAY);
      for (String lab : config.getAsJsonObject("labs").keySet()) {
        require(get(repeatA, lab).equals(get(repeatB, lab)), "same seed reproduces " + lab);
      }
    }

    Person supported = person("acetaminophen", "stable", 0.9);
    initialize(supported);
    // Fix latent ranks to cover the co-occurring-organ branch independently of population rates.
    for (String organ : new String[]{"renal", "respiratory", "circulatory"}) {
      put(supported, organ + "_draw", 0.01);
    }
    call(supported, "advance", START);
    require(flag(supported, "renal_dysfunction") && flag(supported, "renal_support_required")
        && number(supported, "creatinine") > number(supported, "renal_baseline"),
        "renal dysfunction, elevated creatinine, and support agree");
    require(flag(supported, "respiratory_dysfunction") && flag(supported, "airway_support_required")
        && flag(supported, "circulatory_dysfunction") && flag(supported, "circulatory_support_required"),
        "extrahepatic organ dysfunction can co-occur with corresponding support");
    put(supported, "trajectory", "recovery");
    for (int day = 1; day < 9; day++) call(supported, "advance", START + day * DAY);
    require(!flag(supported, "renal_dysfunction") && !flag(supported, "renal_support_required")
        && !flag(supported, "respiratory_dysfunction") && !flag(supported, "airway_support_required")
        && !flag(supported, "circulatory_dysfunction") && !flag(supported, "circulatory_support_required"),
        "organ dysfunction and support resolve with recovery");

    for (String etiology : ETIOLOGIES) {
      Person p = person(etiology, "deterioration", 0.8);
      initialize(p);
      double initialInr = number(p, "inr");
      double initialAst = number(p, "ast");
      for (int day = 0; day < 4; day++) call(p, "advance", START + day * DAY);
      require(number(p, "inr") > initialInr && number(p, "ast") < initialAst,
          etiology + " falling AST must coexist with worsening liver function");
      require(number(p, "encephalopathy_grade") == 4, "worsening encephalopathy");
      require(p.getSymptom("Confusion") == 90, "confusion follows functional severity");
      double turningAst = number(p, "ast");
      double turningInr = number(p, "inr");
      put(p, "trajectory", "recovery");
      call(p, "advance", START + 4 * DAY);
      require(number(p, "ast") == turningAst && number(p, "inr") == turningInr,
          "changing course preserves the current injury and functional state");
      call(p, "advance", START + 5 * DAY);
      require(number(p, "ast") < turningAst && number(p, "inr") < turningInr,
          "new recovery phase improves from the turning point");
    }

    Person stable = person("acetaminophen", "stable", 0.8);
    initialize(stable);
    double stableInr = number(stable, "inr");
    call(stable, "assess", START);
    call(stable, "choose_outcome", START);
    require(observations(stable) == 9, "same-day assessment is idempotent");
    require(get(stable, "suggested_outcome").equals("continue"), "no zero-duration outcome draw");
    call(stable, "advance", START);
    require(number(stable, "inr") == stableInr, "stable trajectory");
    call(stable, "choose_outcome", START + DAY);
    Object chosen = get(stable, "suggested_outcome");
    call(stable, "choose_outcome", START + DAY);
    require(chosen.equals(get(stable, "suggested_outcome")), "one cached outcome per day");
    put(stable, "trajectory", "recovery");
    call(stable, "advance", START + DAY);
    require(number(stable, "severity") == 0.8, "course change starts at current severity");
    call(stable, "advance", START + 2 * DAY);
    require(number(stable, "severity") < 0.8, "changed course then improves");

    Person postop = person("alcohol_cirrhosis", "postoperative", 0.8);
    put(postop, "transplant_status", "recipient");
    initialize(postop);
    require(get(postop, "profile").equals("postoperative"), "recipient uses postoperative profile");
    for (int day = 0; day < 14; day++) call(postop, "advance", START + day * DAY);
    require(number(postop, "inr") == 1.0 && number(postop, "albumin") == 4.2,
        "postoperative recovery does not restore diseased native-liver baseline");

    // The installed engine must honor every configured named probability, with no residual
    // probability falling silently into its final branch (the original issue #2 failure mode).
    class DrawPerson extends Person {
      double draw;
      DrawPerson() { super(42L); }
      @Override public double rand() { return draw; }
    }
    DrawPerson draw = new DrawPerson();
    State choose = Module.getModuleByPath("liver/choose_outcome").getState("Choose");
    for (var profile : config.getAsJsonObject("profiles").entrySet()) {
      for (var band : profile.getValue().getAsJsonObject().getAsJsonObject("outcomes").entrySet()) {
        JsonObject probabilities = band.getValue().getAsJsonObject();
        probabilities.entrySet().forEach(e -> put(draw, "p_" + e.getKey(), e.getValue().getAsDouble()));
        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i < 10000; i++) {
          draw.draw = (i + 0.5) / 10000;
          counts.merge(choose.transition(draw, START), 1, Integer::sum);
        }
        for (var entry : probabilities.entrySet()) {
          require(counts.getOrDefault("Outcome_" + entry.getKey() + "_0", 0)
              == Math.round(entry.getValue().getAsDouble() * 10000),
              profile.getKey() + "/" + band.getKey() + " exhaustive engine probabilities");
        }
      }
    }

    // ICU calibration must not leak into generic liver consumers or evaluation/waiting days.
    for (String etiology : ETIOLOGIES) {
      Person calibration = person(etiology, "stable", 0.8);
      initialize(calibration);
      var base = config.getAsJsonObject("profiles").getAsJsonObject(etiology)
          .getAsJsonObject("outcomes").getAsJsonObject("severe");
      double pEval = base.get("transplant_evaluation").getAsDouble();
      require(Math.abs(number(calibration, "p_transplant_evaluation") - pEval) < 1e-12,
          "generic consumer retains base evaluation probability");
      put(calibration, "care_active", true);
      put(calibration, "level_of_care", "icu");
      put(calibration, "evaluation_done", false);
      call(calibration, "advance", START);
      double factor = config.getAsJsonObject("care").get("evaluation_odds_multiplier").getAsDouble();
      double total = 1 + pEval * (factor - 1);
      double sum = 0;
      for (var entry : base.entrySet()) {
        double expected = entry.getValue().getAsDouble() * (entry.getKey().equals("transplant_evaluation") ? factor : 1) / total;
        double actual = number(calibration, "p_" + entry.getKey());
        require(Math.abs(actual - expected) < 1e-12, "ICU probabilities scale and normalize");
        sum += actual;
      }
      require(Math.abs(sum - 1) < 1e-12, "calibrated probabilities exhaust one daily draw");
      put(calibration, "evaluation_done", true);
      call(calibration, "advance", START + DAY);
      require(Math.abs(number(calibration, "p_transplant_evaluation") - pEval) < 1e-12,
          "evaluation and waiting keep their original competing probabilities");
      put(calibration, "evaluation_done", false);
      put(calibration, "entry_path", "cirrhosis");
      call(calibration, "advance", START + 2 * DAY);
      double cirrhosisFactor = config.getAsJsonObject("cirrhosis_integration").get("evaluation_odds_multiplier").getAsDouble();
      require(Math.abs(number(calibration, "p_transplant_evaluation")
          - pEval * cirrhosisFactor / (1 + pEval * (cirrhosisFactor - 1))) < 1e-12,
          "integrated ICU entry selects its own fitted multiplier");
      put(calibration, "level_of_care", "ward");
      call(calibration, "advance", START + 3 * DAY);
      require(Math.abs(number(calibration, "p_transplant_evaluation") - pEval) < 1e-12,
          "cirrhosis calibration never changes ward escalation probability");
      call(calibration, "finish", START + 4 * DAY);
      call(calibration, "initialize", START + 5 * DAY);
      require(get(calibration, "entry_path").equals("direct_icu"), "new episodes cannot inherit a stale calibration context");
    }

    Person invalid = person("alcohol_cirrhosis", "postoperative", 0.5);
    call(invalid, "initialize", START);
    require(get(invalid, "error") != null && !flag(invalid, "active")
        && get(invalid, "episode") == null && observations(invalid) == 0, "invalid init has no episode/labs");
    put(invalid, "trajectory", "stable");
    initialize(invalid);
    call(invalid, "initialize", START);
    require(number(invalid, "episode") == 1 && observations(invalid) == 9, "no overlapping episodes");
    call(invalid, "assess", START);
    require(get(invalid, "error") == null, "valid retry clears error");
    call(invalid, "finish", START);
    require(!flag(invalid, "active") && invalid.getSymptom("Confusion") == 0, "finish ends ownership");
    call(invalid, "advance", START);
    require(number(invalid, "elapsed_days") == 0 && observations(invalid) == 9, "finished episode cannot advance");
    call(invalid, "initialize", START + DAY);
    require(number(invalid, "episode") == 2 && observations(invalid) == 18, "subsequent episode identity");
    System.out.println("PASS: " + checks + " liver engine assertions");
  }
}
