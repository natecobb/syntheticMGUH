import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import org.mitre.synthea.engine.Module;
import org.mitre.synthea.helpers.Config;
import org.mitre.synthea.helpers.DefaultRandomNumberGenerator;
import org.mitre.synthea.helpers.Utilities;
import org.mitre.synthea.world.agents.PayerManager;
import org.mitre.synthea.world.agents.Person;
import org.mitre.synthea.world.agents.Provider;
import org.mitre.synthea.world.concepts.HealthRecord;
import org.mitre.synthea.world.concepts.HealthRecord.EncounterType;
import org.mitre.synthea.world.geography.Location;

/** Executes unforced GMF episodes in the stock engine, retaining deaths and censored cases. */
public class LiverCalibration {
  static final long START = 1577836800000L;
  static final long DAY = Utilities.convertTime("days", 1);
  static final String[] ETIOLOGIES = {"alcohol_cirrhosis", "acetaminophen", "non_acetaminophen", "cirrhosis"};
  static Provider mguh;
  static boolean hasCode(HealthRecord.Entry entry, String code) {
    return entry.codes.stream().anyMatch(c -> c.code.equals(code));
  }
  static List<HealthRecord.Entry> entries(Person p, String code) {
    List<HealthRecord.Entry> found = new ArrayList<>();
    for (var encounter : p.record.encounters) {
      for (var entry : encounter.procedures) if (hasCode(entry, code)) found.add(entry);
      for (var entry : encounter.medications) if (hasCode(entry, code)) found.add(entry);
    }
    return found;
  }
  static Person patient(long seed, int age, String sex) {
    Person p = new Person(seed);
    p.attributes.put(Person.BIRTHDATE, START - Utilities.convertTime("years", age));
    p.attributes.put(Person.GENDER, sex);
    p.attributes.put(Person.RACE, "white");
    p.attributes.put(Person.ETHNICITY, "nonhispanic");
    p.attributes.put(Person.INCOME, 50000);
    p.attributes.put(Person.OCCUPATION_LEVEL, 1.0);
    p.coverage.setPlanToNoInsurance((long) p.attributes.get(Person.BIRTHDATE));
    p.coverage.setPlanToNoInsurance(START + Utilities.convertTime("years", 4));
    for (EncounterType type : EncounterType.values()) p.setProvider(type, mguh);
    return p;
  }
  static void put(Person p, String key, Object value) { p.attributes.put("liver_" + key, value); }
  public static void main(String[] args) throws Exception {
    Config.load(Path.of("synthea/synthea.properties").toFile());
    PayerManager.loadNoInsurance();
    Provider.loadProviders(new Location("District of Columbia", "Washington"), new DefaultRandomNumberGenerator(3003L));
    mguh = Provider.getProviderList().stream().filter(p -> "1427145176".equals(p.npi)).findFirst()
        .orElseThrow(() -> new IllegalStateException("MGUH provider NPI 1427145176 is required"));
    Path folder = Path.of(args[0]);
    JsonObject config = JsonParser.parseString(Files.readString(folder.resolve("scenario.json"))).getAsJsonObject();
    JsonObject cohort = config.getAsJsonObject("cohort");
    JsonObject simulation = config.getAsJsonObject("simulation");
    boolean integrated = config.has("entry_path") && config.get("entry_path").getAsString().equals("cirrhosis");
    Module.addModules(folder.resolve("modules").toFile());
    JsonObject driver = JsonParser.parseString(Files.readString(folder.resolve("driver.json"))).getAsJsonObject();
    Module template = new Module(driver, false);
    int cases = simulation.get("regional_cases_per_seed").getAsInt();
    int days = simulation.get("observation_days").getAsInt();
    Gson gson = new Gson();
    int shard = args.length > 2 ? Integer.parseInt(args[2]) : 0;
    int shards = args.length > 3 ? Integer.parseInt(args[3]) : 1;
    String suffix = args.length > 1 ? "-" + args[1] + "-" + shard : "";
    try (var output = Files.newBufferedWriter(folder.resolve("episodes" + suffix + ".jsonl"))) {
      for (var seedValue : config.getAsJsonArray("seeds")) {
        long seed = seedValue.getAsLong();
        if (args.length > 1 && seed != Long.parseLong(args[1])) continue;
        SplittableRandom cohortRandom = new SplittableRandom(seed);
        int referrals = 0, admissions = 0, transplants = 0;
        for (int index = 0; index < cases; index++) {
          long personSeed = cohortRandom.nextLong();
          double draw = cohortRandom.nextDouble(), cumulative = 0;
          String etiology = ETIOLOGIES[ETIOLOGIES.length - 1];
          for (String name : ETIOLOGIES) {
            var weights = cohort.getAsJsonObject("etiology_weights");
            cumulative += weights.has(name) ? weights.get(name).getAsDouble() : 0;
            if (draw < cumulative) { etiology = name; break; }
          }
          double low = cohort.getAsJsonArray("initial_severity_range").get(0).getAsDouble();
          double high = cohort.getAsJsonArray("initial_severity_range").get(1).getAsDouble();
          double severity = low + cohortRandom.nextDouble() * (high - low);
          String sex = cohortRandom.nextBoolean() ? "F" : "M";
          if (index % shards != shard) continue;
          Person p = patient(personSeed, cohort.get("age_years").getAsInt(), sex);
          put(p, "case_id", seed + ":" + index);
          put(p, "etiology", etiology);
          put(p, "initial_severity", severity);
          put(p, "trajectory", cohort.get("trajectory").getAsString());
          put(p, "preexisting_cirrhosis", etiology.equals("alcohol_cirrhosis") || etiology.equals("cirrhosis"));
          put(p, "renal_baseline", 0.9);
          put(p, "transplant_status", "none");
          put(p, "alcohol_use", etiology.equals("alcohol_cirrhosis") || etiology.equals("cirrhosis") ? "unknown" : "not_applicable");
          if (integrated) put(p, "cirrhosis_etiology", etiology.equals("alcohol_cirrhosis") ? "alcohol" : "unspecified");
          Module caller = template.clone();
          long time = START;
          boolean done = caller.process(p, time);
          for (int tick = 1; !done && tick <= days * 4; tick++) {
            time = START + tick * DAY / 4;
            done = caller.process(p, time);
          }
          if (p.attributes.get("liver_error") != null) throw new IllegalStateException(p.attributes.get("liver_error").toString());
          boolean referred = Boolean.TRUE.equals(p.attributes.get("liver_referred_to_mguh"));
          var icus = p.record.encounters.stream().filter(e -> hasCode(e, "305351004")).toList();
          var surgeries = entries(p, "18027006").stream().filter(e -> e.stop > e.start && e.stop <= timeAtEnd(p, days)).toList();
          if (icus.size() > 1 || surgeries.size() > 1) throw new IllegalStateException("Duplicate index admission or transplant");
          boolean transplanted = !surgeries.isEmpty();
          JsonObject row = new JsonObject();
          row.addProperty("case_id", seed + ":" + index);
          row.addProperty("seed", seed);
          row.addProperty("etiology", etiology);
          row.addProperty("initial_severity", ((Number) p.attributes.get("liver_initial_severity")).doubleValue());
          row.addProperty("entry_path", integrated ? "cirrhosis" : "direct_icu");
          row.addProperty("referred", referred);
          row.addProperty("icu_admitted", !icus.isEmpty());
          if (!icus.isEmpty()) {
            boolean wardFirst = p.record.encounters.stream().anyMatch(e -> hasCode(e, "305342007") && e.start < icus.get(0).start);
            row.addProperty("icu_entry_route", wardFirst ? "from_ward" : "direct");
            row.addProperty("days_to_icu", (icus.get(0).start - START) / (double) DAY);
          }
          row.addProperty("transplanted", transplanted);
          row.addProperty("dead", !p.alive(time));
          row.addProperty("complete", done);
          row.addProperty("care_outcome", String.valueOf(p.attributes.getOrDefault("liver_care_outcome", "not_referred")));
          row.addProperty("icu_closed", !icus.isEmpty() && icus.get(0).stop > 0);
          if (!icus.isEmpty()) row.addProperty("icu_days", ((icus.get(0).stop > 0 ? icus.get(0).stop : time) - icus.get(0).start) / (double) DAY);
          if (transplanted) row.addProperty("days_to_transplant", (surgeries.get(0).start - icus.get(0).start) / (double) DAY);
          row.addProperty("ventilation", !entries(p, "40617009").isEmpty());
          row.addProperty("rrt", !entries(p, "233585000").isEmpty());
          row.addProperty("vasopressors", !entries(p, "373112").isEmpty());
          output.write(gson.toJson(row));
          output.newLine();
          if (referred) referrals++;
          if (!icus.isEmpty()) admissions++;
          if (transplanted) transplants++;
          if ((index + 1) % 1000 == 0) System.out.println("seed=" + seed + " processed=" + (index + 1));
        }
        System.out.println("seed=" + seed + " regional=" + cases + " referred=" + referrals + " ICU=" + admissions + " transplanted=" + transplants);
      }
    }
  }
  static long timeAtEnd(Person p, int days) {
    // Do not count a scheduled procedure beyond the observation window or after death.
    long end = START + days * DAY;
    Object death = p.attributes.get(Person.DEATHDATE);
    return death == null ? end : Math.min(end, ((Number) death).longValue());
  }
}
