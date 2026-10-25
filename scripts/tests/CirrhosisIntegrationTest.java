import com.google.gson.JsonParser;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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

/** Full parent journeys: diagnosis, decompensation, ICU, recovery/recurrence or transplant. */
public class CirrhosisIntegrationTest {
  static final long START = 1577836800000L;
  static final long DAY = Utilities.convertTime("days", 1);
  static int checks;
  static void require(boolean condition, String message) {
    checks++;
    if (!condition) throw new AssertionError(message);
  }
  static Object get(Person p, String key) { return p.attributes.get("liver_" + key); }
  static boolean flag(Person p, String key) { return Boolean.TRUE.equals(get(p, key)); }
  static double num(Person p, String key) {
    Object value = get(p, key);
    return value == null ? 0 : ((Number) value).doubleValue();
  }
  static boolean hasCode(HealthRecord.Entry e, String code) { return e.codes.stream().anyMatch(c -> code.equals(c.code)); }
  static List<HealthRecord.Entry> entries(Person p, String code) {
    List<HealthRecord.Entry> result = new ArrayList<>();
    for (var encounter : p.record.encounters) {
      List<HealthRecord.Entry> all = new ArrayList<>();
      all.addAll(encounter.conditions); all.addAll(encounter.procedures); all.addAll(encounter.medications);
      all.addAll(encounter.careplans); all.addAll(encounter.devices);
      for (var entry : all) if (hasCode(entry, code)) result.add(entry);
    }
    return result;
  }
  static List<HealthRecord.Encounter> encounters(Person p, String code) {
    return p.record.encounters.stream().filter(e -> hasCode(e, code)).toList();
  }
  static Person patient() {
    Person p = new Person(3003L);
    p.attributes.put(Person.BIRTHDATE, START - Utilities.convertTime("years", 50));
    p.attributes.put(Person.GENDER, "F"); p.attributes.put(Person.RACE, "white");
    p.attributes.put(Person.ETHNICITY, "nonhispanic"); p.attributes.put(Person.INCOME, 50000);
    p.attributes.put(Person.OCCUPATION_LEVEL, 1.0);
    p.coverage.setPlanToNoInsurance((long) p.attributes.get(Person.BIRTHDATE));
    p.coverage.setPlanToNoInsurance(START + Utilities.convertTime("years", 5));
    for (EncounterType type : EncounterType.values()) p.setProvider(type, Provider.getProviderList().get(0));
    return p;
  }
  static HealthRecord.Entry externalCondition(Person p, String code, long time) {
    p.record.encounterStart(time, EncounterType.AMBULATORY);
    var entry = p.record.conditionStart(time, code);
    entry.codes.add(new HealthRecord.Code("SNOMED-CT", code, "External diagnosis"));
    p.record.encounterEnd(time + DAY / 24, EncounterType.AMBULATORY);
    return entry;
  }
  public static void main(String[] args) throws Exception {
    Config.load(Path.of("synthea/synthea.properties").toFile());
    PayerManager.loadNoInsurance();
    Provider.loadProviders(new Location("District of Columbia", "Washington"), new DefaultRandomNumberGenerator(3003L));
    Path folder = Path.of(args[0]);
    Module.addModules(folder.toFile());
    var cases = JsonParser.parseString(Files.readString(folder.resolve("cases.json"))).getAsJsonArray();
    for (var item : cases) {
      var fixture = item.getAsJsonObject();
      String scenario = fixture.get("scenario").getAsString();
      Person p = patient();
      HealthRecord.Entry external = null;
      if (scenario.equals("external_cirrhosis") || scenario.equals("existing_recipient")) {
        external = externalCondition(p, scenario.equals("external_cirrhosis") ? "19943007" : "737297006", START - DAY);
      }
      if (scenario.equals("external_medication")) {
        p.record.encounterStart(START - DAY, EncounterType.AMBULATORY);
        external = p.record.medicationStart(START - DAY, "313988", true);
        external.codes.add(new HealthRecord.Code("RxNorm", "313988", "External furosemide"));
        p.record.encounterEnd(START - DAY / 2, EncounterType.AMBULATORY);
      }
      Module parent = new Module(JsonParser.parseString(Files.readString(Path.of(fixture.get("path").getAsString()))).getAsJsonObject(), false);
      boolean done = parent.process(p, START), returned = false;
      long time = START;
      int targetEpisodes = scenario.equals("recurrence") ? 2 : 1;
      boolean graftInjected = false;
      for (int tick = 1; !done && tick <= 6000; tick++) {
        time = START + tick * DAY / 4;
        if (scenario.equals("cancel_death") && !graftInjected) {
          externalCondition(p, "737297006", time);
          p.record.conditionEnd(time, "19943007");
          p.attributes.put("liver_transplant_status", "recipient");
          graftInjected = true;
        }
        if (scenario.equals("busy_owner") && time <= START + 3 * DAY) {
          p.attributes.put("liver_active", true);
          p.attributes.put("liver_care_active", true);
          p.attributes.put("liver_etiology", "external_owner");
        } else if (scenario.equals("busy_owner") && time == START + 3 * DAY + DAY / 4) {
          p.attributes.put("liver_active", false);
          p.attributes.put("liver_care_active", false);
        }
        done = parent.process(p, time);
        require(get(p, "error") == null, scenario + " contract error: " + get(p, "error"));
        if (scenario.equals("busy_owner") && time <= START + 3 * DAY) {
          require(encounters(p, "50849002").isEmpty() && get(p, "etiology").equals("external_owner"), "wait without overwriting another liver owner");
        }
        if (num(p, "episode") >= targetEpisodes && !flag(p, "care_active") && !flag(p, "active")
            && !p.history.isEmpty() && p.history.get(0).name.equals("Manage_Delay")) {
          returned = true; break;
        }
        if (scenario.equals("routine_followup") && p.record.encounters.size() >= 2
            && p.history.get(0).name.equals("Manage_Delay")) { returned = true; break; }
      }
      require(done || returned, scenario + " must finish or return to real chronic management");
      if (scenario.equals("routine_followup")) {
        require(returned && num(p, "episode") == 0 && entries(p, "19943007").size() == 1, "routine management remains available without acute disease");
        for (String med : new String[]{"313096", "313988", "856519", "391937"}) {
          require(entries(p, med).size() == 1 && entries(p, med).get(0).stop == 0, "refill does not duplicate an active chronic prescription");
        }
        System.out.println("PASS: cirrhosis " + scenario); continue;
      }
      if (scenario.equals("chronic_death")) {
        require(done && !p.alive(time) && num(p, "episode") == 0, "native-liver death remains reachable without a graft");
        for (String med : new String[]{"313096", "313988", "856519"}) require(entries(p, med).get(0).stop > 0 && entries(p, med).get(0).stop <= time, "chronic death ends owned medications");
        System.out.println("PASS: cirrhosis " + scenario); continue;
      }
      if (scenario.equals("external_cirrhosis") || scenario.equals("existing_recipient")) {
        require(done && num(p, "episode") == 0 && external.stop == 0, "no duplicate course for an existing disease owner or graft");
        require(p.record.encounters.size() == 1, "no new encounters for excluded parent onset");
        System.out.println("PASS: cirrhosis " + scenario); continue;
      }
      if (scenario.equals("cancel_death")) {
        require(done && p.alive(time) && num(p, "episode") == 0, "later graft cancels native-liver scheduled death");
        require(entries(p, "313096").get(0).stop > 0, "cancelled native-liver course releases its prescriptions");
        System.out.println("PASS: cirrhosis " + scenario); continue;
      }
      require(get(p, "etiology").equals(scenario.equals("alcohol_known") ? "alcohol_cirrhosis" : "cirrhosis"), "preserve mixed-cause diagnosis unless alcohol cause is explicit");
      require(!flag(p, "active") && !flag(p, "care_active") && !flag(p, "cirrhosis_decompensation_active"), "release acute ownership");
      require(!p.hasCurrentEncounter(), "no encounter left open after return");
      boolean fatal = scenario.equals("ward_death") || scenario.equals("icu_death") || scenario.equals("postoperative_death");
      boolean transplanted = scenario.equals("transplant_survival") || scenario.equals("postoperative_death") || scenario.equals("external_medication");
      boolean icu = scenario.startsWith("icu_") || transplanted || scenario.equals("ward_deterioration");
      require(p.alive(time) != fatal, "expected survival " + scenario);
      require(entries(p, "18027006").size() == (transplanted ? 1 : 0), "expected transplant count");
      require(encounters(p, "305351004").size() == (icu ? 1 : 0), "ICU entry follows severity/escalation");
      require(encounters(p, "50849002").size() == targetEpisodes, "one ED assessment for each complication");
      require(num(p, "episode") == targetEpisodes, "ED to ward/ICU continues the same episode identity");
      require(entries(p, "19943007").size() == 1, "no duplicate native cirrhosis condition");
      require(entries(p, "197270009").isEmpty(), "chronic decompensation is never labeled de novo ALF");
      var cirrhosis = entries(p, "19943007").get(0);
      if (transplanted) {
        var surgery = entries(p, "18027006").get(0);
        require(cirrhosis.stop == surgery.stop, "graft replaces native cirrhosis");
        for (String med : new String[]{"313096", "313988", "856519", "391937"}) {
          for (var order : entries(p, med)) if (order != external) require(order.stop > 0 && order.stop <= surgery.start, "owned native-liver medicines stop before graft");
        }
        if (!fatal) require(num(p, "followup_count") == 5 && done && !returned, "recipient follows graft care and leaves chronic parent");
      } else if (!fatal) {
        require(returned && cirrhosis.stop == 0, "medical recovery returns to management with cirrhosis intact");
        require(entries(p, "313096").stream().anyMatch(e -> e.stop == 0), "chronic medication resumes after recovery");
        require(entries(p, "13920009").stream().allMatch(e -> e.stop > 0), "acute encephalopathy resolves between episodes");
        require(entries(p, "716203000").stream().allMatch(e -> e.stop > 0), "decompensation resolves before recurrence");
      }
      if (external != null) require(external.stop == 0, "preserve another module's medication ownership");
      long death = fatal ? ((Number) p.attributes.get(Person.DEATHDATE)).longValue() : Long.MAX_VALUE;
      for (int index = 1; index < p.record.encounters.size(); index++) {
        require(p.record.encounters.get(index - 1).stop <= p.record.encounters.get(index).start,
            "sequential care encounters do not overlap: " + scenario);
      }
      for (var encounter : p.record.encounters) {
        require(encounter.stop >= encounter.start && encounter.stop <= death, "encounter chronology and death boundary");
        for (var lab : encounter.observations) require(lab.start >= encounter.start && lab.start <= encounter.stop, "labs belong within their encounter");
        for (var operation : encounter.procedures) require(operation.stop <= encounter.stop && operation.stop <= death, "procedure chronology");
      }
      if (fatal) for (String med : new String[]{"313096", "313988", "856519", "391937"}) {
        for (var order : entries(p, med)) require(order.stop > 0 && order.stop <= death, "no owned chronic medication survives death");
      }
      if (done) {
        int count = p.record.encounters.size();
        parent.process(p, time + 365 * DAY);
        require(p.record.encounters.size() == count, "terminal parent never resumes chronic care");
      }
      System.out.println("PASS: cirrhosis " + scenario);
    }
    System.out.println("PASS: " + cases.size() + " integrated cirrhosis journeys; " + checks + " assertions");
  }
}
