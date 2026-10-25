import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
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

/** Real stock-engine care journeys; fixtures force decisions without production test switches. */
public class LiverCareTest {
  static final long START = 1577836800000L;
  static final long DAY = Utilities.convertTime("days", 1);
  static int checks;
  static void require(boolean condition, String message) {
    checks++;
    if (!condition) throw new AssertionError(message);
  }
  static Object get(Person p, String key) { return p.attributes.get("liver_" + key); }
  static boolean flag(Person p, String key) { return Boolean.TRUE.equals(get(p, key)); }
  static double num(Person p, String key) { return ((Number) get(p, key)).doubleValue(); }
  static boolean hasCode(HealthRecord.Entry e, String code) {
    return e.codes.stream().anyMatch(c -> c.code.equals(code));
  }
  static List<HealthRecord.Entry> entries(Person p, String code) {
    List<HealthRecord.Entry> result = new ArrayList<>();
    for (var encounter : p.record.encounters) {
      List<HealthRecord.Entry> all = new ArrayList<>();
      all.addAll(encounter.procedures); all.addAll(encounter.conditions);
      all.addAll(encounter.medications); all.addAll(encounter.careplans);
      all.addAll(encounter.devices);
      for (var entry : all) if (hasCode(entry, code)) result.add(entry);
    }
    return result;
  }
  static Person patient() {
    return patient(3003L);
  }
  static Person patient(long seed) {
    Person p = new Person(seed);
    p.attributes.put(Person.BIRTHDATE, START - Utilities.convertTime("years", 50));
    p.attributes.put(Person.GENDER, "F");
    p.attributes.put(Person.RACE, "white");
    p.attributes.put(Person.ETHNICITY, "nonhispanic");
    p.attributes.put(Person.INCOME, 50000);
    p.attributes.put(Person.OCCUPATION_LEVEL, 1.0);
    p.coverage.setPlanToNoInsurance((long) p.attributes.get(Person.BIRTHDATE));
    p.coverage.setPlanToNoInsurance(START + Utilities.convertTime("years", 2));
    Provider provider = Provider.getProviderList().get(0);
    for (EncounterType type : EncounterType.values()) p.setProvider(type, provider);
    return p;
  }
  public static void main(String[] args) throws Exception {
    Config.load(Path.of("synthea/synthea.properties").toFile());
    PayerManager.loadNoInsurance();
    Provider.loadProviders(new Location("District of Columbia", "Washington"), new DefaultRandomNumberGenerator(3003L));
    Path folder = Path.of(args[0]);
    Module.addModules(folder.toFile());
    JsonArray cases = JsonParser.parseString(Files.readString(folder.resolve("cases.json"))).getAsJsonArray();
    for (var item : cases) {
      JsonObject fixture = item.getAsJsonObject();
      String scenario = fixture.get("scenario").getAsString();
      String etiology = fixture.get("etiology").getAsString();
      Person p = patient();
      Module caller = new Module(JsonParser.parseString(Files.readString(Path.of(fixture.get("path").getAsString()))).getAsJsonObject(), false);
      long time = START;
      boolean done = caller.process(p, time);
      require(get(p, "error") == null, "valid admission: " + get(p, "error"));
      require(p.record.encounters.get(0).type.equals("inpatient"), "ICU uses inpatient class");
      require(hasCode(p.record.encounters.get(0), "305351004"), "ICU is explicitly coded");
      if (scenario.equals("no_support")) {
        require(entries(p, "40617009").isEmpty() && entries(p, "233585000").isEmpty()
            && entries(p, "373112").isEmpty(), "support is not universal");
        if (etiology.equals("alcohol_cirrhosis") || etiology.equals("cirrhosis")) require(get(p, "syndrome").equals("decompensated_cirrhosis"), "decompensation is not automatically ACLF");
      } else if (!scenario.equals("stochastic")) {
        require(!entries(p, "40617009").isEmpty() && !entries(p, "233585000").isEmpty()
            && !entries(p, "373112").isEmpty(), "indicated organ support starts");
      }
      int ticks = 0;
      boolean graftSeen = false;
      while (!done && ticks++ < 1300) {
        time += DAY / 4;
        done = caller.process(p, time);
        if (!graftSeen && get(p, "transplant_status").equals("recipient")) {
          require(get(p, "trajectory").equals("postoperative"), "preoperative outcome must not drive new graft trajectory");
          graftSeen = true;
        }
      }
      require(done, scenario + "/" + etiology + " must terminate within 325 days");
      require(get(p, "error") == null, "no runtime contract error: " + get(p, "error"));
      require(!flag(p, "active") && !flag(p, "care_active") && !p.hasCurrentEncounter(), "episode/encounter ownership released");
      var surgeries = entries(p, "18027006");
      boolean transplant = scenario.equals("transplant_survival") || scenario.equals("postoperative_death") || scenario.equals("rejection") || scenario.equals("wait_deterioration");
      boolean fatal = scenario.equals("death") || scenario.equals("wait_death") || scenario.equals("postoperative_death") || scenario.equals("prolonged_death");
      if (!scenario.equals("stochastic")) {
        require(surgeries.size() == (transplant ? 1 : 0), "exact expected surgery count: " + scenario);
        require(p.alive(time) != fatal, "expected survival: " + scenario);
      }
      require(surgeries.size() <= 1, "at most one graft in this episode");
      if (!surgeries.isEmpty()) {
        var surgery = surgeries.get(0);
        require(surgery.stop - surgery.start == Utilities.convertTime("hours", 6), "timed surgery");
        require(entries(p, "1076381000000104").get(0).start < surgery.start, "evaluation precedes surgery");
        var waiting = entries(p, "698305006").get(0);
        require(waiting.start < surgery.start && waiting.stop == surgery.start, "graft follows an actual wait");
        require(get(p, "transplant_status").equals("recipient") && num(p, "transplant_count") == 1, "persistent recipient status");
        require(entries(p, "737297006").size() == 1 && entries(p, "737297006").get(0).stop == 0, "persistent coded transplant history");
        if (p.alive(time)) {
          require(num(p, "followup_count") == 5 && flag(p, "followup_complete"), "five follow-ups completed");
          for (String med : new String[]{"198377", "199058"}) require(entries(p, med).get(0).stop == 0, "maintenance medication persists");
        }
        if (etiology.equals("alcohol_cirrhosis") || etiology.equals("cirrhosis")) require(entries(p, "19943007").get(0).stop == surgery.stop, "native cirrhosis reconciled at graft replacement");
      } else if (etiology.equals("alcohol_cirrhosis") || etiology.equals("cirrhosis")) {
        require(entries(p, "19943007").get(0).stop == 0, "medical recovery retains chronic cirrhosis");
      }
      if (scenario.equals("evaluation_recovery")) require(get(p, "tx_result").equals("recovery") && entries(p, "698305006").isEmpty(), "recovery during evaluation avoids listing");
      if (scenario.equals("waiting_recovery")) require(get(p, "tx_result").equals("recovery") && entries(p, "698305006").get(0).stop > 0, "recovery while waiting ends listing");
      if (scenario.equals("not_candidate")) require(get(p, "tx_result").equals("not_candidate") && entries(p, "698305006").isEmpty(), "non-candidacy avoids listing");
      if (scenario.equals("no_graft")) require(get(p, "tx_result").equals("recovery") && num(p, "waiting_days") == 20,
          "waiting continues beyond day 14 until an actual competing outcome");
      if (scenario.startsWith("prolonged_")) require(p.record.encounters.get(0).stop >= START + 45 * DAY,
          "day 30 never forces recovery or death");
      if (scenario.equals("rejection")) {
        require(entries(p, "431222008").size() == 1 && entries(p, "431222008").get(0).stop > 0, "one resolved rejection episode");
        require(entries(p, "312615").get(0).stop > 0, "temporary rejection medication ends");
      }
      long death = p.alive(time) ? Long.MAX_VALUE : (long) p.attributes.get(Person.DEATHDATE);
      for (var encounter : p.record.encounters) {
        require(encounter.stop >= encounter.start && encounter.stop <= death, "encounter boundaries do not exceed death");
        for (var observation : encounter.observations) require(observation.start >= encounter.start && observation.start <= encounter.stop, "labs belong within their encounter");
        for (var procedure : encounter.procedures) require(procedure.start >= encounter.start && procedure.stop <= encounter.stop && procedure.stop <= death, "procedure chronology");
        for (var plan : encounter.careplans) require(plan.stop >= plan.start && plan.stop <= death, "temporary support interval closed before death");
        for (var device : encounter.devices) require(device.start >= encounter.start && device.stop > device.start
            && device.stop <= encounter.stop && device.stop <= death, "device preserves exact support interval");
      }
      for (var med : entries(p, "373112")) require(med.stop > med.start && med.stop <= death, "vasopressor stops");
      if (p.alive(time)) {
        for (String code : new String[]{"197270009", "59927004", "716203000", "13920009", "14669001", "65710008", "27942005"}) {
          for (var condition : entries(p, code)) require(condition.stop > 0, "recovery ends owned acute disease");
        }
      }
      int count = p.record.encounters.size();
      caller.process(p, time + 30 * DAY);
      require(p.record.encounters.size() == count, "terminal journey produces no subsequent care");
      System.out.println("PASS: " + scenario + "/" + etiology);
    }
    // Conditions and treatments created by an earlier clinical pathway must not be ended
    // merely because the shared liver episode has recovered.
    JsonObject ownership = JsonParser.parseString(Files.readString(folder.resolve("recovery_alcohol_cirrhosis.json"))).getAsJsonObject();
    JsonObject states = ownership.getAsJsonObject("states");
    states.getAsJsonObject("Initial").addProperty("direct_transition", "External_encounter");
    JsonObject extra = JsonParser.parseString("""
      {"External_encounter":{"type":"Encounter","encounter_class":"ambulatory","codes":[{"system":"SNOMED-CT","code":"185347001","display":"Encounter for problem"}],"direct_transition":"External_renal"},
       "External_renal":{"type":"ConditionOnset","assign_to_attribute":"external_renal","codes":[{"system":"SNOMED-CT","code":"14669001","display":"Acute renal failure syndrome"}],"direct_transition":"External_cirrhosis"},
       "External_cirrhosis":{"type":"ConditionOnset","assign_to_attribute":"external_cirrhosis","codes":[{"system":"SNOMED-CT","code":"19943007","display":"Cirrhosis of liver"}],"direct_transition":"External_medication"},
       "External_medication":{"type":"MedicationOrder","assign_to_attribute":"external_pressor","codes":[{"system":"RxNorm","code":"373112","display":"norepinephrine Injectable Solution"}],"direct_transition":"External_end"},
       "External_end":{"type":"EncounterEnd","direct_transition":"Input_0"}}
      """).getAsJsonObject();
    extra.entrySet().forEach(e -> states.add(e.getKey(), e.getValue()));
    Person external = patient();
    Module owner = new Module(ownership, false);
    boolean ended = false;
    for (int tick = 0; tick < 1300 && !ended; tick++) ended = owner.process(external, START + tick * DAY / 4);
    require(ended && external.alive(START + 325 * DAY), "ownership fixture recovers");
    for (String key : new String[]{"external_renal", "external_cirrhosis", "external_pressor"}) {
      require(((HealthRecord.Entry) external.attributes.get(key)).stop == 0, "preserve external owner: " + key);
    }
    JsonObject retryDefinition = JsonParser.parseString("""
      {"name":"Late transplant retry","gmf_version":2,"states":{
      "Initial":{"type":"Initial","direct_transition":"Retry"},
      "Retry":{"type":"CallSubmodule","submodule":"fixture_recovery/transplant","direct_transition":"Terminal"},
      "Terminal":{"type":"Terminal"}}}
      """).getAsJsonObject();
    require(new Module(retryDefinition, false).process(external, START + 325 * DAY), "invalid late transplant call terminates");
    require(get(external, "error") != null && entries(external, "18027006").isEmpty(), "no surgery outside an active ICU course");
    external.attributes.remove(owner.name);
    external.attributes.put("liver_tx_result", "stale_previous_episode");
    external.attributes.put("liver_evaluation_context", "stale_previous_episode");
    Module repeat = new Module(JsonParser.parseString(Files.readString(folder.resolve("recovery_alcohol_cirrhosis.json"))).getAsJsonObject(), false);
    require(!repeat.process(external, START + 365 * DAY), "new episode begins with inpatient care");
    require(num(external, "episode") == 2 && get(external, "tx_result").equals("not_evaluated")
        && get(external, "evaluation_context") == null && get(external, "error") == null, "new episode clears old decision metadata");
    ended = false;
    for (int tick = 1; tick < 1300 && !ended; tick++) ended = repeat.process(external, START + 365 * DAY + tick * DAY / 4);
    require(ended && !flag(external, "care_active"), "repeat episode completes");
    // Each referral case gets one draw even when the caller checks the decision again.
    for (int index = 0; index < 100; index++) {
      Person referred = patient(90000L + index);
      referred.attributes.put("liver_case_id", "case-" + index);
      for (int call = 0; call < 3; call++) {
        JsonObject referral = JsonParser.parseString("""
          {"name":"Referral test","gmf_version":2,"states":{
          "Initial":{"type":"Initial","direct_transition":"Decide"},
          "Decide":{"type":"CallSubmodule","submodule":"fixture_stochastic/referral","direct_transition":"Terminal"},
          "Terminal":{"type":"Terminal"}}}
          """).getAsJsonObject();
        referral.addProperty("name", "Referral test " + call);
        Object before = get(referred, "referred_to_mguh");
        long drawsBefore = referred.getCount();
        if (call == 2) referred.attributes.put("liver_case_id", "next-" + index);
        require(new Module(referral, false).process(referred, START), "referral decision completes immediately");
        require(get(referred, "error") == null, "valid referral case");
        require(get(referred, "referral_decision_case_id").equals(get(referred, "case_id")), "decision belongs to the current case");
        if (call == 1) {
          require(get(referred, "referred_to_mguh").equals(before), "same case retains its referral decision");
          require(referred.getCount() == drawsBefore, "same case consumes no second referral draw");
        }
        if (call == 2) require(referred.getCount() > drawsBefore, "new case gets its own referral draw");
        require(referred.record.encounters.isEmpty(), "referral alone does not imply ICU admission");
      }
    }
    System.out.println("PASS: " + cases.size() + " ICU/transplant journeys; " + checks + " engine assertions");
  }
}
