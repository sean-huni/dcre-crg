package za.co.fnb.dcre.crg.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SCRUM-107 red-proof for the {@code dcre.prg} -> {@code dcre.crg} config-prefix rename.
 *
 * <p>R-48 (MAS) shipped once already: a prefix renamed in code with the yml key left behind binds
 * nothing, every reader silently falls to its {@code @Value} constant default, and the build stays
 * at exit 0 because no assertion anywhere looks at the committed yml. That defect is invisible to
 * the existing suites here for a second reason: {@code PsrReportServiceSliceTest} and
 * {@code ImmediateReportIT} both SET {@code dcre.crg.psr-slice-size} as a test property, so they
 * pass whatever the committed file says, and the committed value (50000) equals the
 * {@code @Value} fallback, so no injected number can tell bound from fallen-back either.
 *
 * <p>So this test asserts the two sides against each other instead of asserting a value: every
 * {@code ${dcre.*}} placeholder written in main sources must resolve against the committed
 * application.yml. It fails in BOTH directions (yml renamed but code not, code renamed but yml
 * not) and it generalises to any future prefix, which a single hardcoded key assertion would not.
 */
class ConfigPlaceholderBindingTest {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{(dcre\\.[A-Za-z0-9._-]+)");
    private static final Path MAIN_SOURCES = Path.of("src/main/java");

    @Test
    void everyDcrePlaceholderInMainSourcesResolvesAgainstTheCommittedYaml() throws IOException {
        final Set<String> referenced = referencedKeys();
        final PropertySource<?> yaml = committedYaml();

        // Control (verification.md 11a): an empty scan would pass vacuously, and the whole point
        // of this test is that an absence must be distinguishable from "the search did not run".
        assertThat(referenced)
                .as("the placeholder scan found nothing, so it proves nothing")
                .isNotEmpty()
                .contains("dcre.crg.psr-slice-size");

        assertThat(referenced)
                .allSatisfy(key -> assertThat(yaml.containsProperty(key))
                        .as("application.yml has no key for placeholder ${%s}: the code prefix and"
                                + " the yml key have drifted apart (R-48)", key)
                        .isTrue());
    }

    @Test
    void theRetiredPrgPrefixIsGoneFromBothSides() throws IOException {
        final PropertySource<?> yaml = committedYaml();

        assertThat(yaml.containsProperty("dcre.prg.psr-slice-size"))
                .as("application.yml still carries the retired dcre.prg prefix")
                .isFalse();
        assertThat(referencedKeys())
                .as("main sources still reference the retired dcre.prg prefix")
                .noneMatch(key -> key.startsWith("dcre.prg."));
    }

    private Set<String> referencedKeys() throws IOException {
        final Set<String> keys = new TreeSet<>();
        try (Stream<Path> files = Files.walk(MAIN_SOURCES)) {
            for (Path file : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                final Matcher m = PLACEHOLDER.matcher(Files.readString(file, StandardCharsets.UTF_8));
                while (m.find()) {
                    keys.add(m.group(1));
                }
            }
        }
        return keys;
    }

    private PropertySource<?> committedYaml() throws IOException {
        final List<PropertySource<?>> loaded = new YamlPropertySourceLoader()
                .load("application.yml", new ClassPathResource("application.yml"));
        assertThat(loaded).as("application.yml did not load").isNotEmpty();
        return loaded.getFirst();
    }
}
