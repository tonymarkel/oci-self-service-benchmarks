import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import prepare_deathstarbench_hotel_images as hotel


def write(path, content="fixture\n"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


ORIGINAL_METHOD = "// CheckAvailability checks if given information is available\nfunc oldAvailability() {}\n"
USER_FIXTURE = '''package main
func initializeDatabase(url string) (*mongo.Client, func()) {
    name := fmt.Sprintf("Cornell_%x", suffix)
}
'''
FRONTEND_FIXTURE = '''package frontend
func (s *Server) initReviewClient(name string) error {
    conn, err := dialer.Dial(
\t\tname,
\t\tdialer.WithTracer(s.Tracer),
\t\tdialer.WithBalancer(s.Registry.Client),
\t)
}
func (s *Server) initAttractionsClient(name string) error {
    conn, err := dialer.Dial(
\t\tname,
\t\tdialer.WithTracer(s.Tracer),
\t\tdialer.WithBalancer(s.Registry.Client),
\t)
}
func (s *Server) getGprcConn(name string) (*grpc.ClientConn, error) {
    fmt.Sprintf("consul://%s/%s.%s", s.ConsulAddr, name, s.KnativeDns)
    fmt.Sprintf("consul://%s/%s", s.ConsulAddr, name)
}
'''
RESERVATION_FIXTURE = (
    'package reservation\nfunc oldWrite() {\n'
    '\tmemc_key := hotelId + "_" + inDate.String()[0:10] + "_" + outdate\n}\n'
    + ORIGINAL_METHOD + '\ntype reservation struct {}\n'
)


def source_fixture(root):
    for name in hotel.APP_DIRECTORIES:
        write(root / "hotelReservation" / name / "fixture.txt")
    for name in hotel.APP_FILES:
        write(root / "hotelReservation" / name)
    write(root / "hotelReservation/cmd/user/db.go", USER_FIXTURE)
    write(root / "hotelReservation/services/frontend/server.go", FRONTEND_FIXTURE)
    write(root / "hotelReservation/services/reservation/server.go", RESERVATION_FIXTURE)
    write(root / "hotelReservation/vendor/example.invalid/dependency/LICENSE", "dependency license\n")
    write(root / "hotelReservation/vendor/modules.txt", "# pinned dependency fixture\n")
    write(root / "hotelReservation/x509/server_key.pem", "DEMO KEY MUST NOT ENTER CONTEXT\n")
    write(root / "hotelReservation/wrk2/unused", "excluded load driver\n")
    write(root / "LICENSE", "Apache upstream license\n")


class HotelContextTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="hotel-image-fixtures-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.source = self.root / "source"
        source_fixture(self.source)
        self.validation = mock.patch.object(hotel, "validate_tracked_source", return_value=self.source)
        self.validate = self.validation.start()
        self.addCleanup(self.validation.stop)
        inventory = [{"path": p.relative_to(self.source).as_posix(), "sha256": hotel.sha256(p)}
                     for p in sorted(self.source.rglob("*")) if p.is_file()]
        self.inventory = mock.patch.object(hotel, "_inventory", return_value=inventory)
        self.inventory.start()
        self.addCleanup(self.inventory.stop)
        self.method_hash = mock.patch.object(hotel, "ORIGINAL_AVAILABILITY_SHA256",
            hashlib.sha256(ORIGINAL_METHOD.encode()).hexdigest())
        self.method_hash.start()
        self.addCleanup(self.method_hash.stop)

    def prepare(self, name="out"):
        output = self.root / name
        return output, hotel.prepare_contexts(self.source, output)

    def test_candidate_only_manifest_binds_sources_patches_software_and_licenses(self):
        output, manifest = self.prepare()
        self.assertTrue(manifest["candidate_only"])
        self.assertFalse(manifest["released"])
        self.assertFalse(manifest["byte_reproducible_rebuild"])
        self.assertEqual(manifest["upstream_revision"], hotel.UPSTREAM_REVISION)
        self.assertEqual(manifest["patch_revision"], hotel.PATCH_REVISION)
        self.assertEqual(manifest["dataset_revision"], hotel.DATASET_REVISION)
        self.assertEqual(manifest["source_inventory_sha256"], hotel._json_digest(manifest["source_inventory"]))
        self.assertEqual(set(manifest["contexts"]), {"app"})
        self.assertEqual(manifest["contexts"]["app"]["context_sha256"], hotel.tree_sha256(output / "app"))
        for relative, values in manifest["source_patches"].items():
            self.assertEqual(values["original_sha256"], hotel.sha256(self.source / relative))
            self.assertEqual(values["patched_sha256"], hotel.sha256(output / "app" / Path(relative).relative_to("hotelReservation")))
            self.assertNotEqual(values["original_sha256"], values["patched_sha256"])
        self.assertEqual(manifest["software"]["builder"], hotel.GO_BUILDER)
        self.assertEqual(manifest["license_identities"]["deathstarbench_license_sha256"], hotel.sha256(self.source / "LICENSE"))
        self.assertEqual(json.loads((output / "context-manifest.json").read_text()), manifest)
        self.validate.assert_called_once_with(self.source,
            relative_roots=("hotelReservation", "LICENSE"), anchors=hotel.UPSTREAM_ANCHORS)

    def test_same_inputs_produce_identical_contexts_and_manifests(self):
        _, first = self.prepare("first")
        previous_umask = os.umask(0o077)
        try:
            _, second = self.prepare("second")
        finally:
            os.umask(previous_umask)
        self.assertEqual(first, second)

    def test_app_contains_audited_config_license_and_tests_not_demo_keys_or_driver(self):
        output, manifest = self.prepare()
        app = output / "app"
        self.assertEqual((app / "config.json").read_bytes(), (self.source / "hotelReservation/config.json").read_bytes())
        self.assertEqual((app / "LICENSE.deathstarbench").read_bytes(), (self.source / "LICENSE").read_bytes())
        self.assertTrue((app / "licenses/vendor/example.invalid/dependency/LICENSE").is_file())
        self.assertFalse((app / "x509").exists())
        self.assertFalse((app / "wrk2").exists())
        self.assertEqual(len(manifest["semantic_test_fixtures"]), 3)
        for path, digest in manifest["semantic_test_fixtures"].items():
            self.assertEqual(hotel.sha256(app / path), digest)

    def test_native_immutable_build_recipe_needs_no_mutable_downloads(self):
        recipe = hotel._dockerfile()
        self.assertIn("FROM --platform=$TARGETPLATFORM " + hotel.GO_BUILDER, recipe)
        self.assertIn('test "$BUILDPLATFORM" = "$TARGETPLATFORM"', recipe)
        self.assertIn("linux/amd64|linux/arm64", recipe)
        self.assertIn("RUN --network=none", recipe)
        self.assertIn("GOPROXY=off GOSUMDB=off", recipe)
        self.assertIn("go1.21.13", recipe)
        self.assertIn("FROM scratch", recipe)
        self.assertIn("go test -mod=vendor -vet=off", recipe)
        self.assertIn("USER 65532:65532", recipe)
        self.assertIn("COPY config.json /workspace/config.json", recipe)
        self.assertNotIn("git clone", recipe)
        self.assertNotIn("apt-get", recipe)
        self.assertNotIn("go get", recipe)

    def test_username_patch_preserves_password_hash_and_fails_on_reapplication(self):
        original = USER_FIXTURE + '\npassword := fmt.Sprintf("%x", sum)\n'
        corrected = hotel._patched_user(original)
        self.assertIn('fmt.Sprintf("Cornell_%s", suffix)', corrected)
        self.assertIn('password := fmt.Sprintf("%x", sum)', corrected)
        self.assertIn("seededUsername(suffix)", corrected)
        with self.assertRaises(hotel.PreparationError):
            hotel._patched_user(corrected)

    def test_frontend_calls_shared_consul_path_for_both_extra_services(self):
        corrected = hotel._patched_frontend(FRONTEND_FIXTURE)
        self.assertEqual(corrected.count("conn, err := s.getGprcConn(name)"), 2)
        self.assertIn('fmt.Sprintf("consul://%s/%s", s.ConsulAddr, name)', corrected)
        with self.assertRaises(hotel.PreparationError):
            hotel._patched_frontend(FRONTEND_FIXTURE.replace("dialer.WithBalancer(s.Registry.Client)", "unknown()", 1))

    def test_reservation_patch_checks_exact_old_method_and_shared_nightly_write_key(self):
        corrected = hotel._patched_reservation(RESERVATION_FIXTURE)
        self.assertIn("memc_key := reservationCacheKey(hotelId, indate, outdate)", corrected)
        self.assertIn("capacityMissFilter(ids)", corrected)
        self.assertIn("fetched, err := store.Capacities(ctx, misses)", corrected)
        self.assertNotIn("err == memcache.ErrCacheMiss", corrected)
        with self.assertRaises(hotel.PreparationError):
            hotel._patched_reservation(RESERVATION_FIXTURE.replace("func oldAvailability() {}", "func drifted() {}"))

    def test_patch_failure_removes_only_its_fresh_output(self):
        output = self.root / "out"
        write(self.root / "unrelated/keep", "must survive\n")
        with mock.patch.object(hotel, "_patched_reservation", side_effect=hotel.PreparationError("drift")):
            with self.assertRaises(hotel.PreparationError):
                hotel.prepare_contexts(self.source, output)
        self.assertFalse(output.exists())
        self.assertEqual((self.root / "unrelated/keep").read_text(), "must survive\n")

    def test_existing_output_and_input_overlap_remain_untouched(self):
        existing = self.root / "existing"
        write(existing / "keep", "must survive\n")
        for target in (existing, self.source / "candidate", self.source.parent):
            with self.subTest(target=target):
                with self.assertRaises(hotel.PreparationError):
                    hotel.prepare_contexts(self.source, target)
        self.assertEqual((existing / "keep").read_text(), "must survive\n")

    def test_unqualified_mutation_seeding_and_backing_images_stay_explicit_blockers(self):
        _, manifest = self.prepare()
        blockers = " ".join(manifest["release_blockers"])
        self.assertIn("InsertMany", blockers)
        self.assertIn("not an atomic capacity transaction", blockers)
        self.assertIn("Warmup", blockers)
        self.assertIn("MongoDB 5.0", blockers)
        self.assertNotIn("published_image_digest", manifest)


@unittest.skipUnless(shutil.which("go"), "Go is unavailable; native Go fixture CI remains required")
class HotelGoSemanticsTests(unittest.TestCase):
    def test_offline_corrected_core_cold_partial_warm_capacity_and_seed_semantics(self):
        """Execute the actual core against small type adapters, no vendored fetch.

        Candidate artifact CI also tests these same fixtures against the full
        pinned vendored Go packages and native immutable Go builder.
        """
        production = hotel._asset("availability.go").read_text()
        first = production.index("type mongoAvailabilityStore struct")
        last = production.index("type availabilityNight struct")
        production = production[:first] + production[last:]
        production = re.sub(r"func \(s \*Server\) CheckAvailability\([^\n]+\n.*?^}\n", "", production, flags=re.M | re.S)
        production = production.replace("memcache.Item", "fixtureItem").replace("pb.Request", "fixtureRequest").replace("pb.Result", "fixtureResult").replace("opentracing.StartSpanFromContext", "startFixtureSpan")
        adapters = '''package hotelcandidate
import ("context"; "fmt"; "strconv"; "strings"; "sync"; "time")
type fixtureItem struct { Key string; Value []byte }
type fixtureRequest struct { HotelId []string; InDate, OutDate string; RoomNumber int32 }
type fixtureResult struct { HotelId []string }
type fixtureSpan struct{}
func (fixtureSpan) SetTag(string, interface{}) {}
func (fixtureSpan) Finish() {}
func startFixtureSpan(ctx context.Context, name string) (fixtureSpan, context.Context) { return fixtureSpan{}, ctx }
'''
        test = hotel._asset("availability_test.go").read_text()
        test = test.replace("package reservation", "package hotelcandidate")
        test = re.sub(r'^\s*(?:pb )?"(?:github.com/bradfitz/gomemcache/memcache|github.com/delimitrou/DeathStarBench/tree/master/hotelReservation/services/reservation/proto|go.mongodb.org/mongo-driver/bson)"\n', "", test, flags=re.M)
        test = re.sub(r"func TestCandidateCapacityQueryWrapsHotelID\(.*?^}\n", "", test, flags=re.M | re.S)
        test = test.replace("memcache.Item", "fixtureItem").replace("pb.Request", "fixtureRequest")
        user = hotel._patched_user(USER_FIXTURE)
        user_helper = re.search(r"func seededUsername\([^\n]+\n.*?^}", user, flags=re.M | re.S).group()
        frontend = hotel._patched_frontend(FRONTEND_FIXTURE)
        frontend_helper = re.search(r"func \(s \*Server\) consulDialTarget\([^\n]+\n.*?^}", frontend, flags=re.M | re.S).group()
        helpers = 'package hotelcandidate\nimport "fmt"\ntype Server struct { ConsulAddr, KnativeDns string }\n' + user_helper + "\n" + frontend_helper + "\n"
        with tempfile.TemporaryDirectory(prefix="hotel-go-semantics-") as directory:
            root = Path(directory)
            write(root / "go.mod", "module hotelcandidate\n\ngo 1.18\n")
            write(root / "availability.go", adapters + production)
            write(root / "availability_test.go", test)
            write(root / "helpers.go", helpers)
            for name, package in (("user_seed_test.go", "main"), ("frontend_resolver_test.go", "frontend")):
                write(root / name, hotel._asset(name).read_text().replace("package " + package, "package hotelcandidate"))
            environment = {**os.environ, "CGO_ENABLED": "0", "GOTOOLCHAIN": "local", "GOPROXY": "off", "GOSUMDB": "off", "GOCACHE": str(root / "cache")}
            result = subprocess.run([shutil.which("go"), "test", "-timeout=30s", "-count=1", "./..."], cwd=root, env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=120)
            self.assertEqual(result.returncode, 0, result.stdout)


if __name__ == "__main__":
    unittest.main()
