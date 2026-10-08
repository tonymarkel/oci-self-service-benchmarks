import hashlib
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import deathstarbench_artifact_common as common


class ArtifactSourceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.source = self.root / "upstream"
        self.source.mkdir()
        (self.source / "workload").mkdir()
        (self.source / "workload/source.txt").write_text("pinned source\n")
        (self.source / "LICENSE").write_text("license fixture\n")
        (self.source / ".gitignore").write_text("ignored\n")
        self.git("init", "-q")
        self.git("add", ".")
        self.git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture")
        self.revision = self.git("rev-parse", "HEAD").strip()
        self.revision_patch = mock.patch.object(common, "UPSTREAM_REVISION", self.revision)
        self.revision_patch.start()
        self.addCleanup(self.revision_patch.stop)
        self.addCleanup(self.temporary.cleanup)

    def git(self, *arguments):
        return subprocess.check_output(["git", "-C", str(self.source), *arguments], text=True)

    def validate(self, **kwargs):
        return common.validate_tracked_source(
            self.source, relative_roots=("workload", "LICENSE"),
            anchors={"LICENSE": common.sha256(self.source / "LICENSE")}, **kwargs,
        )

    def test_exact_checkout_and_single_file_copy(self):
        self.assertEqual(self.validate(), self.source.resolve())
        common.copy_tracked_tree(self.source, "LICENSE", self.root / "new/LICENSE")
        self.assertEqual((self.root / "new/LICENSE").read_bytes(), (self.source / "LICENSE").read_bytes())

    def test_wrong_revision_fails(self):
        with mock.patch.object(common, "UPSTREAM_REVISION", "0" * 40):
            with self.assertRaisesRegex(common.PreparationError, "revision"):
                self.validate()

    def test_dirty_and_untracked_files_fail(self):
        (self.source / "workload/source.txt").write_text("changed")
        with self.assertRaisesRegex(common.PreparationError, "dirty"):
            self.validate()
        (self.source / "workload/source.txt").write_text("pinned source\n")
        (self.source / "workload/other").write_text("extra")
        with self.assertRaisesRegex(common.PreparationError, "dirty"):
            self.validate()

    def test_ignored_file_and_empty_directory_fail(self):
        (self.source / "workload/ignored").write_text("hidden compiler output")
        with self.assertRaisesRegex(common.PreparationError, "inventory"):
            self.validate()
        (self.source / "workload/ignored").unlink()
        (self.source / "workload/empty").mkdir()
        with self.assertRaisesRegex(common.PreparationError, "inventory"):
            self.validate()

    def test_assume_unchanged_cannot_hide_modified_bytes(self):
        self.git("update-index", "--assume-unchanged", "workload/source.txt")
        (self.source / "workload/source.txt").write_text("hidden drift")
        with self.assertRaisesRegex(common.PreparationError, "bytes drifted"):
            self.validate()

    def test_executable_bit_verified_even_when_git_ignores_filemode(self):
        self.git("config", "core.filemode", "false")
        (self.source / "workload/source.txt").chmod(0o755)
        with self.assertRaisesRegex(common.PreparationError, "mode drifted"):
            self.validate()

    def test_anchor_must_be_tracked_and_in_selected_inventory(self):
        with self.assertRaisesRegex(common.PreparationError, "anchor"):
            common.validate_tracked_source(
                self.source, relative_roots=("workload",),
                anchors={"LICENSE": common.sha256(self.source / "LICENSE")},
            )

    def test_git_symlink_target_verified_and_safe_copy_preserves_link(self):
        (self.source / "workload/link").symlink_to("source.txt")
        self.git("add", "workload/link")
        self.git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "link")
        with mock.patch.object(common, "UPSTREAM_REVISION", self.git("rev-parse", "HEAD").strip()):
            self.validate()
            common.copy_tracked_tree(self.source, "workload", self.root / "copy")
            self.assertEqual(os.readlink(self.root / "copy/link"), "source.txt")
        with self.assertRaisesRegex(common.PreparationError, "excluded"):
            common.copy_tracked_tree(self.source, "workload", self.root / "excluded", exclude=("source.txt",))

    def test_escaping_symlink_fails(self):
        (self.source / "workload/link").symlink_to("../LICENSE")
        self.git("add", "workload/link")
        self.git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "escape")
        with mock.patch.object(common, "UPSTREAM_REVISION", self.git("rev-parse", "HEAD").strip()):
            with self.assertRaisesRegex(common.PreparationError, "Escaping"):
                self.validate()

    def test_no_unsafe_relative_paths(self):
        for value in ("", ".", "../source", "/source", "workload/../LICENSE", "workload//source.txt", ".git/config", "workload\\source", "bad\0path"):
            with self.subTest(value=value), self.assertRaises(common.PreparationError):
                common.copy_tracked_tree(self.source, value, self.root / "unsafe")


class ArtifactOutputTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        self.source.mkdir()
        self.protected = self.root / "input.rock"
        self.protected.write_text("protected")

    def test_success_keeps_output_and_failure_removes_only_fresh_output(self):
        with common.candidate_output(self.source, self.root / "success") as output:
            (output / "manifest").write_text("candidate")
        self.assertTrue((self.root / "success/manifest").exists())
        with self.assertRaisesRegex(ValueError, "intentional"):
            with common.candidate_output(self.source, self.root / "failure") as output:
                (output / "partial").write_text("candidate")
                raise ValueError("intentional")
        self.assertFalse((self.root / "failure").exists())
        self.assertEqual(self.protected.read_text(), "protected")

    def test_existing_paths_and_broken_symlinks_are_never_removed(self):
        targets = (self.source, self.protected, self.root / "broken")
        targets[-1].symlink_to("does-not-exist")
        for target in targets:
            with self.subTest(target=target), self.assertRaises(common.PreparationError):
                with common.candidate_output(self.source, target):
                    self.fail("existing target was accepted")
            self.assertTrue(os.path.lexists(target))

    def test_inside_or_ancestor_inputs_are_rejected(self):
        for target in (self.source / "inside", self.protected / "inside"):
            with self.subTest(target=target), self.assertRaisesRegex(common.PreparationError, "overlaps"):
                with common.candidate_output(self.source, target, protected_paths=(self.protected,)):
                    self.fail("overlap accepted")
        with self.assertRaises(common.PreparationError):
            with common.candidate_output(self.source, self.root):
                self.fail("ancestor accepted")

    def test_symlinked_parent_cannot_redirect_output_into_checkout(self):
        (self.root / "alias").symlink_to(self.source, target_is_directory=True)
        with self.assertRaisesRegex(common.PreparationError, "overlaps"):
            with common.candidate_output(self.source, self.root / "alias/output"):
                self.fail("redirect accepted")

    def test_cleanup_preserves_replaced_directory_or_symlink(self):
        for kind in ("directory", "symlink"):
            target = self.root / kind
            moved = self.root / (kind + "-moved")
            with self.subTest(kind=kind), self.assertRaisesRegex(common.PreparationError, "replacement preserved"):
                with common.candidate_output(self.source, target):
                    target.rename(moved)
                    if kind == "directory":
                        target.mkdir()
                        (target / "valuable").write_text("unrelated data")
                    else:
                        target.symlink_to(self.source)
                    raise ValueError("intentional preparation failure")
            self.assertTrue(moved.is_dir())
            if kind == "directory":
                self.assertEqual((target / "valuable").read_text(), "unrelated data")
            else:
                self.assertTrue(target.is_symlink())

    def test_digest_covers_names_modes_links_and_bytes_not_timestamps(self):
        tree = self.root / "tree"
        tree.mkdir()
        file = tree / "data"
        file.write_text("data")
        initial = common.tree_sha256(tree)
        os.utime(file, (1, 1))
        self.assertEqual(initial, common.tree_sha256(tree))
        file.chmod(0o755)
        self.assertNotEqual(initial, common.tree_sha256(tree))
        file.chmod(0o644)
        file.rename(tree / "renamed")
        self.assertNotEqual(initial, common.tree_sha256(tree))
        file = tree / "renamed"
        original = common.tree_sha256(tree)
        file.write_text("different")
        self.assertNotEqual(original, common.tree_sha256(tree))
        (tree / "link").symlink_to("renamed")
        self.assertNotEqual(original, common.tree_sha256(tree))

    def test_exact_patch_counts_fail_closed(self):
        self.assertEqual(common.replace_exact("x x", "x", "y", label="fixture", count=2), "y y")
        for text in ("", "x x"):
            with self.assertRaises(common.PreparationError):
                common.replace_exact(text, "x", "y", label="fixture")


if __name__ == "__main__":
    unittest.main()
