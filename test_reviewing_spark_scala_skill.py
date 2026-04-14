from pathlib import Path
import unittest


class ReviewingSparkScalaSkillTest(unittest.TestCase):
    SKILL_PATH = (
        Path(__file__).resolve().parent
        / ".cursor"
        / "skills"
        / "reviewing-spark-scala-notebooks"
        / "SKILL.md"
    )

    def read_skill(self) -> str:
        self.assertTrue(self.SKILL_PATH.exists(), f"Skill file not found: {self.SKILL_PATH}")
        return self.SKILL_PATH.read_text(encoding="utf-8")

    def test_skill_requires_translating_non_scala_code_to_scala(self) -> None:
        contents = self.read_skill()

        required_markers = [
            "If the notebook contains executable code outside Scala, translate it to Scala in the final notebook.",
            "The final notebook must not keep mixed-language executable cells when Scala can express the same logic.",
            "no executable code outside Scala remains in the final notebook unless the user explicitly approves an exception",
        ]

        for marker in required_markers:
            self.assertIn(marker, contents)


if __name__ == "__main__":
    unittest.main()
