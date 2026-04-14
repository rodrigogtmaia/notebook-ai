from pathlib import Path
import unittest


class CocoLendingReviewedNotebookTest(unittest.TestCase):
    NOTEBOOK_PATH = (
        Path(__file__).resolve().parent
        / "outputs"
        / "coco_lending_suboptimality_reviewed.scala"
    )

    def read_notebook(self) -> str:
        self.assertTrue(
            self.NOTEBOOK_PATH.exists(),
            f"Reviewed notebook not found: {self.NOTEBOOK_PATH}",
        )
        return self.NOTEBOOK_PATH.read_text(encoding="utf-8")

    def test_notebook_follows_canonical_section_order(self) -> None:
        contents = self.read_notebook()
        section_markers = [
            "## 0. Intro / README",
            "## 1. Imports",
            "## 2. Data sources",
            "## 3. Widgets",
            "## 4. Variable filters",
            "## 5. Functions",
            "## 6. Code / analysis",
            "## 7. Validation",
            "## 8. Display",
        ]

        positions = [contents.index(marker) for marker in section_markers]
        self.assertEqual(positions, sorted(positions))

    def test_notebook_removes_python_cells_and_fixes_public_payroll_split(self) -> None:
        contents = self.read_notebook()

        self.assertNotIn("%python", contents)
        self.assertNotIn("JOIN inss_elig", contents)
        self.assertIn("'public_payroll' AS product_type", contents)
        self.assertIn("publicPayrollProviderEligibilityColumns", contents)
        self.assertIn('startsWith("is_eligible__public_payroll_provider")', contents)
        self.assertIn('filterNot(_ == "is_eligible__public_payroll_provider_dataprev")', contents)
        self.assertIn("WHERE m.is_eligible__public_payroll = true", contents)
        self.assertNotIn("COALESCE(m.is_eligible__inss, false) = false", contents)

    def test_notebook_includes_fail_fast_source_guards(self) -> None:
        contents = self.read_notebook()

        required_markers = [
            "DBTITLE 1,Create early schema guard helper",
            "DBTITLE 1,Validate source schemas before transformations",
            'assertNoDuplicateKeys(monitoringBase, "monitoringBase", Seq("customer__id"))',
            'assertNoDuplicateKeys(fgtsEligibilityRates, "fgtsEligibilityRates", Seq("customer__id"))',
            'assertNoDuplicateKeys(publicPayrollEligibilityRates, "publicPayrollEligibilityRates", Seq("customer__id"))',
            'assertNoDuplicateKeys(privatePayrollEligibilityRates, "privatePayrollEligibilityRates", Seq("customer__id"))',
            "publicPayrollProviderEligibilityColumns.nonEmpty",
        ]

        for marker in required_markers:
            self.assertIn(marker, contents)

        self.assertLess(
            contents.index("DBTITLE 1,Validate source schemas before transformations"),
            contents.index("DBTITLE 1,Build recent lending contracts"),
        )

    def test_notebook_includes_validation_and_target_segment_guards(self) -> None:
        contents = self.read_notebook()

        required_markers = [
            "Check configured products against contract and eligibility coverage",
            "Check target segment uniqueness at customer grain",
            'assertNoDuplicateKeys(spark.table("current_eligibility"), "current_eligibility", Seq("customer__id", "product_type"))',
            'assertNoDuplicateKeys(spark.table("target_segment"), "target_segment", Seq("customer__id"))',
            "ROW_NUMBER() OVER (",
            "WHERE saving_rank = 1",
        ]

        for marker in required_markers:
            self.assertIn(marker, contents)


if __name__ == "__main__":
    unittest.main()
