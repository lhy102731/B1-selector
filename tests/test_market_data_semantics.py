from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from research_automation.data_generation.market_data import (
    BarAvailability,
    BarObservation,
    BarUse,
    FetchFailure,
    MarketDataUseError,
    MarketSessionKey,
    NavValuation,
    NoBarConfirmation,
    PortfolioValuation,
    ValuationState,
    classify_bar_availability,
    value_portfolio_position,
)
from tools.audit_market_data_semantics import scan_data_dir
from utils.market_data_semantics import audit_frame, summarize_checks


class MarketDataSemanticsTests(unittest.TestCase):
    def test_consistent_adjusted_rows_pass(self):
        frame = pd.DataFrame(
            {
                "date": ["2026-06-23", "2026-06-24", "2026-06-25"],
                "open": [10.0, 10.5, 11.5],
                "high": [10.5, 11.5, 12.5],
                "low": [9.5, 10.5, 11.5],
                "close": [10.0, 11.0, 12.0],
                "change_pct": [None, 10.0, 9.0909090909],
                "amplitude": [None, 10.0, 9.0909090909],
            }
        )

        checks = audit_frame(frame, code="000001")

        comparable = [row for row in checks if row["return_eligible"]]
        self.assertEqual(2, len(comparable))
        self.assertFalse(any(row["return_bad"] for row in comparable))
        self.assertFalse(any(row["amplitude_bad"] for row in comparable))

    def test_mixed_price_scale_is_reported(self):
        frame = pd.DataFrame(
            {
                "date": ["2026-06-23", "2026-06-24"],
                "open": [100.0, 94.0],
                "high": [101.0, 97.0],
                "low": [99.0, 92.0],
                "close": [100.0, 95.0],
                "change_pct": [None, 2.0],
                "amplitude": [None, 2.0],
            }
        )

        checks = audit_frame(frame, code="300274")
        row = checks[-1]

        self.assertTrue(row["return_bad"])
        self.assertAlmostEqual(-5.0, row["calculated_change_pct"], places=8)
        self.assertAlmostEqual(7.0, row["return_error_pp"], places=8)
        self.assertTrue(row["amplitude_bad"])
        self.assertAlmostEqual(5.0, row["calculated_amplitude_pct"], places=8)

    def test_cross_sectional_spike_quarantines_dataset(self):
        frames = []
        for code in ("000001", "000002"):
            frame = pd.DataFrame(
                {
                    "date": ["2026-06-23", "2026-06-24"],
                    "high": [101.0, 97.0],
                    "low": [99.0, 92.0],
                    "close": [100.0, 95.0],
                    "change_pct": [None, 2.0],
                    "amplitude": [None, 2.0],
                }
            )
            frames.extend(audit_frame(frame, code=code))

        summary = summarize_checks(
            frames,
            cross_sectional_spike_ratio=0.5,
            min_eligible=2,
        )

        self.assertEqual("SEMANTIC_QUARANTINE", summary["status"])
        day = next(item for item in summary["dates"] if item["date"] == "2026-06-24")
        self.assertEqual(2, day["return_bad"])
        self.assertEqual(1.0, day["return_bad_ratio"])
        self.assertIn("RETURN_CROSS_SECTIONAL_SPIKE", day["flags"])

    def test_missing_optional_fields_are_not_treated_as_valid(self):
        frame = pd.DataFrame(
            {
                "date": ["2026-06-23", "2026-06-24"],
                "high": [10.0, 11.0],
                "low": [9.0, 10.0],
                "close": [9.5, 10.5],
            }
        )

        checks = audit_frame(frame, code="000001")

        self.assertFalse(checks[-1]["return_eligible"])
        self.assertFalse(checks[-1]["amplitude_eligible"])

    def test_data_dir_scan_reports_market_wide_incident_without_changing_sources(self):
        with TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            stock_dir = data_dir / "00"
            stock_dir.mkdir(parents=True)
            source_hashes = {}
            for code in ("000001", "000002"):
                path = stock_dir / f"{code}.csv"
                pd.DataFrame(
                    {
                        "date": ["2026-06-24", "2026-06-23"],
                        "high": [97.0, 101.0],
                        "low": [92.0, 99.0],
                        "close": [95.0, 100.0],
                        "change_pct": [2.0, None],
                        "amplitude": [2.0, None],
                    }
                ).to_csv(path, index=False, encoding="gbk")
                source_hashes[path] = path.read_bytes()

            summary, bad_rows = scan_data_dir(
                data_dir,
                recent_rows=10,
                cross_sectional_spike_ratio=0.5,
                min_eligible=2,
            )

            self.assertEqual("SEMANTIC_QUARANTINE", summary["status"])
            self.assertEqual(2, summary["files_scanned"])
            self.assertEqual(0, summary["files_failed"])
            self.assertEqual(4, len(bad_rows))
            for path, original in source_hashes.items():
                self.assertEqual(original, path.read_bytes())

    def test_overlay_scan_projects_replacements_without_changing_sources(self):
        with TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            overlay_dir = Path(directory) / "overlay"
            source_dir = data_dir / "00"
            replacement_dir = overlay_dir / "00"
            source_dir.mkdir(parents=True)
            replacement_dir.mkdir(parents=True)
            source_bytes = {}
            for code in ("000001", "000002"):
                source = source_dir / f"{code}.csv"
                bad = pd.DataFrame(
                    {
                        "date": ["2026-06-24", "2026-06-23"],
                        "high": [97.0, 101.0],
                        "low": [92.0, 99.0],
                        "close": [95.0, 100.0],
                        "change_pct": [2.0, None],
                        "amplitude": [2.0, None],
                    }
                )
                bad.to_csv(source, index=False, encoding="gbk")
                source_bytes[source] = source.read_bytes()
                corrected = bad.copy()
                corrected.loc[0, ["high", "low", "close"]] = [103.0, 101.0, 102.0]
                corrected.to_csv(
                    replacement_dir / source.name,
                    index=False,
                    encoding="gbk",
                )

            summary, bad_rows = scan_data_dir(
                data_dir,
                overlay_dir=overlay_dir,
                recent_rows=10,
                cross_sectional_spike_ratio=0.5,
                min_eligible=2,
            )

            self.assertEqual("NO_MARKET_WIDE_SPIKE", summary["status"])
            self.assertEqual(2, summary["overlay_files_used"])
            self.assertEqual([], bad_rows)
            for path, original in source_bytes.items():
                self.assertEqual(original, path.read_bytes())


class MissingBarSemanticsTests(unittest.TestCase):
    def setUp(self):
        self.key = MarketSessionKey("000001", date(2026, 7, 30))

    def _confirmation(self):
        return NoBarConfirmation(
            key=self.key,
            source_id="exchange-status",
            evidence_ref="provider-response:2026-07-30:000001",
        )

    def _fetch_failure(self):
        return FetchFailure(
            key=self.key,
            source_id="eastmoney",
            error_ref="request-error:timeout",
        )

    def test_present_payload_is_released_read_only_through_the_use_gate(self):
        present = classify_bar_availability(
            key=self.key,
            bar_payload={"close": 10.5, "volume": 1_000},
        )

        payload = present.require_usable_for(BarUse.FEATURE)

        self.assertEqual({"close": 10.5, "volume": 1_000}, dict(payload))
        self.assertFalse(hasattr(present, "bar_payload"))
        with self.assertRaises(TypeError):
            payload["close"] = 9.5

    def test_present_payload_is_recursively_snapshotted_and_frozen(self):
        source = {
            "close": 10.5,
            "metadata": {"quality": "valid", "flags": ["source-ok"]},
        }
        present = classify_bar_availability(
            key=self.key,
            bar_payload=source,
        )

        source["metadata"]["quality"] = "corrupt"
        source["metadata"]["flags"].append("late-mutation")
        payload = present.require_usable_for(BarUse.FEATURE)

        self.assertEqual("valid", payload["metadata"]["quality"])
        self.assertEqual(("source-ok",), payload["metadata"]["flags"])
        with self.assertRaises(TypeError):
            payload["metadata"]["quality"] = "corrupt"

    def test_no_bar_confirmation_is_bound_to_instrument_and_session(self):
        other_key = MarketSessionKey("000002", date(2026, 7, 30))
        confirmation = NoBarConfirmation(
            key=other_key,
            source_id="exchange-status",
            evidence_ref="provider-response:2026-07-30:000002",
        )

        with self.assertRaises(ValueError):
            classify_bar_availability(
                key=self.key,
                no_bar_confirmation=confirmation,
            )

    def test_fetch_failure_is_bound_to_instrument_and_session(self):
        other_key = MarketSessionKey("000002", date(2026, 7, 30))
        failure = FetchFailure(
            key=other_key,
            source_id="eastmoney",
            error_ref="request-error:timeout",
        )

        with self.assertRaises(ValueError):
            classify_bar_availability(key=self.key, fetch_failure=failure)

    def test_only_source_confirmed_absence_is_suspension(self):
        confirmed = classify_bar_availability(
            key=self.key,
            no_bar_confirmation=self._confirmation(),
        )
        unknown = classify_bar_availability(key=self.key)

        self.assertEqual(BarAvailability.NO_BAR_CONFIRMED, confirmed.availability)
        self.assertTrue(confirmed.is_suspended)
        self.assertEqual(BarAvailability.UNKNOWN_NO_BAR, unknown.availability)
        self.assertFalse(unknown.is_suspended)

    def test_fetch_failure_remains_distinct_from_unknown_or_suspension(self):
        failed = classify_bar_availability(
            key=self.key,
            fetch_failure=self._fetch_failure(),
        )

        self.assertEqual(BarAvailability.FETCH_FAILED, failed.availability)
        self.assertFalse(failed.is_suspended)
        self.assertEqual("request-error:timeout", failed.fetch_failure.error_ref)

    def test_classification_rejects_conflicting_or_noncanonical_evidence(self):
        invalid_inputs = (
            {
                "key": self.key,
                "bar_payload": {"close": 10.5},
                "no_bar_confirmation": self._confirmation(),
            },
            {
                "key": self.key,
                "bar_payload": {"close": 10.5},
                "fetch_failure": self._fetch_failure(),
            },
            {
                "key": self.key,
                "no_bar_confirmation": self._confirmation(),
                "fetch_failure": self._fetch_failure(),
            },
        )

        for values in invalid_inputs:
            with self.subTest(values=values), self.assertRaises(ValueError):
                classify_bar_availability(**values)
        with self.assertRaises(ValueError):
            NoBarConfirmation(
                key=self.key,
                source_id="  ",
                evidence_ref="provider-response:invalid",
            )
        with self.assertRaises(ValueError):
            NoBarConfirmation(
                key=self.key,
                source_id=None,
                evidence_ref="provider-response:invalid",
            )
        with self.assertRaises(ValueError):
            FetchFailure(
                key=self.key,
                source_id="eastmoney",
                error_ref=None,
            )

    def test_only_present_bar_can_feed_features_signals_entries_or_exits(self):
        present = classify_bar_availability(
            key=self.key,
            bar_payload={"close": 10.5},
        )
        unavailable = (
            classify_bar_availability(
                key=self.key,
                no_bar_confirmation=self._confirmation(),
            ),
            classify_bar_availability(key=self.key),
            classify_bar_availability(
                key=self.key,
                fetch_failure=self._fetch_failure(),
            ),
        )

        for use in BarUse:
            with self.subTest(use=use, availability=BarAvailability.PRESENT):
                self.assertEqual(
                    {"close": 10.5},
                    dict(present.require_usable_for(use)),
                )
            for observation in unavailable:
                with (
                    self.subTest(use=use, availability=observation.availability),
                    self.assertRaises(MarketDataUseError),
                ):
                    observation.require_usable_for(use)

    def test_stale_valuation_is_nav_only_and_cannot_feed_a_model(self):
        missing = classify_bar_availability(key=self.key)

        valuation = value_portfolio_position(
            missing,
            current_value=None,
            last_known_value=125_000.0,
        )
        nav_value = valuation.for_portfolio_nav()

        self.assertEqual(ValuationState.STALE_VALUATION, valuation.state)
        self.assertTrue(valuation.is_stale)
        self.assertNotIsInstance(nav_value, (int, float))
        self.assertEqual(ValuationState.STALE_VALUATION, nav_value.state)
        self.assertEqual(125_000.0, nav_value.amount)
        self.assertFalse(hasattr(valuation, "value"))
        with self.assertRaises(MarketDataUseError):
            valuation.for_model_feature()

    def test_stale_valuation_preserves_each_missing_state_and_session_key(self):
        observations = (
            classify_bar_availability(
                key=self.key,
                no_bar_confirmation=self._confirmation(),
            ),
            classify_bar_availability(key=self.key),
            classify_bar_availability(
                key=self.key,
                fetch_failure=self._fetch_failure(),
            ),
        )

        for observation in observations:
            valuation = value_portfolio_position(
                observation,
                current_value=None,
                last_known_value=125_000.0,
            )
            nav_value = valuation.for_portfolio_nav()
            with self.subTest(availability=observation.availability):
                self.assertEqual(
                    observation.availability,
                    valuation.source_availability,
                )
                self.assertEqual(
                    observation.availability,
                    nav_value.source_availability,
                )
                self.assertEqual(self.key, nav_value.key)

    def test_present_bar_requires_current_valuation_before_model_use(self):
        present = classify_bar_availability(
            key=self.key,
            bar_payload={"close": 10.5},
        )

        with self.assertRaises(ValueError):
            value_portfolio_position(
                present,
                current_value=None,
                last_known_value=120_000.0,
            )

        valuation = value_portfolio_position(
            present,
            current_value=125_000.0,
            last_known_value=120_000.0,
        )
        self.assertEqual(ValuationState.CURRENT, valuation.state)
        self.assertFalse(valuation.is_stale)
        self.assertEqual(125_000.0, valuation.for_model_feature())

    def test_presence_without_a_bound_payload_is_rejected(self):
        with self.assertRaises((TypeError, ValueError)):
            classify_bar_availability(bar_present=True)

    def test_classified_and_valuation_objects_cannot_bypass_public_factories(self):
        with self.assertRaises(TypeError):
            BarObservation(
                availability=BarAvailability.PRESENT,
                key=self.key,
                _bar_payload={"close": 10.5},
            )
        with self.assertRaises(TypeError):
            PortfolioValuation(
                _value=125_000.0,
                state=ValuationState.CURRENT,
                source_availability=BarAvailability.PRESENT,
                key=self.key,
            )
        with self.assertRaises(ValueError):
            NavValuation(
                amount=125_000.0,
                state=ValuationState.STALE_VALUATION,
                source_availability=BarAvailability.PRESENT,
                key=self.key,
            )


if __name__ == "__main__":
    unittest.main()
