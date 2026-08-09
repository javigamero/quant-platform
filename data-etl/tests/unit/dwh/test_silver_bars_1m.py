from datetime import date, time

import numpy as np
import pandas as pd
import pytest

from src.dwh import schemas
from src.dwh.silver import bars_1m

_SYMBOL = "AAPL"
_FULL_SESSION = date(2016, 1, 4)
_NEXT_SESSION = date(2016, 1, 5)
_HALF_SESSION = date(2016, 1, 6)


_DEFAULT_SESSIONS = [
    (_FULL_SESSION, time(16, 0)),
    (_NEXT_SESSION, time(16, 0)),
    (_HALF_SESSION, time(13, 0)),
]


def make_calendar(sessions=None) -> pd.DataFrame:
    sessions = _DEFAULT_SESSIONS if sessions is None else sessions

    if not sessions:
        return pd.DataFrame(
            columns=["session_date", "open_et", "close_et", "session_minutes", "is_half_day"]
        )

    return pd.DataFrame([
        {
            "session_date": session_date,
            "open_et": time(9, 30),
            "close_et": close_et,
            "session_minutes": (close_et.hour * 60 + close_et.minute) - (9 * 60 + 30),
            "is_half_day": close_et < time(16, 0),
        }
        for session_date, close_et in sessions
    ])


def make_bars(session_dates=(_FULL_SESSION,), close=100.0, symbol=_SYMBOL) -> pd.DataFrame:
    """Builds a complete set of bronze bars for whole sessions, at a flat price."""

    calendar = make_calendar().set_index("session_date")
    frames = []

    for session_date in session_dates:
        minutes = int(calendar.loc[session_date, "session_minutes"])
        session_open = pd.Timestamp(f"{session_date} 09:30", tz=bars_1m.MARKET_TIMEZONE)
        frames.append(pd.DataFrame({
            "symbol": symbol,
            "timestamp_at": pd.date_range(session_open, periods=minutes, freq="1min").tz_convert("UTC"),
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": 1_000,
            "trade_count": 10,
            "vwap": close,
            "feed": "sip",
            "adjustment": "raw",
        }))

    return pd.concat(frames, ignore_index=True)


class TestBuildSessionGrid:

    def test_full_session_has_390_minutes(self):
        grid = bars_1m.build_session_grid(make_calendar([(_FULL_SESSION, time(16, 0))]), _SYMBOL)
        assert len(grid) == bars_1m.REGULAR_SESSION_MINUTES

    def test_half_session_has_210_minutes(self):
        grid = bars_1m.build_session_grid(make_calendar([(_HALF_SESSION, time(13, 0))]), _SYMBOL)
        assert len(grid) == bars_1m.HALF_SESSION_MINUTES

    def test_first_minute_is_the_0930_et_open(self):
        grid = bars_1m.build_session_grid(make_calendar([(_FULL_SESSION, time(16, 0))]), _SYMBOL)
        assert grid.iloc[0]["timestamp_at"] == pd.Timestamp("2016-01-04 14:30", tz="UTC")
        assert grid.iloc[0]["minute_index"] == 0

    def test_last_minute_is_the_one_before_the_close(self):
        grid = bars_1m.build_session_grid(make_calendar([(_FULL_SESSION, time(16, 0))]), _SYMBOL)
        assert grid.iloc[-1]["timestamp_at"] == pd.Timestamp("2016-01-04 20:59", tz="UTC")
        assert grid.iloc[-1]["minute_index"] == 389

    def test_summer_sessions_open_an_hour_earlier_in_utc(self):
        grid = bars_1m.build_session_grid(make_calendar([(date(2016, 7, 5), time(16, 0))]), _SYMBOL)
        assert grid.iloc[0]["timestamp_at"] == pd.Timestamp("2016-07-05 13:30", tz="UTC")

    def test_grid_is_built_per_symbol(self):
        grid = bars_1m.build_session_grid(make_calendar([(_FULL_SESSION, time(16, 0))]), ["AAPL", "SPY"])
        assert grid.groupby("symbol").size().tolist() == [390, 390]

    def test_empty_calendar_yields_an_empty_grid(self):
        assert bars_1m.build_session_grid(make_calendar([]), _SYMBOL).empty


class TestGapFilling:

    @pytest.fixture
    def bars_with_a_gap(self):
        bars = make_bars()
        return bars.drop(index=[5, 6]).reset_index(drop=True)

    def test_missing_minutes_are_restored(self, bars_with_a_gap):
        frame = bars_1m.build_bars_1m(bars_with_a_gap, make_calendar())
        assert len(frame) == bars_1m.REGULAR_SESSION_MINUTES

    def test_restored_minutes_are_flagged(self, bars_with_a_gap):
        frame = bars_1m.build_bars_1m(bars_with_a_gap, make_calendar())
        assert frame["is_imputed"].sum() == 2
        assert frame.loc[[5, 6], "is_imputed"].all()

    def test_restored_minutes_carry_the_previous_close(self, bars_with_a_gap):
        frame = bars_1m.build_bars_1m(bars_with_a_gap, make_calendar())
        imputed = frame.loc[5]
        assert imputed["open"] == imputed["high"] == imputed["low"] == imputed["close"] == 100.0

    def test_restored_minutes_have_no_activity(self, bars_with_a_gap):
        frame = bars_1m.build_bars_1m(bars_with_a_gap, make_calendar())
        assert frame.loc[5, "volume"] == 0
        assert frame.loc[5, "trade_count"] == 0

    def test_restored_minutes_use_the_close_as_vwap(self, bars_with_a_gap):
        frame = bars_1m.build_bars_1m(bars_with_a_gap, make_calendar())
        assert frame.loc[5, "vwap"] == frame.loc[5, "close"]

    def test_real_minutes_are_not_flagged(self, bars_with_a_gap):
        frame = bars_1m.build_bars_1m(bars_with_a_gap, make_calendar())
        assert not frame.loc[0, "is_imputed"]

    def test_sessions_starting_before_the_first_bar_are_dropped_whole(self):
        bars = make_bars(session_dates=[_FULL_SESSION, _NEXT_SESSION])
        bars = bars[bars["timestamp_at"] > pd.Timestamp("2016-01-04 15:00", tz="UTC")]

        frame = bars_1m.build_bars_1m(bars, make_calendar())

        assert set(frame["session_date"]) == {_NEXT_SESSION}


class TestSessionShape:

    def test_extended_hours_bars_are_excluded(self):
        bars = make_bars()
        premarket = bars.iloc[[0]].copy()
        premarket["timestamp_at"] = pd.Timestamp("2016-01-04 09:00", tz="UTC")

        frame = bars_1m.build_bars_1m(pd.concat([premarket, bars], ignore_index=True), make_calendar())

        assert len(frame) == bars_1m.REGULAR_SESSION_MINUTES

    def test_half_days_produce_210_rows(self):
        frame = bars_1m.build_bars_1m(make_bars(session_dates=[_HALF_SESSION]), make_calendar())
        assert len(frame) == bars_1m.HALF_SESSION_MINUTES
        assert frame["is_half_day"].all()

    def test_output_matches_the_contract_columns(self):
        frame = bars_1m.build_bars_1m(make_bars(), make_calendar())
        assert list(frame.columns) == list(schemas.BARS_1M_SCHEMA.names)

    def test_empty_input_returns_an_empty_frame(self):
        frame = bars_1m.build_bars_1m(pd.DataFrame(columns=["symbol"]), make_calendar())
        assert frame.empty

    def test_mixing_feeds_is_rejected(self):
        bars = make_bars()
        bars.loc[0, "feed"] = "iex"

        with pytest.raises(ValueError, match="single 'feed'"):
            bars_1m.build_bars_1m(bars, make_calendar())


class TestAdjustmentFactors:

    _SPLIT = pd.DataFrame([
        {"symbol": _SYMBOL, "ex_date": _HALF_SESSION, "type": "split", "ratio": 4.0, "cash_amount": np.nan}
    ])

    def test_no_actions_leaves_every_factor_at_one(self):
        factors = bars_1m.compute_adjustment_factors([_FULL_SESSION], pd.DataFrame(), pd.Series(dtype=float))
        assert factors.loc[_FULL_SESSION, "adj_factor"] == 1.0

    def test_split_scales_earlier_sessions_down(self):
        factors = bars_1m.compute_adjustment_factors(
            [_FULL_SESSION, _HALF_SESSION], self._SPLIT, pd.Series(dtype=float)
        )
        assert factors.loc[_FULL_SESSION, "adj_factor"] == pytest.approx(0.25)

    def test_split_does_not_touch_the_ex_date_itself(self):
        factors = bars_1m.compute_adjustment_factors(
            [_FULL_SESSION, _HALF_SESSION], self._SPLIT, pd.Series(dtype=float)
        )
        assert factors.loc[_HALF_SESSION, "adj_factor"] == 1.0

    def test_cash_dividend_uses_the_previous_close(self):
        dividend = pd.DataFrame([
            {"symbol": _SYMBOL, "ex_date": _HALF_SESSION, "type": "cash_dividend",
             "ratio": np.nan, "cash_amount": 2.0}
        ])
        daily_close = pd.Series({_FULL_SESSION: 100.0, _NEXT_SESSION: 100.0})

        factors = bars_1m.compute_adjustment_factors(
            [_FULL_SESSION, _NEXT_SESSION, _HALF_SESSION], dividend, daily_close
        )

        assert factors.loc[_FULL_SESSION, "adj_factor"] == pytest.approx(0.98)

    def test_dividend_before_any_close_is_skipped(self):
        dividend = pd.DataFrame([
            {"symbol": _SYMBOL, "ex_date": _FULL_SESSION, "type": "cash_dividend",
             "ratio": np.nan, "cash_amount": 2.0}
        ])
        factors = bars_1m.compute_adjustment_factors([_FULL_SESSION], dividend, pd.Series(dtype=float))

        assert factors.loc[_FULL_SESSION, "adj_factor"] == 1.0

    def test_factors_of_several_actions_compound(self):
        actions = pd.concat([
            self._SPLIT,
            pd.DataFrame([{"symbol": _SYMBOL, "ex_date": _NEXT_SESSION, "type": "split",
                           "ratio": 2.0, "cash_amount": np.nan}]),
        ], ignore_index=True)

        factors = bars_1m.compute_adjustment_factors(
            [_FULL_SESSION, _NEXT_SESSION, _HALF_SESSION], actions, pd.Series(dtype=float)
        )

        assert factors.loc[_FULL_SESSION, "adj_factor"] == pytest.approx(0.125)

    def test_mergers_are_ignored_rather_than_guessed(self):
        merger = pd.DataFrame([
            {"symbol": _SYMBOL, "ex_date": _HALF_SESSION, "type": "merger",
             "ratio": np.nan, "cash_amount": np.nan}
        ])
        factors = bars_1m.compute_adjustment_factors([_FULL_SESSION], merger, pd.Series(dtype=float))

        assert factors.loc[_FULL_SESSION, "adj_factor"] == 1.0

    def test_only_splits_feed_the_volume_factor(self):
        dividend = pd.DataFrame([
            {"symbol": _SYMBOL, "ex_date": _HALF_SESSION, "type": "cash_dividend",
             "ratio": np.nan, "cash_amount": 2.0}
        ])
        daily_close = pd.Series({_FULL_SESSION: 100.0})
        factors = bars_1m.compute_adjustment_factors([_FULL_SESSION, _HALF_SESSION], dividend, daily_close)

        assert factors.loc[_FULL_SESSION, "split_factor"] == 1.0


class TestAdjustedColumns:

    @pytest.fixture
    def frame(self):
        split = pd.DataFrame([
            {"symbol": _SYMBOL, "ex_date": _NEXT_SESSION, "type": "split", "ratio": 4.0, "cash_amount": np.nan}
        ])
        return bars_1m.build_bars_1m(
            make_bars(session_dates=[_FULL_SESSION, _NEXT_SESSION]), make_calendar(), split
        )

    def test_prices_before_the_split_are_divided_by_the_ratio(self, frame):
        before = frame[frame["session_date"] == _FULL_SESSION].iloc[0]
        assert before["close_adj"] == pytest.approx(25.0)

    def test_prices_from_the_split_onwards_are_untouched(self, frame):
        after = frame[frame["session_date"] == _NEXT_SESSION].iloc[0]
        assert after["close_adj"] == pytest.approx(100.0)

    def test_volume_before_the_split_is_multiplied_by_the_ratio(self, frame):
        before = frame[frame["session_date"] == _FULL_SESSION].iloc[0]
        assert before["volume_adj"] == 4_000

    def test_raw_prices_are_preserved_alongside(self, frame):
        before = frame[frame["session_date"] == _FULL_SESSION].iloc[0]
        assert before["close"] == pytest.approx(100.0)

    def test_every_price_column_gets_an_adjusted_twin(self, frame):
        before = frame[frame["session_date"] == _FULL_SESSION].iloc[0]
        for column in ("open_adj", "high_adj", "low_adj", "vwap_adj"):
            assert before[column] == pytest.approx(25.0)

    def test_a_split_only_adjusts_its_own_symbol(self):
        bars = pd.concat([
            make_bars(session_dates=[_FULL_SESSION, _NEXT_SESSION], symbol="AAPL"),
            make_bars(session_dates=[_FULL_SESSION, _NEXT_SESSION], symbol="SPY"),
        ], ignore_index=True)
        split = pd.DataFrame([
            {"symbol": "AAPL", "ex_date": _NEXT_SESSION, "type": "split", "ratio": 4.0, "cash_amount": np.nan}
        ])

        frame = bars_1m.build_bars_1m(bars, make_calendar(), split)
        first_session = frame[frame["session_date"] == _FULL_SESSION]

        assert first_session[first_session["symbol"] == "AAPL"]["close_adj"].iloc[0] == pytest.approx(25.0)
        assert first_session[first_session["symbol"] == "SPY"]["close_adj"].iloc[0] == pytest.approx(100.0)


class TestValidation:

    def test_a_clean_frame_reports_no_violations(self):
        frame = bars_1m.build_bars_1m(make_bars(), make_calendar())
        assert bars_1m.validate_bars_1m(frame, make_calendar()) == []

    def test_detects_ohlc_bounds_violations(self):
        frame = bars_1m.build_bars_1m(make_bars(), make_calendar())
        frame.loc[0, "high"] = 1.0

        assert any("low <= min(open, close)" in violation
                   for violation in bars_1m.validate_bars_1m(frame, make_calendar()))

    def test_detects_vwap_outside_the_bar_range(self):
        frame = bars_1m.build_bars_1m(make_bars(), make_calendar())
        frame.loc[0, "vwap"] = 500.0

        assert any("vwap outside" in violation
                   for violation in bars_1m.validate_bars_1m(frame, make_calendar()))

    def test_detects_volume_without_trades(self):
        frame = bars_1m.build_bars_1m(make_bars(), make_calendar())
        frame.loc[0, "trade_count"] = 0

        assert any("volume > 0" in violation
                   for violation in bars_1m.validate_bars_1m(frame, make_calendar()))

    def test_detects_a_session_with_missing_rows(self):
        frame = bars_1m.build_bars_1m(make_bars(), make_calendar())

        assert any("unexpected row count" in violation
                   for violation in bars_1m.validate_bars_1m(frame.drop(index=[3]), make_calendar()))

    def test_detects_duplicate_minutes(self):
        frame = bars_1m.build_bars_1m(make_bars(), make_calendar())
        duplicated = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)

        assert any("duplicate row" in violation
                   for violation in bars_1m.validate_bars_1m(duplicated, make_calendar()))

    def test_detects_an_unexplained_price_jump(self):
        frame = bars_1m.build_bars_1m(make_bars(), make_calendar())
        frame.loc[10, "close_adj"] = 1_000.0

        assert any("unexplained 1-minute return" in violation
                   for violation in bars_1m.validate_bars_1m(frame, make_calendar()))

    def test_a_jump_on_a_corporate_action_date_is_explained(self):
        frame = bars_1m.build_bars_1m(make_bars(), make_calendar())
        frame.loc[10, "close_adj"] = 1_000.0
        actions = pd.DataFrame([
            {"symbol": _SYMBOL, "ex_date": _FULL_SESSION, "type": "split", "ratio": 4.0, "cash_amount": np.nan}
        ])

        assert not any("unexplained 1-minute return" in violation
                       for violation in bars_1m.validate_bars_1m(frame, make_calendar(), actions))

    def test_the_overnight_gap_is_not_treated_as_a_1_minute_return(self):
        bars = pd.concat([
            make_bars(session_dates=[_FULL_SESSION], close=100.0),
            make_bars(session_dates=[_NEXT_SESSION], close=10.0),
        ], ignore_index=True)
        frame = bars_1m.build_bars_1m(bars, make_calendar())

        assert not any("unexplained 1-minute return" in violation
                       for violation in bars_1m.validate_bars_1m(frame, make_calendar()))

    def test_assert_raises_on_violations(self):
        frame = bars_1m.build_bars_1m(make_bars(), make_calendar())
        frame.loc[0, "vwap"] = 500.0

        with pytest.raises(bars_1m.SilverValidationError):
            bars_1m.assert_bars_1m_valid(frame, make_calendar())

    def test_assert_passes_on_a_clean_frame(self):
        frame = bars_1m.build_bars_1m(make_bars(), make_calendar())
        bars_1m.assert_bars_1m_valid(frame, make_calendar())


class TestRunBronzeToSilver:

    def _seed_bronze(self, lakehouse):
        from src.dwh.bronze import alpaca_bars, alpaca_reference

        bars = make_bars(session_dates=[_FULL_SESSION, _NEXT_SESSION])
        bars["currency"] = "USD"
        table = alpaca_bars.build_bars_raw(bars, feed="sip", adjustment="raw", currency="USD")
        lakehouse.write_partitions(
            table, schemas.BRONZE_LAYER, schemas.BARS_RAW_TABLE, schemas.PARTITION_COLUMNS
        )

        calendar = make_calendar()
        calendar["settlement_date"] = None
        lakehouse.write_table(
            alpaca_reference.build_market_calendar(calendar),
            schemas.BRONZE_LAYER,
            schemas.MARKET_CALENDAR_TABLE,
        )

        actions = pd.DataFrame([
            {"symbol": _SYMBOL, "ex_date": _NEXT_SESSION, "type": "split", "ratio": 4.0, "cash_amount": np.nan}
        ])
        lakehouse.write_table(
            alpaca_reference.build_corporate_actions(actions),
            schemas.BRONZE_LAYER,
            schemas.CORPORATE_ACTIONS_TABLE,
        )

    def test_writes_a_validated_silver_table(self, lakehouse):
        self._seed_bronze(lakehouse)
        frame = bars_1m.run_bronze_to_silver(_SYMBOL, lakehouse=lakehouse)

        assert len(frame) == 2 * bars_1m.REGULAR_SESSION_MINUTES

        written = lakehouse.read_table(schemas.SILVER_LAYER, schemas.BARS_1M_TABLE)
        assert written.num_rows == len(frame)

    def test_written_table_matches_the_contract_schema(self, lakehouse):
        self._seed_bronze(lakehouse)
        bars_1m.run_bronze_to_silver(_SYMBOL, lakehouse=lakehouse)

        table = lakehouse.read_table(schemas.SILVER_LAYER, schemas.BARS_1M_TABLE)

        assert set(table.schema.names) == set(schemas.BARS_1M_SCHEMA.names)
        assert table.schema.field("adj_factor").type == schemas.BARS_1M_SCHEMA.field("adj_factor").type

    def test_the_table_is_registered_in_the_catalog(self, lakehouse):
        self._seed_bronze(lakehouse)
        bars_1m.run_bronze_to_silver(_SYMBOL, lakehouse=lakehouse)

        registered = lakehouse.client.get_table("lakehouse.silver.fact_bars_1m")

        assert registered["data_source_format"] == "DELTA"
        assert [column["name"] for column in registered["columns"] if "partition_index" in column] == [
            "symbol", "year", "month"
        ]

    def test_rerunning_does_not_duplicate_rows(self, lakehouse):
        self._seed_bronze(lakehouse)
        bars_1m.run_bronze_to_silver(_SYMBOL, lakehouse=lakehouse)
        bars_1m.run_bronze_to_silver(_SYMBOL, lakehouse=lakehouse)

        written = lakehouse.read_table(schemas.SILVER_LAYER, schemas.BARS_1M_TABLE)
        assert written.num_rows == 2 * bars_1m.REGULAR_SESSION_MINUTES

    def test_the_split_survives_the_round_trip(self, lakehouse):
        self._seed_bronze(lakehouse)
        frame = bars_1m.run_bronze_to_silver(_SYMBOL, lakehouse=lakehouse)

        assert frame[frame["session_date"] == _FULL_SESSION]["close_adj"].iloc[0] == pytest.approx(25.0)

    def test_missing_bronze_produces_nothing(self, lakehouse):
        assert bars_1m.run_bronze_to_silver(_SYMBOL, lakehouse=lakehouse).empty
