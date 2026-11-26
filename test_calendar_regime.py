import unittest

import pandas as pd

from strategies.reversal_core import classify_multiday_direction


class CalendarRegimeTests(unittest.TestCase):
    def frame(self):
        times = pd.date_range('2026-11-20', periods=100, freq='h', tz='Europe/Moscow')
        return pd.DataFrame([
            dict(time=t, is_complete=True, close=100+i, ema20=99+i,
                 ema50=98+i, atr=2., macd=1., macd_signal=0., ao=1.)
            for i, t in enumerate(times)
        ])

    def test_calendar_windows_do_not_expand_across_session_gaps(self):
        frame = self.frame()
        frame = frame[frame.time.dt.hour.between(9, 18)]
        result = classify_multiday_direction(frame, 'LONG')
        self.assertTrue(result['available'])
        self.assertLess(result['window_bars'], 48)
        self.assertLessEqual(pd.Timestamp(result['window_end'])-pd.Timestamp(result['window_start']), pd.Timedelta(hours=72))
        self.assertEqual(result['regime'], 'trend')

    def test_many_bars_do_not_replace_two_calendar_days(self):
        frame = self.frame()
        frame['time'] = pd.date_range('2026-11-20', periods=100, freq='15min', tz='UTC')
        self.assertFalse(classify_multiday_direction(frame, 'LONG')['available'])

    def test_48_hour_move_uses_elapsed_time_not_48_observations(self):
        frame = self.frame().iloc[::2].copy()
        result = classify_multiday_direction(frame, 'LONG')
        # Last close=198, close exactly 48h earlier=150, ATR=2.
        self.assertEqual(result['move_48_atr'], 24.)
        self.assertEqual(result['window_bars'], 37)

    def test_old_candles_cannot_change_window_statistics(self):
        frame = self.frame()
        expected = classify_multiday_direction(frame, 'LONG')
        frame.loc[:20, ['close', 'ema20', 'ema50', 'atr']] = 1000000.
        self.assertEqual(classify_multiday_direction(frame, 'LONG'), expected)

    def test_timezone_order_duplicates_and_incomplete_candle(self):
        frame = self.frame()
        expected = classify_multiday_direction(frame, 'LONG')
        future = frame.iloc[[-1]].copy()
        future['time'] += pd.Timedelta(hours=1)
        future['is_complete'] = False
        future['close'] = 0.
        frame = pd.concat([frame, frame.iloc[[-1]], future]).iloc[::-1]
        frame['time'] = frame.time.dt.tz_convert('UTC')
        self.assertEqual(classify_multiday_direction(frame, 'LONG'), expected)

    def test_no_timestamp_or_long_closure_fails_closed(self):
        frame = self.frame()
        self.assertFalse(classify_multiday_direction(frame.drop(columns='time'), 'LONG')['available'])
        self.assertFalse(classify_multiday_direction(frame.iloc[list(range(30))+[99]], 'LONG')['available'])

    def test_direction_is_symmetric(self):
        frame = self.frame()
        expected = classify_multiday_direction(frame, 'LONG')
        for col in ['close','ema20','ema50','macd','macd_signal','ao']:
            frame[col] = -frame[col]
        self.assertEqual(classify_multiday_direction(frame, 'SHORT'), expected)


if __name__ == '__main__':
    unittest.main()
