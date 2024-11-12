import numpy as np
import pandas as pd
import plotly.express
import sweat

from sports_planner_lib.athlete import Athlete
from sports_planner_lib.metrics.activity import Curve, DurationRegressor
from sports_planner_lib.utils.logging import info_time


class LongTermCurve:
    """Equivalent to :class:`sports_planner_lib.metric.activity.Curve`"""

    @info_time
    def __init__(self, athlete: Athlete, column: str, days: int, callback_func=None):
        curves = []
        for _, activities in athlete.days.activities[0:days].iteritems():
            if isinstance(activities, float):
                continue
            for activity in activities:
                curves.append(activity.get_metric(Curve[column]))

        self.y = np.array([])

        for i in range(0, 3600):
            vals = [
                curve["y"][i]
                for curve in curves
                if curve is not None and len(curve["y"]) > i
            ]
            self.y = np.append(
                self.y,
                max(vals),
            )

        self.x = list(range(0, len(self.y)))
        X = sweat.array_1d_to_2d(self.x)
        self.y = pd.Series(index=self.x, data=self.y)
        print(self.y)
        pt = DurationRegressor(model="omni")
        pt.fit(X, self.y)
        print(f"fitted: {pt.is_fitted_}, params: {pt.get_params()}")

        prediction = pt.predict(X)

        fig = plotly.express.line(x=self.x, y=self.y)
        fig.show()

        fig = plotly.express.line(x=self.x, y=prediction)
        fig.show()
