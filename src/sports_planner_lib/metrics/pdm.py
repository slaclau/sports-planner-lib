import numpy as np
import sweat

from sports_planner_lib.metrics.base import ActivityMetric
from sports_planner_lib.utils import format


class DurationRegressor(sweat.PowerDurationRegressor):
    def __init__(
        self,
        model="2 param",
        cp=300,
        w_prime=20000,
        p_max=1000,
        tau=300,
        tau2=1800,
        tcp_max=1800,
        a=50,
    ):
        self.model = model
        self.cp = cp
        self.w_prime = w_prime
        self.p_max = p_max
        self.tau = tau
        self.tau2 = tau2
        self.tcp_max = tcp_max
        self.a = a

    def _aerobic(self, X, cp, w_prime, tau, tau2, a):
        t = X.T[0]
        result = cp * (1 - np.exp(-t / tau2))

        return np.where(
            t <= self.tcp_max, result, result - a * np.log(t / self.tcp_max)
        )

    def _anaerobic(self, X, cp, w_prime, tau, tau2, a):
        t = X.T[0]
        return w_prime / t * (1 - np.exp(-t / tau))

    def predict_ae(self, X):
        assert self.model == "pt"
        _, params = self._model_selection()
        func = self._aerobic

        args = []
        for param_name in params:
            args.append(getattr(self, f"{param_name}_"))

        return func(X, *args)

    def predict_an(self, X):
        assert self.model == "pt"
        _, params = self._model_selection()
        func = self._anaerobic

        args = []
        for param_name in params:
            args.append(getattr(self, f"{param_name}_"))

        return func(X, *args)

    def _pt_model(self, X, cp, w_prime, tau, tau2, a):
        return self._aerobic(X, cp, w_prime, tau, tau2, a) + self._anaerobic(
            X, cp, w_prime, tau, tau2, a
        )

    def _model_selection(self):
        if self.model == "pt":
            func = self._pt_model
            params = ["cp", "w_prime", "tau", "tau2", "a"]
            return func, params
        return super()._model_selection()


class CurveMeta(type):
    """A metaclass for :class:`Curve`."""

    classes = {}

    @classmethod
    def __getitem__(mcs, item):
        """Return a new subclass of :class:`Curve`."""
        if item in mcs.classes:
            return mcs.classes[item]
        curve = type(
            f'Curve["{item}"]',
            (Curve,),
            {"column": item, "name": f"{item}-duration curve"},
        )
        mcs.classes[item] = curve
        return curve


class Curve(ActivityMetric, metaclass=CurveMeta):
    """A curve showing the maximum durations for different values."""

    column: str  #: The column of the dataframe to compute the curve for

    def applicable(self):
        """

        Returns
        -------
        bool
            `True` if the column exists
        """
        return self.column in self.df.columns

    def compute(self):
        """

        Returns
        -------
        dict
            A dictionary with the keys for each model
        """
        y = self.activity.meanmaxes_df[f"mean_max_{self.column}"]
        x = range(1, len(y) + 1)
        X = sweat.array_1d_to_2d(x)

        rtn = {}
        for model in ["2 param", "3 param", "exponential", "omni", "pt"]:
            try:
                model_instance = DurationRegressor(model=model)
                model_instance.fit(X, y)
                func, params = model_instance._model_selection()
                rtn[model] = {
                    param: getattr(model_instance, f"{param}_") for param in params
                }

            except RuntimeError as e:
                pass
            except ValueError as e:
                pass
        return rtn


class MeanMaxMeta(type):
    """A metaclass for :class:`MeanMax`."""

    classes = {}

    @classmethod
    def __getitem__(mcs, item):
        """Return a new subclass of :class:`MeanMax`."""
        if item in mcs.classes:
            return mcs.classes[item]
        column = item[0]
        time = item[1]
        duration = format.time(time)
        mean_max = type(
            f'MeanMax["{column}", {time}]',
            (MeanMax,),
            {
                "column": column,
                "time": time,
                "deps": [],
                "name": f"{duration} max {column}",
            },
        )
        mcs.classes[item] = mean_max
        return mean_max


class MeanMax(ActivityMetric, metaclass=MeanMaxMeta):
    """The maximum of a quantity for a given time."""

    column: str
    time: int
    format = ".2f"

    def applicable(self) -> bool:
        """

        Returns
        -------
        bool
            `True` if the relevant :class:`Curve` extends for a sufficient duration
        """
        return self.column in self.df.columns and len(self.df.index) >= self.time

    def compute(self) -> float:
        """

        Returns
        -------
        float
            The corresponding meanmax value
        """
        return float(
            getattr(self.activity.meanmaxes[self.time - 1], f"mean_max_{self.column}")
        )
