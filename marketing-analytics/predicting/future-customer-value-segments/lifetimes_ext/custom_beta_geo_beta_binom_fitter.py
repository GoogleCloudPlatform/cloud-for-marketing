# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Custom Beta-Geo Beta-Binomial Fitter.

Fixes https://github.com/CamDavidsonPilon/lifetimes/issues/259 by overriding
the `log_likelihood` and `negative_log_likelihood` static methods of the base
`BetaGeoBetaBinomFitter`.
"""
import numpy as np
from autograd.numpy import log, exp, logaddexp
from autograd.scipy.special import betaln

from lifetimes import BetaGeoBetaBinomFitter as BaseBetaGeoBetaBinomFitter


class BetaGeoBetaBinomFitter(BaseBetaGeoBetaBinomFitter):
    def __init__(self, penalizer_coef=0.0):
        """Initialization, set penalizer_coef."""
        super().__init__(penalizer_coef)

    @staticmethod
    def _loglikelihood(params, x, tx, T):
        """Log likelihood for optimizer."""
        alpha, beta, gamma, delta = params

        betaln_ab = betaln(alpha, beta)
        betaln_gd = betaln(gamma, delta)

        A = betaln(alpha + x, beta + T - x) - betaln_ab + betaln(gamma, delta + T) - betaln_gd

        B = 1e-15 * np.ones_like(T)
        recency_T = T - tx - 1

        for j in np.arange(recency_T.max() + 1):
            ix = recency_T >= j
            B1 = betaln(alpha + x, beta + tx - x + j)
            B2 = betaln(gamma + 1, delta + tx + j)
            B = B + ix * (exp(B1 - betaln_ab)) * (exp(B2 - betaln_gd))
            # v0.11.3
            # B = B + ix * betaf(alpha + x, beta + tx - x + j) * betaf(gamma + 1, delta + tx + j)

        log_B = log(B)
        # v0.11.3
        # B = log(B) - betaln_gd - betaln_ab
        result = logaddexp(A, log_B)
        return result

    @staticmethod
    def _negative_log_likelihood(log_params, frequency, recency, n_periods, weights, penalizer_coef=0):
        params = exp(log_params)
        penalizer_term = penalizer_coef * sum(params ** 2)
        return (
            -(BetaGeoBetaBinomFitter._loglikelihood(params, frequency, recency, n_periods) * weights).sum()
            / weights.sum()
            + penalizer_term
        )
