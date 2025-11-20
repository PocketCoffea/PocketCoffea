##THIS IS THE MuonScaRe.py from https://gitlab.cern.ch/cms-muonPOG/muonscarekit/-/tree/master/scripts?ref_type=heads

import numpy as np
import math
from scipy.special import erfinv, erf
from random import random
import awkward as ak
from typing import List

import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)

class SeedSequence:
    def __init__(self, seeds: List[int]):
        self.seeds = [s & 0xFFFFFFFF for s in seeds]

    def generate(self, n: int) -> List[int]:
        if n<=0:
            return []

        mult = 0x9e3779b9
        mix_const = 0x85ebca6b

        buffer = [0x8b8b8b8b] * n
        s = len(self.seeds)

        for i in range(min(n, s)):
            buffer[i] ^= (self.seeds[i] + mult * i) & 0xFFFFFFFF

        for i in range(s, n):
            buffer[i] ^= (mult * i) & 0xFFFFFFFF

        for k in range(n):
            z = (buffer[(k + n - 1) % n] ^ (buffer[k] >> 27)) & 0xFFFFFFFF
            buffer[k] = ((z * mix_const) & 0xFFFFFFFF) ^ ((buffer[k] << 13) & 0xFFFFFFFF)

        return [x & 0xFFFFFFFF for x in buffer]

class CrystallBall:

    def __init__(self, m, s, a, n):
        self.pi = 3.14159
        self.sqrtPiOver2 = np.sqrt(self.pi/2.0)
        self.sqrt2 = np.sqrt(2.0)
        self.m = ak.Array(m)
        self.s = ak.Array(s)
        self.a = ak.Array(a)
        self.n = ak.Array(n)
        self.fa = abs(self.a)
        self.ex = np.exp(-self.fa * self.fa/2)
        self.A  = (self.n/self.fa)**self.n * self.ex
        self.C1 = self.n/self.fa/(self.n-1) * self.ex
        self.D1 = 2 * self.sqrtPiOver2 * erf(self.fa/self.sqrt2)

        self.B = self.n/self.fa - self.fa
        self.C = (self.D1 + 2 * self.C1)/self.C1
        self.D = (self.D1 + 2 * self.C1)/2

        self.N = 1.0/self.s/(self.D1 + 2 * self.C1)
        self.k = 1.0/(self.n - 1)

        self.NA = self.N * self.A
        self.Ns = self.N * self.s
        self.NC = self.Ns * self.C1
        self.F = 1 - self.fa * self.fa/self.n
        self.G = self.s * self.n/self.fa
        self.cdfMa = self.cdf(self.m - self.a * self.s)
        self.cdfPa = self.cdf(self.m + self.a * self.s)


    def cdf(self, x):
        x = ak.Array(x)
        d = (x - self.m)/self.s
        result = ak.full_like(d, 1.0)
        
        # define different conditions
        c1a = (d<-self.a) & (self.F - self.s * d / self.G > 0)
        c1b = (d<-self.a) & (self.F - self.s * d / self.G <= 0)
        c2a = (d>self.a) & (self.F + self.s * d / self.G > 0)
        c2b = (d>self.a) & (self.F + self.s * d / self.G <= 0)

        c3 = ~c1a & ~c1b & ~c2a & ~c2b
        # For d < -a
        result = ak.where(c1a, 
                        self.NC / np.power(self.F - self.s * d / self.G, self.n - 1), 
                        result)
        result = ak.where(c1b, 
                        self.NC, 
                        result)

        # For d > a
        result = ak.where(c2a, 
                        self.NC * (self.C - np.power(self.F + self.s * d / self.G, 1 - self.n)), 
                        result)
        result = ak.where(c2b, 
                        self.NC * self.C, 
                        result)

        # For -a <= d <= a
        result = ak.where(c3, 
                        self.Ns * (self.D - self.sqrtPiOver2 * erf(-d / self.sqrt2)), 
                        result)

        return result


    def invcdf(self, u):
        u = ak.Array(u)
        result = ak.zeros_like(u)

        c1a = (u < self.cdfMa) & (self.NC/u > 0)
        c1b = (u < self.cdfMa) & (self.NC/u <= 0)
        c2a = (u > self.cdfPa) &  (self.C - u/self.NC > 0)
        c2b = (u > self.cdfPa) &  (self.C - u/self.NC <= 0)
        c3 = ~c1a & ~c1b & ~c2a & ~c2b

        # For u < cdfMa
        result = ak.where(c1a, 
                        self.m + self.G * (self.F - (self.NC / u) ** self.k), 
                        result)
        result = ak.where(c1b, 
                        self.m + self.G * self.F, 
                        result)

        # For u > cdfPa
        result = ak.where(c2a, 
                        self.m - self.G * (self.F - (self.C - u / self.NC) ** (-self.k)), 
                        result)
        result = ak.where(c2b, 
                        self.m - self.G * self.F, 
                        result)

        # For cdfMa <= u <= cdfPa
        result = ak.where(c3, 
                        self.m - self.sqrt2 * self.s * erfinv((self.D - u / self.Ns) / self.sqrtPiOver2), 
                        result)

        return result


def _get_rnd_func(rnd_gen):
    if isinstance(rnd_gen, str):
        rnd_gen = rnd_gen.lower()
        if rnd_gen == "np":
            rnd_func = lambda seed: np.random.Generator(np.random.MT19937(seed=seed)).random()
        else:
            raise ValueError(f"unknown rnd_gen string '{rnd_gen}', should either be 'root' or 'np'")
    elif callable(rnd_gen):
        rnd_func = rnd_gen
    else:
        raise TypeError(f"unsupported rnd_gen '{rnd_gen}', should be string or callable")
    return rnd_func


def get_rndm(eta, phi, nL, evtNr, lumiNr, cset, nested=False, rnd_gen="root"):
    # obtain parameters from correctionlib
    if nested:
        eta_f, phi_f, nL_f, nmuons = ak.flatten(eta), ak.flatten(phi), ak.flatten(nL), ak.num(nL)
        evtNr_f  = np.repeat(np.asarray(evtNr),  nmuons)
        lumiNr_f = np.repeat(np.asarray(lumiNr), nmuons)
    else:
        eta_f, phi_f, nL_f, nmuons = eta, phi, nL, np.ones_like(eta)

    mean_f = cset.get("cb_params").evaluate(abs(eta_f), nL_f, 0)
    sigma_f = cset.get("cb_params").evaluate(abs(eta_f), nL_f, 1)
    n_f = cset.get("cb_params").evaluate(abs(eta_f), nL_f, 2)
    alpha_f = cset.get("cb_params").evaluate(abs(eta_f), nL_f, 3)

    phi_int_f = (((np.asarray(phi_f, dtype=np.float64) / math.pi) * (1 << 31 - 1) ).astype(np.int64) & 0xFFF).astype(np.uint32)

    seeds = [
        SeedSequence([np.uint32(ev), np.uint32(lumi), np.uint32(phi_int)]).generate(1)[0]
        for ev, lumi, phi_int in zip(evtNr_f, lumiNr_f, phi_int_f)
    ]

    # get random number following the CB
    rnd_func = _get_rnd_func(rnd_gen)
    rndm_f = [rnd_func(seed) for seed in seeds]

    cb_f = CrystallBall(mean_f, sigma_f, alpha_f, n_f)

    result_f = cb_f.invcdf(rndm_f)

    if nested:
        result = ak.unflatten(result_f, nmuons)
    else:
        result = result_f

    return result


def get_std(pt, eta, nL, cset, nested=False):
    if nested:
        eta_f, nL_f, pt_f, nmuons = ak.flatten(eta), ak.flatten(nL), ak.flatten(pt), ak.num(nL)
    else:
        eta_f, nL_f, pt_f, nmuons = eta, nL, pt, 1

    # obtain parameters from correctionlib    
    param0_f = cset.get("poly_params").evaluate(abs(eta_f), nL_f, 0)
    param1_f = cset.get("poly_params").evaluate(abs(eta_f), nL_f, 1)
    param2_f = cset.get("poly_params").evaluate(abs(eta_f), nL_f, 2)

    # calculate value and return max(0, val)
    sigma_f = param0_f + param1_f * pt_f + param2_f * pt_f*pt_f
    sigma_corrected_f = np.where(sigma_f < 0, 0, sigma_f)

    if nested:
        result = ak.unflatten(sigma_corrected_f, nmuons)
    else:
        result = sigma_corrected_f

    return result


def get_k(eta, var, cset, nested=False):
    if nested:
        eta_f, nmuons = ak.flatten(eta), ak.num(eta)
    else:
        eta_f = eta

    # obtain parameters from correctionlib
    k_data_f = cset.get("k_data").evaluate(abs(eta_f), var)
    k_mc_f = cset.get("k_mc").evaluate(abs(eta_f), var)

    # calculate residual smearing factor 
    # return 0 if smearing in MC already larger than in data
    k_f = np.zeros_like(k_data_f)
    condition = k_mc_f<k_data_f
    k_f[condition] = (k_data_f[condition]**2 - k_mc_f[condition]**2)**.5

    if nested:
        result = ak.unflatten(k_f, nmuons)
    else:
        result = k_f

    return result


def filter_boundaries(pt_corr, pt, nested, low_pt_threshold = 26, silent=False):
    if not nested:
        pt_corr = np.asarray(pt_corr)
        pt = np.asarray(pt)

    # Check for pt values outside the range of [low_pt_threshold, 200]
    outside_bounds = (pt < low_pt_threshold) | (pt > 200)

    if nested:
        n_pt_outside = ak.sum(ak.any(outside_bounds, axis=-1))
    else:
        n_pt_outside = np.sum(outside_bounds)

    if n_pt_outside > 0:
        if not silent:
            print(
                f"There are {n_pt_outside} events with muon pt outside of [" + str(low_pt_threshold) + ",200] GeV. "
                "Setting those entries to their initial value."
            )
        pt_corr = np.where(pt>200, pt, pt_corr)
        pt_corr = np.where(pt<low_pt_threshold, pt, pt_corr)

    # Check for NaN entries in pt_corr
    nan_entries = np.isnan(pt_corr)

    if nested:
        n_nan = ak.sum(ak.any(nan_entries, axis=-1))
    else:
        n_nan = np.sum(nan_entries)

    if n_nan > 0:
        if not silent:
            print(
                f"There are {n_nan} nan entries in the corrected pt. "
                "This might be due to the number of tracker layers hitting boundaries. "
                "Setting those entries to their initial value."
            )
        pt_corr = np.where(np.isnan(pt_corr), pt, pt_corr)

    return pt_corr


def pt_resol(pt, eta, phi, nL, evtNr, lumiNr, cset, nested=False, low_pt_threshold = 26, rnd_gen="root"):
    """"
    Function for the calculation of the resolution correction
    Input: 
    pt - muon transverse momentum
    eta - muon pseudorapidity
    nL - muon number of tracker layers
    cset - correctionlib object

    This function should only be applied to reco muons in MC!
    """
    rndm = get_rndm(eta, phi, nL, evtNr, lumiNr, cset, nested, rnd_gen=rnd_gen)
    std = get_std(pt, eta, nL, cset, nested)
    k = get_k(eta, "nom", cset, nested)

    pt_corr = pt * (1 + k * std * rndm)

    pt_corr = filter_boundaries(pt_corr, pt, nested, low_pt_threshold)

    pt_filter = (pt_corr / pt > 2) | (pt_corr / pt < 0.1) | (pt_corr < 0)
    pt_corr = ak.where(pt_filter, pt, pt_corr)

    return pt_corr


def pt_resol_var(pt_woresol, pt_wresol, eta, updn, cset, nested=False):
    """
    Function for the calculation of the resolution uncertainty
    Input:
    pt_woresol - muon transverse momentum without resolution correction
    pt_wresol - muon transverse momentum with resolution correction
    eta - muon pseudorapidity
    updn - uncertainty variation (up or dn)
    cset - correctionlib object
    
    This function should only be applied to reco muons in MC!
    """
    
    if nested:
        eta_f, nmuons = ak.flatten(eta), ak.num(eta)
        pt_wresol_f, pt_woresol_f = ak.flatten(pt_wresol), ak.flatten(pt_woresol)
    else:
        eta_f, nmuons = eta, 1
        pt_wresol_f, pt_woresol_f = pt_wresol, pt_woresol

    k_unc_f = cset.get("k_mc").evaluate(abs(eta_f), "stat")
    k_f = cset.get("k_mc").evaluate(abs(eta_f), "nom")

    pt_var_f = pt_wresol_f

     # Define condition and standard correction
    condition = k_f > 0
    std_x_cb = (pt_wresol_f / pt_woresol_f - 1) / k_f

    # Apply up or down variation using ak.where
    if updn == "up":
        pt_var_f = ak.where(
            condition,
            pt_woresol_f * (1 + (k_f + k_unc_f) * std_x_cb),
            pt_var_f,
        )
    elif updn == "dn":
        pt_var_f = ak.where(
            condition,
            pt_woresol_f * (1 + (k_f - k_unc_f) * std_x_cb),
            pt_var_f,
        )
    else:
        print("ERROR: updn must be 'up' or 'dn'")

    pt_filter = (pt_var_f / pt_woresol_f > 2) | (pt_var_f / pt_woresol_f < 0.1) | (pt_var_f < 0)
    pt_var_f = ak.where(pt_filter, pt_woresol_f, pt_var_f)

    if nested:
        pt_var = ak.unflatten(pt_var_f, nmuons)
    else:
        pt_var = pt_var_f

    return pt_var

def pt_scale(is_data, pt, eta, phi, charge, cset, nested=False, low_pt_threshold = 26):
    """
    Function for the calculation of the scale correction
    Input:
    is_data - flag that is True if dealing with data and False if MC
    pt - muon transverse momentum
    eta - muon pseudorapidity
    phi - muon angle
    charge - muon charge
    var - variation (standard is "nom")
    cset - correctionlib object
    
    This function should be applied to reco muons in data and MC
    """
    if is_data:
        dtmc = "data"
    else:
        dtmc = "mc"

    if nested:
        eta_f, phi_f, nmuons = ak.flatten(eta), ak.flatten(phi), ak.num(eta)
    else:
        eta_f, phi_f, nmuons = eta, phi, 1
    
    a_f = cset.get("a_"+dtmc).evaluate(eta_f, phi_f, "nom")
    m_f = cset.get("m_"+dtmc).evaluate(eta_f, phi_f, "nom")

    if nested:
        a, m = ak.unflatten(a_f, nmuons), ak.unflatten(m_f, nmuons)
    else: 
        a, m = a_f, m_f

    pt_corr = 1. / (m/pt + charge * a)

    pt_corr = filter_boundaries(pt_corr, pt, nested, low_pt_threshold)

    return pt_corr


def pt_scale_var(pt, eta, phi, charge, updn, cset, nested=False):
    """
    Function for the calculation of the scale uncertainty
    Input:
    pt - muon transverse momentum
    eta - muon pseudorapidity
    phi - muon angle
    charge - muon charge
    updn - uncertainty variation (up or dn)
    cset - correctionlib object

    This function should be applied to reco muons in MC!
    """

    if nested:
        eta_f, phi_f, nmuons = ak.flatten(eta), ak.flatten(phi), ak.num(eta)
    else:
        eta_f, phi_f, pt_f, nmuons = eta, phi, pt, 1

    stat_a_f = cset.get("a_mc").evaluate(eta_f, phi_f, "stat")
    stat_m_f = cset.get("m_mc").evaluate(eta_f, phi_f, "stat")
    stat_rho_f = cset.get("m_mc").evaluate(eta_f, phi_f, "rho_stat")

    if nested:
        stat_a, stat_m, stat_rho = ak.unflatten(stat_a_f, nmuons), ak.unflatten(stat_m_f, nmuons), ak.unflatten(stat_rho_f, nmuons)

    unc = pt*pt * (stat_m*stat_m / (pt*pt) + stat_a*stat_a + 2*charge*stat_rho*stat_m/pt*stat_a)**.5

    pt_var = pt

    if updn=="up":
        pt_var = pt_var + unc
    elif updn=="dn":
        pt_var = pt_var - unc

    return pt_var

