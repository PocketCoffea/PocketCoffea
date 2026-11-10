import ctypes
import numpy as np
import os

# Cache loaded libraries to avoid reloading
_lib_cache = {}

class RoccoR:
    def __init__(self, txt_file, so_path="./RoccoR.so"):
        global _lib_cache
        so_path = os.path.abspath(so_path)
        if so_path not in _lib_cache:
            _lib_cache[so_path] = ctypes.CDLL(so_path)
        self._lib = _lib_cache[so_path]
        self._setup_signatures()
        self._rc = self._lib.RoccoR_new(txt_file.encode())
        # Members per error set, from the "NMEM" header line of the txt file
        # (UL: set 0 nominal, set 1 = 100 stat replicas, sets 2-5 = 1 syst each)
        with open(txt_file) as f:
            self.nmem = next([int(x) for x in l.split()[1:]] for l in f if l.startswith("NMEM"))

    def _setup_signatures(self):
        lib = self._lib
        vp   = ctypes.c_void_p
        dblp = ctypes.POINTER(ctypes.c_double)
        intp = ctypes.POINTER(ctypes.c_int)

        lib.RoccoR_new.restype  = vp
        lib.RoccoR_new.argtypes = [ctypes.c_char_p]

        lib.RoccoR_kScaleDT_vec.restype  = None
        lib.RoccoR_kScaleDT_vec.argtypes = [vp, intp, dblp, dblp, dblp,
                                             ctypes.c_int, ctypes.c_int, ctypes.c_int, dblp]

        lib.RoccoR_kSpreadMC_vec.restype  = None
        lib.RoccoR_kSpreadMC_vec.argtypes = [vp, intp, dblp, dblp, dblp, dblp,
                                              ctypes.c_int, ctypes.c_int, ctypes.c_int, dblp]

        lib.RoccoR_kSmearMC_vec.restype  = None
        lib.RoccoR_kSmearMC_vec.argtypes = [vp, intp, dblp, dblp, dblp, intp, dblp,
                                             ctypes.c_int, ctypes.c_int, ctypes.c_int, dblp]

    def set_deltas(self, func, *args):
        """Shift of the correction factor for each error set s >= 1, w.r.t. the nominal.
        Single-member set: signed f(s,0) - f(0,0). Multi-member set (stat replicas): RMS
        of f(s,m) - f(0,0). `func` is one of kScaleDT / kSpreadMC / kSmearMC, `args` its
        arguments without s, m."""
        nominal = func(*args, s=0, m=0)
        deltas = []
        for s, nmem in enumerate(self.nmem[1:], start=1):
            d = [func(*args, s=s, m=m) - nominal for m in range(nmem)]
            deltas.append(d[0] if nmem == 1 else np.sqrt(np.mean(np.square(d), axis=0)))
        return deltas

    def error(self, func, *args):
        """Vectorised RoccoR::error: total uncertainty on the correction factor,
        sqrt(sum_s sum_m (f(s,m) - f(0,0))^2 / nmem[s]) = set_deltas summed in quadrature."""
        return np.sqrt(np.sum(np.square(self.set_deltas(func, *args)), axis=0))

    @staticmethod
    def _prep(arr, dtype):
        return np.ascontiguousarray(arr, dtype=dtype)

    def kScaleDT(self, charge, pt, eta, phi, s=0, m=0):
        Q, pt_, eta_, phi_ = (self._prep(x, np.int32) if i==0 else self._prep(x, np.float64)
                               for i, x in enumerate([charge, pt, eta, phi]))
        n   = len(pt_)
        out = np.empty(n, dtype=np.float64)
        dblp = ctypes.POINTER(ctypes.c_double)
        intp = ctypes.POINTER(ctypes.c_int)
        self._lib.RoccoR_kScaleDT_vec(self._rc,
            Q.ctypes.data_as(intp), pt_.ctypes.data_as(dblp),
            eta_.ctypes.data_as(dblp), phi_.ctypes.data_as(dblp),
            s, m, n, out.ctypes.data_as(dblp))
        return out

    def kSpreadMC(self, charge, pt, eta, phi, gen_pt, s=0, m=0):
        Q    = self._prep(charge, np.int32)
        pt_  = self._prep(pt,     np.float64)
        eta_ = self._prep(eta,    np.float64)
        phi_ = self._prep(phi,    np.float64)
        gt_  = self._prep(gen_pt, np.float64)
        n    = len(pt_)
        out  = np.empty(n, dtype=np.float64)
        dblp = ctypes.POINTER(ctypes.c_double)
        intp = ctypes.POINTER(ctypes.c_int)
        self._lib.RoccoR_kSpreadMC_vec(self._rc,
            Q.ctypes.data_as(intp), pt_.ctypes.data_as(dblp),
            eta_.ctypes.data_as(dblp), phi_.ctypes.data_as(dblp),
            gt_.ctypes.data_as(dblp), s, m, n, out.ctypes.data_as(dblp))
        return out

    def kSmearMC(self, charge, pt, eta, phi, n_tracker_layers, u_random, s=0, m=0):
        Q    = self._prep(charge,           np.int32)
        pt_  = self._prep(pt,               np.float64)
        eta_ = self._prep(eta,              np.float64)
        phi_ = self._prep(phi,              np.float64)
        nl_  = self._prep(n_tracker_layers, np.int32)
        u_   = self._prep(u_random,         np.float64)
        n    = len(pt_)
        out  = np.empty(n, dtype=np.float64)
        dblp = ctypes.POINTER(ctypes.c_double)
        intp = ctypes.POINTER(ctypes.c_int)
        self._lib.RoccoR_kSmearMC_vec(self._rc,
            Q.ctypes.data_as(intp), pt_.ctypes.data_as(dblp),
            eta_.ctypes.data_as(dblp), phi_.ctypes.data_as(dblp),
            nl_.ctypes.data_as(intp), u_.ctypes.data_as(dblp),
            s, m, n, out.ctypes.data_as(dblp))
        return out
