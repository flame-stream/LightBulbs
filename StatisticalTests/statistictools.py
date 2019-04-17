import numpy as np
from scipy.stats import chisquare
from rpy2.robjects.packages import importr
import rpy2.robjects.numpy2ri as np2r


def get_sorted_counts(sample):
    _, counts = np.unique(sample, return_counts=True)
    counts[::-1].sort()
    return counts


def grouped_counts(counts, zipf_counts, min_count=5):
    group_counts = [0]
    group_zipf_counts = [0]
    for (c, zc) in zip(counts, zipf_counts):
        if group_zipf_counts[-1] >= min_count:
            group_zipf_counts.append(zc)
            group_counts.append(c)
        else:
            group_zipf_counts[-1] += zc
            group_counts[-1] += c
    return group_counts, group_zipf_counts


def processed_counts(sorted_counts, *, power, alpha):
    zipf_counts = np.array([alpha / np.power(i, power) for i in range(1, len(sorted_counts) + 1)])
    sorted_counts, zipf_counts = grouped_counts(sorted_counts, zipf_counts)
    return sorted_counts, zipf_counts


def chisquare_for_zipf(sorted_counts, *, power, alpha, skip_top_procent=1):
    counts, zipf_counts = processed_counts(sorted_counts, power=power, alpha=alpha)
    skip_top = int(skip_top_procent * len(counts) / 100)
    p = chisquare(counts[skip_top:], zipf_counts[skip_top:])[1]
    return p


def ls_parameter_estimation(sorted_counts, start=10, min_count=5):
    counts = sorted_counts[np.where(sorted_counts >= min_count)][start:]
    lc = len(counts)
    if lc < 3:
        return 1.0, sorted_counts[0]

    logf = np.log(counts)
    logn = np.log(np.arange(start=1, stop=lc + 1, step=1) + start)
    sum_logf = np.sum(logf)
    sum_logn = np.sum(logn)
    dot_logf_logn = np.dot(logf, logn)
    dot_logn_logn = np.dot(logn, logn)

    power = (lc * dot_logf_logn / sum_logn - sum_logf) / (sum_logn - lc * dot_logn_logn / sum_logn)
    alpha = np.exp((power * dot_logn_logn + dot_logf_logn) / sum_logn)
    return max(0.65, power), alpha


def mle_parameter_estimation(sample):
    tolerance = importr('tolerance')
    np2r.activate()
    unique, counts = np.unique(sample, return_counts=True)
    res = tolerance.zm_ll(x=sample, N=len(unique), dist="Zipf-Man")
    split = str(res).split('b')[3].strip().split(' ')
    s = float(split[0])
    beta = float(split[1])
    return s, beta
