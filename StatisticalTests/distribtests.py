import random
import string
import mmh3
import numpy as np
from collections import defaultdict
from statistictools import ls_parameter_estimation, processed_counts


def hash_generator(size=10, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def group_index_by_hash(el, i, parallelism):
    return mmh3.hash(hash_generator() + str(el) + hash_generator()) % parallelism


def group_index_by_order(el, i, parallelism):
    return mmh3.hash(str(i)) % parallelism


def distributed_samples(sample, *, dist_type, parallelism, border_mark='конецтекстаначалологнормального'):
    samples = defaultdict(list)
    group_id_func = {'hash': group_index_by_hash,
                     'round': group_index_by_order}
    group_id = group_id_func[dist_type]
    borderlines = {}

    for timestamp, el in enumerate(sample):
        if el == border_mark:
            for group_index in samples:
                borderlines[group_index] = len(samples[group_index])
        samples[group_id(el, timestamp, parallelism)].append((el, timestamp))
    return samples, borderlines or None


def distributed_test(stat_test, sample, *, dist_type, lamd, window_size, parallelism):
    samples, borderlines = distributed_samples(sample, dist_type=dist_type, parallelism=parallelism)
    p_values = {}
    for group_index in samples:
        p_values[group_index] = exp_smoothing_test(stat_test, samples[group_index], window_size=window_size, lamd=lamd)
    return p_values, borderlines


def distributed_topwords(sample, *, dist_type, lamd, window_size, parallelism):
    samples, borderlines = distributed_samples(sample, dist_type=dist_type, parallelism=parallelism)
    topwords = {}
    for group_index in samples:
        topwords[group_index] = exp_smoothing_topwords(samples[group_index], window_size=window_size, lamd=lamd)
    return topwords


def distributed_counts(sample, *, dist_type, lamd, window_size, parallelism):
    samples, borderlines = distributed_samples(sample, dist_type=dist_type, parallelism=parallelism)
    all_counts = {}
    for group_index in samples:
        all_counts[group_index] = exp_smoothing_counts(samples[group_index], window_size=window_size, lamd=lamd)
    return all_counts


def exp_smoothing_test(stat_test, sample, *, window_size, lamd):
    p_values = []
    word_counter = defaultdict(int)
    window_word_counter = defaultdict(int)
    k = np.exp(lamd)

    for i, (el, timestamp) in enumerate(sample):
        window_word_counter[el] += 1
        if i >= window_size and i % window_size == 0:
            counts = []
            for word in set(word_counter).union(set(window_word_counter)):
                word_counter[word] = k * word_counter[word] + window_word_counter.get(word, 0)
                if word_counter[word] >= 1:
                    counts.append(word_counter[word])
            counts = np.array(counts)
            counts[::-1].sort()
            power, alpha = ls_parameter_estimation(counts)
            st = stat_test(counts, power=power, alpha=alpha)
            p_values.append((i, st if st > 0.05 else -1, timestamp))
            window_word_counter.clear()
    return p_values


def exp_smoothing_counts(sample, *, window_size, lamd, epoch=10):
    all_counts = []
    word_counter = defaultdict(int)
    window_word_counter = defaultdict(int)
    k = np.exp(lamd)

    for i, el in enumerate(sample):
        window_word_counter[el] += 1
        if i >= window_size and i % window_size == 0:
            for word in set(word_counter).union(set(window_word_counter)):
                word_counter[word] = k * word_counter[word] + window_word_counter.get(word, 0)
            if i % (window_size * epoch) == 0:
                counts = []
                for word in word_counter:
                    if word_counter[word] >= 1:
                        counts.append(word_counter[word])
                counts = np.array(counts)
                counts[::-1].sort()
                power, alpha = ls_parameter_estimation(counts)
                c, zc = processed_counts(counts, power=power, alpha=alpha)
                all_counts.append((i, c, zc))
            window_word_counter.clear()
    return all_counts


def exp_smoothing_topwords(sample, *, window_size, lamd, epoch=10, topn=100):
    topwords = []
    word_counter = defaultdict(int)
    window_word_counter = defaultdict(int)
    k = np.exp(lamd)

    for i, el in enumerate(sample):
        window_word_counter[el] += 1
        if i >= window_size and i % window_size == 0:
            for word in set(word_counter).union(set(window_word_counter)):
                word_counter[word] = k * word_counter[word] + window_word_counter.get(word, 0)

            if i % (window_size * epoch) == 0:
                counts = []
                for word in word_counter:
                    if word_counter[word] >= 1:
                        counts.append((word_counter[word], word))
                counts.sort()
                counts = counts[::-1]
                topwords.append((i, counts[:topn]))
            window_word_counter.clear()
    return topwords
