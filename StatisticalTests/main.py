import json
import itertools
import re
from time import time
from distribtests import *
from statistictools import *
import matplotlib.pyplot as plt


def load_json(jsonfile):
    with open(jsonfile, 'r') as json_file:
        s = json_file.read()
        if s:
            return json.loads(s)
        else:
            return {}


def save_pvalues(stat_test, files, distrib_type, parallelisms, lamds, window_sizes, jsonfile):
    data = load_json(jsonfile)

    for file in files:
        f = open(file, 'r')
        text = f.read().lower()
        words = re.sub('\W', ' ', text).split()
        for ps, lamd, ws in itertools.product(parallelisms, lamds, window_sizes):
            if f'{distrib_type} {file} {ps} {lamd} {ws}' not in data:
                t = time()
                data[f'{distrib_type} {file} {ps} {lamd} {ws}'] = distributed_test(stat_test,
                                                                                   words,
                                                                                   dist_type=distrib_type,
                                                                                   parallelism=ps,
                                                                                   lamd=lamd,
                                                                                   window_size=ws)
                print(f'time = {time() - t}', file, ps, lamd, ws)
                with open(jsonfile, 'w') as jsf:
                    json.dump(data, jsf)
        f.close()


def get_pvalues(stat_test, file, distrib_type, parallelism, lamd, window_size):
    with open(file, 'r') as f:
        text = f.read().lower()
        words = re.sub('\W', ' ', text).split()
        return distributed_test(stat_test, words, dist_type=distrib_type, parallelism=parallelism, lamd=lamd,
                                window_size=window_size)


def topwords(file, distrib_type, parallelism, lamd, window_size):
    with open(file, 'r') as f:
        text = f.read().lower()
        words = re.sub('\W', ' ', text).split()
        return distributed_topwords(words, dist_type=distrib_type, parallelism=parallelism, lamd=lamd,
                                    window_size=window_size)


def get_counts(file, distrib_type, parallelism, lamd, window_size):
    with open(file, 'r') as f:
        text = f.read().lower()
        words = re.sub('\W', ' ', text).split()
        return distributed_counts(words, dist_type=distrib_type, parallelism=parallelism, lamd=lamd,
                                  window_size=window_size)


def new_corrupt_file(basefile, sample_func, size, args, decim=2, sep_name='_',
                     border_mark='конецтекстаначалологнормального'):
    size -= size % 100
    filedir = basefile.rsplit('/', maxsplit=1)[0] + '/'
    sample_file = filedir + sample_func.__name__ + sep_name + str(size) + sep_name + \
                  sep_name.join(str(a) for a in args) + '.txt'

    corrupt_file = basefile.replace('.txt', '') + '+' + sample_file.rsplit('/', maxsplit=1)[1]

    sample = sample_func(*args, size=size)
    sample = np.around(sample, decimals=decim) * (10 ** decim)
    sample = np.array(sample, dtype=np.int)
    np.savetxt(sample_file, sample.reshape((size // 100, 100)), fmt='%i', newline='\n')

    with open(corrupt_file, 'w') as outfile:
        with open(basefile) as infile:
            outfile.write(infile.read())

        outfile.write(' ' + border_mark + ' ')

        with open(sample_file) as infile:
            outfile.write(infile.read())
    return (sample_file, corrupt_file)


def plot_counts(filename):
    file = open(filename, 'r')
    text = file.read().lower()
    file.close()

    words = re.sub('\W', ' ', text).split()
    unique, counts = np.unique(words, return_counts=True)
    counts[::-1].sort()

    plt.plot(range(1, len(counts) + 1), counts, label=filename)
    plt.xlabel('rank')
    plt.ylabel('counts')
    plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
    plt.show()


if __name__ == '__main__':
    files = [f'../TestData/wiki+lognorm/{i}.txt' for i in range(10)]
    parals = [1, 2, 4, 8, 12, 16, 20, 24, 28, 32]
    save_pvalues(chisquare_for_zipf, files, 'hash', parals, [-0.05], [500], 'new_results.json')
    save_pvalues(chisquare_for_zipf, files, 'round', parals, [-0.05], [500], 'new_results.json')


