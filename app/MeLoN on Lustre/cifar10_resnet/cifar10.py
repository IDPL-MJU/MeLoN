from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from keras import backend as K
import numpy as np
import os
import sys
from six.moves import cPickle

FileSystemType = os.environ['FILE_SYSTEM_TYPE']
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

if FileSystemType == 'LUSTRE':
    DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, '..')) + '/input_data/cifar-10-batches-py'
else:
    os.system('mkdir -p ./input_data/cifar-10-batches-py')
    os.system('hdfs dfs -get /tmp/melon/input_data/cifar-10-batches-py/* ./input_data/cifar-10-batches-py/')
    DATA_DIR = './input_data/cifar-10-batches-py'

def load_batch(fpath, label_key='labels'):
    with open(fpath, 'rb') as f:
        if sys.version_info < (3,):
            d = cPickle.load(f)
        else:
            d = cPickle.load(f, encoding='bytes')
            d_decoded = {}
            for k, v in d.items():
                d_decoded[k.decode('utf8')] = v
            d = d_decoded
        data = d['data']
        labels = d[label_key]

        data = data.reshape(data.shape[0], 3, 32, 32)
        return data, labels

def load_data():
    path = DATA_DIR

    num_train_samples=50000

    x_train = np.empty((num_train_samples, 3, 32, 32), dtype='uint8')
    y_train = np.empty((num_train_samples,), dtype='uint8')

    for i in range(1, 6):
        fpath = os.path.join(path, 'data_batch_' + str(i))
        (x_train[(i-1) * 10000: i * 10000, :, :, :],
         y_train[(i-1) * 10000: i * 10000]) = load_batch(fpath)

    fpath = os.path.join(path, 'test_batch')
    x_test, y_test = load_batch(fpath)

    y_train = np.reshape(y_train, (len(y_train), 1))
    y_test = np.reshape(y_test, (len(y_test), 1))

    if K.image_data_format() == 'channels_last':
        x_train = x_train.transpose(0, 2, 3, 1)
        x_test = x_test.transpose(0, 2, 3, 1)

    return (x_train, y_train), (x_test, y_test)
