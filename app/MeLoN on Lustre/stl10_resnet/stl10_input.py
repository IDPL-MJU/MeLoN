from __future__ import print_function

import sys
import os, sys, tarfile, errno
import numpy as np
#import matplotlib.pyplot as plt
    
if sys.version_info >= (3, 0, 0):
    import urllib.request as urllib # ugly but works
else:
    import urllib

FileSystemType = os.environ['FILE_SYSTEM_TYPE']
APP_ID = os.environ['APP_ID']
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
#try:
#    from imageio import imsave
#except:
#    from scipy.misc import imsave

HEIGHT = 96
WIDTH = 96
DEPTH = 3
if FileSystemType == 'LUSTRE':
    DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, '..')) + '/mnt/lustre/melon/input_data/stl10_binary'
else:
    os.system('mkdir -p ./input_data/stl10_binary')
    os.system('hdfs dfs -get /tmp/melon/input_data/stl10_binary/* ./input_data/stl10_binary/')
    DATA_DIR = './input_data/stl10_binary'

def read_labels(path_to_labels):
    with open(path_to_labels, 'rb') as f:
        labels = np.fromfile(f, dtype=np.uint8)
        return labels

def read_all_images(path_to_data):
    with open(path_to_data, 'rb') as f:
        everything = np.fromfile(f, dtype=np.uint8)
        images = np.reshape(everything, (-1, 3, 96, 96))
        images = np.transpose(images, (0, 3, 2, 1))
        return images

def load_data():
    path = DATA_DIR
    train_data_path = os.path.join(path, 'train_X.bin')
    train_label_path = os.path.join(path, 'train_y.bin')
    test_data_path = os.path.join(path, 'test_X.bin')
    test_label_path = os.path.join(path, 'test_y.bin')

    x_train = read_all_images(train_data_path)
    print(x_train.shape)

    y_train = read_labels(train_label_path)
    print(y_train.shape)

    x_test = read_all_images(test_data_path)
    print(x_test.shape)

    y_test = read_labels(test_label_path)
    print(y_test.shape)

    return (x_train, y_train), (x_test, y_test)
