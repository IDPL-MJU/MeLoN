import keras, os
import tensorflow as tf
from keras import backend
from keras.backend.tensorflow_backend import set_session
from keras.models import Model
from keras.layers import Dense, Input, Add, Flatten, ZeroPadding2D, GlobalAveragePooling2D, BatchNormalization, Activation, Conv2D, MaxPooling2D
from keras.utils import to_categorical

import stl10_input

config = tf.ConfigProto()
config.gpu_options.allow_growth = True
session = tf.Session(config=config)
set_session(session)

class DATA():
    def __init__(self):
        num_classes = 10
        (x_train, y_train), (x_test, y_test) = stl10_input.load_data()

        rows, cols = 96, 96

        if backend.image_data_format() == 'channels_first':
            x_train = x_train.reshape(x_train.shape[0], 3, rows, cols)
            x_test = x_test.reshape(x_test.shape[0], 3, rows, cols)
            input_shape = (3, rows, cols)
        else:
            x_train = x_train.reshape(x_train.shape[0], rows, cols, 3)
            x_test = x_test.reshape(x_test.shape[0], rows, cols, 3)
            input_shape = (rows, cols, 3)

        x_train = x_train.astype('float32') / 255
        y_train = to_categorical(y_train - 1, num_classes)

        x_test = x_test.astype('float32') / 255
        y_test = to_categorical(y_test -1, num_classes)

        self.input_shape = input_shape
        self.num_classes = num_classes
        self.x_train, self.y_train = x_train, y_train
        self.x_test, self.y_test = x_test, y_test

def model(input_shape, num_classes):
    input_tensor = Input(shape=input_shape)
    x = conv1(input_tensor)

    x = conv(x, 3, 16, 0)
    x = conv(x, 4, 32)
    x = conv(x, 6, 64)
    x = conv(x, 3, 128)

    x = GlobalAveragePooling2D()(x)
    output_tensor = Dense(num_classes, activation='softmax')(x)

    return Model(input_tensor, output_tensor)

def conv1(x):
    x = Conv2D(16, (3, 3), strides=(1, 1), padding='same')(x)
    x = BatchNormalization()(x)
    x = Activation('relu')(x)
    return x

def conv(x, num, filters, stage=1):
    stride = (1, 1)
    shortcut = x
    if stage==1:
        stride = (2, 2)
        shortcut = Conv2D(filters, (1, 1), strides=stride, padding='same')(shortcut)

    for i in range(num):
        if i == 0:
            x = Conv2D(filters, (3, 3), strides=stride, padding='same')(x)
            x = BatchNormalization()(x)
            x = Activation('relu')(x)
        else:
            x = Conv2D(filters, (3, 3), strides=(1, 1), padding='same')(x)
            x = BatchNormalization()(x)
            x = Activation('relu')(x)

        x = Conv2D(filters, (3, 3), strides=(1, 1), padding='same')(x)
        x = BatchNormalization()(x)
        x = Activation('relu')(x)

        x = Add()([x, shortcut])
        x = Activation('relu')(x)

        shortcut = x

    return x

def main(batch_size):
    data = DATA()

    resnet = model(data.input_shape, data.num_classes)
    resnet.summary()

    resnet.compile(optimizer='rmsprop', loss='categorical_crossentropy', metrics=['accuracy'])

    history = resnet.fit(data.x_train, data.y_train, batch_size=batch_size, epochs=10, verbose=0)
    score = resnet.evaluate(data.x_test, data.y_test, verbose=0)
    
    print("==============================================")
    print('Training Loss : ', history.history['loss'][9])
    print('Training Accuracy : ', history.history['acc'][9])
    print("==============================================")
    print('Test Loss : ', score[0])
    print('Test Accuracy : ', score[1])

if __name__ == '__main__':
    main(16)
