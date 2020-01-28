import keras, os
from keras import backend
from keras.models import Model
from keras.layers import Dense, Input, Add, Flatten, ZeroPadding2D, GlobalAveragePooling2D, BatchNormalization, Activation, Conv2D, MaxPooling2D
from keras.utils import plot_model, to_categorical

#from keras import datasets
#from .cifar10 import load_data
import cifar10

class DATA():
    def __init__(self, dataset):
        num_classes = 10
        (train_data, train_labels), (test_data, test_labels) = cifar10.load_data()
            
        img_rows, img_cols = 32, 32

        if backend.image_data_format() == 'channels_first':
            train_images = train_data.reshape(train_data.shape[0], 3, img_rows, img_cols)
            test_images = test_data.reshape(test_data.shape[0], 3, img_rows, img_cols)
            input_shape = (3, img_rows, img_cols)
        else:
            train_images = train_data.reshape(train_data.shape[0], img_rows, img_cols, 3)
            test_images = test_data.reshape(test_data.shape[0], img_rows, img_cols, 3)
            input_shape = (img_rows, img_cols, 3)

        train_images = train_images.astype('float32') / 255
        train_labels = to_categorical(train_labels, num_classes)

        test_images = test_images.astype('float32') / 255
        test_labels = to_categorical(test_labels, num_classes)

        self.input_shape = input_shape
        self.num_classes = num_classes
        self.train_images, self.train_labels = train_images, train_labels
        self.test_images, self.test_labels = test_images, test_labels

def model(input_shape, num_classes, depth):
    input_tensor = Input(shape=input_shape)
    x = conv1(input_tensor)
    if depth == 34:
        x = conv(x, 3, 16, 0)
        x = conv(x, 4, 32)
        x = conv(x, 6, 64)
        x = conv(x, 3, 128)
    else:
        x = conv(x, 2, 16, 0)
        x = conv(x, 2, 32)
        x = conv(x, 2, 64)
        x = conv(x, 2, 128)

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
    print(stage)
    if stage==1:
        stride = (2, 2)
        shortcut = Conv2D(filters, (1, 1), strides=stride, padding='same')(shortcut)

    for i in range(num):
        if(i == 0):
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

def main(batch_size, depth, dataset):
    data = DATA(dataset)

    resnet = model(data.input_shape, data.num_classes, depth)
    resnet.summary()
    plot_model(resnet, to_file="resnet{0}.png".format(depth),show_shapes=True)

    resnet.compile(optimizer='rmsprop', loss='categorical_crossentropy', metrics=['accuracy'])

    history = resnet.fit(data.train_images, data.train_labels, batch_size=batch_size, epochs=10, validation_split=0.2)
    score = resnet.evaluate(data.test_images, data.test_labels)

    print("=== batch_size is {0}, depth is {1}, dataset is {2}".format(batch_size, depth, dataset))
    print('Test Loss : ', score[0])
    print('Test Accuracy : ', score[1])

if __name__ == '__main__':
    main(32, 3, 'cifar10')
