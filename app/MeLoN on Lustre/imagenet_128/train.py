import numpy as np
import keras, os
from keras.models import Model
from keras.layers import Dense, Input, Flatten, Conv2D, MaxPooling2D
from keras.utils import to_categorical

FileSystemType = os.environ['FILE_SYSTEM_TYPE']
APP_ID = os.environ['APP_ID']
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

if FileSystemType == 'LUSTRE':
    DATA_DIR = os.path.abspath(os.path.join(BASE_dir, '..')) + '/input_data/imagenet'
else:
    os.system('mkdir -p ./input_data/imagenet')
    os.system('hdfs dfs -get /tmp/melon/input_data/imagenet/* ./input_data/imagenet/')
    DATA_DIR = './input_data/stl10_binary'

train = np.load(DATA_DIR + '/train_128.npz')
test = np.load(DATA_DIR + '/test_128.npz')

train_x = train['data']
train_y = train['label']

test_x = test['data']
test_y = test['label']

row, col = 128, 128
num_classes = 1000
input_shape = (row, col, 3)

train_x = train_x / 255
test_x = test_x / 255

train_y = to_categorical(train_y, num_classes)
test_y = to_categorical(test_y, num_classes)

input_tensor = Input(shape=input_shape)
x = Conv2D(64, (3,3), activation='relu', padding='same')(input_tensor)
x = Conv2D(64, (3,3), activation='relu', padding='same')(x)
x = MaxPooling2D((2,2), strides=(2,2))(x)

x = Conv2D(128, (3,3), activation='relu', padding='same')(x)
x = Conv2D(128, (3,3), activation='relu', padding='same')(x)
x = MaxPooling2D((2,2), strides=(2,2))(x)

x = Conv2D(256, (3,3), activation='relu', padding='same')(x)
x = Conv2D(256, (3,3), activation='relu', padding='same')(x)
x = Conv2D(256, (3,3), activation='relu', padding='same')(x)
x = MaxPooling2D((2,2), strides=(2,2))(x)

x = Conv2D(512, (3,3), activation='relu', padding='same')(x)
x = Conv2D(512, (3,3), activation='relu', padding='same')(x)
x = Conv2D(512, (3,3), activation='relu', padding='same')(x)
x = MaxPooling2D((2,2), strides=(2,2))(x)

x = Conv2D(512, (3,3), activation='relu', padding='same')(x)
x = Conv2D(512, (3,3), activation='relu', padding='same')(x)
x = Conv2D(512, (3,3), activation='relu', padding='same')(x)
x = MaxPooling2D((2,2), strides=(2,2))(x)

x = Flatten()(x)
x = Dense(4096, activation='relu')(x)
x = Dense(4096, activation='relu')(x)
output_tensor = Dense(num_classes, activation='softmax')(x)

imagenet = Model(input_tensor, output_tensor)
imagenet.summary()

imagenet.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])

history = imagenet.fit(train_x, train_y, batch_size=64, epochs=10, validation_split=0.2, verbos=0)
score = imagenet.evaluate(test_x, test_y, verbose=0)

print("==============================================")
print('Training Loss : ', history.history['loss'][9])
print('Training Accuracy : ', history.history['acc'][9])
print("==============================================")
print('test Loss : ', score[0])
print('test Accuracy : ', score[1])
